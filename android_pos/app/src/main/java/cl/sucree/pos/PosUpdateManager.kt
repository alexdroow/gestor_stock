package cl.sucree.pos

import android.app.AlarmManager
import android.app.DownloadManager
import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.net.Uri
import android.os.Build
import android.os.Environment
import org.json.JSONObject
import java.net.HttpURLConnection
import java.net.URL
import java.util.Calendar

object PosUpdateManager {
    private const val PAGE_URL = "https://pasteleriasucree.cl/descargar-pos-android"
    private const val PREFS = "pos_app_updates"
    private const val LATEST_VERSION_KEY = "latest_version"
    private const val LATEST_APK_PATH_KEY = "latest_apk_path"
    private const val CHANNEL_ID = "pos_app_updates_v1"
    private const val NOTIFICATION_ID = 1490
    private const val DAILY_REQUEST_CODE = 1491

    fun check(context: Context, announce: Boolean, callback: ((JSONObject) -> Unit)? = null) {
        Thread {
            val result = runCatching { fetchLatest(context) }.getOrElse { error ->
                JSONObject().put("success", false).put("error", error.message ?: "No se pudo consultar la página de actualizaciones.")
            }
            if (result.optBoolean("success") && result.optBoolean("available") && announce) {
                showUpdateAvailable(context, result.optString("latestVersion"))
            }
            callback?.invoke(result)
        }.start()
    }

    fun scheduleDaily(context: Context) {
        val alarms = context.getSystemService(AlarmManager::class.java)
        val calendar = Calendar.getInstance().apply {
            set(Calendar.HOUR_OF_DAY, 13)
            set(Calendar.MINUTE, 0)
            set(Calendar.SECOND, 0)
            set(Calendar.MILLISECOND, 0)
            if (timeInMillis <= System.currentTimeMillis()) add(Calendar.DAY_OF_YEAR, 1)
        }
        val intent = Intent(context, PosUpdateReceiver::class.java).setAction(PosUpdateReceiver.ACTION_DAILY_CHECK)
        val pending = PendingIntent.getBroadcast(
            context,
            DAILY_REQUEST_CODE,
            intent,
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE,
        )
        try {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S && !alarms.canScheduleExactAlarms()) {
                alarms.setAndAllowWhileIdle(AlarmManager.RTC_WAKEUP, calendar.timeInMillis, pending)
            } else {
                alarms.setExactAndAllowWhileIdle(AlarmManager.RTC_WAKEUP, calendar.timeInMillis, pending)
            }
        } catch (_: Exception) {
            alarms.setAndAllowWhileIdle(AlarmManager.RTC_WAKEUP, calendar.timeInMillis, pending)
        }
    }

    fun downloadAvailableUpdate(context: Context): Boolean {
        val prefs = context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
        val version = prefs.getString(LATEST_VERSION_KEY, null).orEmpty()
        val path = prefs.getString(LATEST_APK_PATH_KEY, null).orEmpty()
        if (!isNewerVersion(version, BuildConfig.VERSION_NAME) || !path.startsWith("/downloader/apk/")) return false
        return runCatching {
            val request = DownloadManager.Request(Uri.parse("https://pasteleriasucree.cl$path"))
                .setTitle("Actualización SucréePOS $version")
                .setDescription("Descargando la versión nueva. Al terminar, abre la notificación para instalarla.")
                .setMimeType("application/vnd.android.package-archive")
                .setNotificationVisibility(DownloadManager.Request.VISIBILITY_VISIBLE_NOTIFY_COMPLETED)
                .setAllowedOverMetered(true)
                .setAllowedOverRoaming(true)
                .setDestinationInExternalFilesDir(context, Environment.DIRECTORY_DOWNLOADS, "SucreePOS-$version.apk")
            context.getSystemService(DownloadManager::class.java).enqueue(request)
            true
        }.getOrDefault(false)
    }

    private fun fetchLatest(context: Context): JSONObject {
        val connection = (URL(PAGE_URL).openConnection() as HttpURLConnection).apply {
            connectTimeout = 7_000
            readTimeout = 7_000
            useCaches = false
            setRequestProperty("Accept", "text/html")
            setRequestProperty("Cache-Control", "no-cache")
            setRequestProperty("User-Agent", "SucreePOS-Android/${BuildConfig.VERSION_NAME}")
        }
        val html = try {
            if (connection.responseCode !in 200..299) error("La página de versiones respondió ${connection.responseCode}.")
            connection.inputStream.bufferedReader(Charsets.UTF_8).use { it.readText() }
        } finally {
            connection.disconnect()
        }
        val version = metaContent(html, "sucree-pos-version")
            ?: error("La página no informa la versión de SucréePOS.")
        val apkPath = metaContent(html, "sucree-pos-apk")
            ?: error("La página no informa el enlace de descarga.")
        if (!Regex("^/downloader/apk/[A-Za-z0-9._-]+\\.apk$").matches(apkPath)) {
            error("El enlace de actualización publicado no es válido.")
        }
        val current = BuildConfig.VERSION_NAME
        val available = isNewerVersion(version, current)
        context.getSharedPreferences(PREFS, Context.MODE_PRIVATE).edit()
            .putString(LATEST_VERSION_KEY, version)
            .putString(LATEST_APK_PATH_KEY, apkPath)
            .apply()
        return JSONObject()
            .put("success", true)
            .put("currentVersion", current)
            .put("latestVersion", version)
            .put("available", available)
    }

    private fun metaContent(html: String, name: String): String? {
        val pattern = Regex(
            "(?is)<meta\\s+name=[\"']${Regex.escape(name)}[\"']\\s+content=[\"']([^\"']+)[\"']",
        )
        return pattern.find(html)?.groupValues?.getOrNull(1)?.trim()?.takeIf(String::isNotBlank)
    }

    private fun isNewerVersion(candidate: String, installed: String): Boolean {
        val newParts = candidate.split('.').map { it.toIntOrNull() ?: return false }
        val oldParts = installed.split('.').map { it.toIntOrNull() ?: return false }
        for (index in 0 until maxOf(newParts.size, oldParts.size)) {
            val newer = newParts.getOrElse(index) { 0 }
            val older = oldParts.getOrElse(index) { 0 }
            if (newer != older) return newer > older
        }
        return false
    }

    private fun showUpdateAvailable(context: Context, version: String) {
        if (Build.VERSION.SDK_INT >= 33 &&
            context.checkSelfPermission(android.Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED
        ) return
        val manager = context.getSystemService(NotificationManager::class.java)
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O && manager.getNotificationChannel(CHANNEL_ID) == null) {
            manager.createNotificationChannel(NotificationChannel(
                CHANNEL_ID,
                "Actualizaciones de SucréePOS",
                NotificationManager.IMPORTANCE_HIGH,
            ).apply { description = "Avisos cuando hay una nueva versión disponible para descargar." })
        }
        val intent = Intent(context, PosUpdateReceiver::class.java).setAction(PosUpdateReceiver.ACTION_DOWNLOAD_UPDATE)
        val download = PendingIntent.getBroadcast(
            context,
            NOTIFICATION_ID,
            intent,
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE,
        )
        val notification = Notification.Builder(context, CHANNEL_ID)
            .setSmallIcon(R.drawable.ic_sucree)
            .setContentTitle("Actualización disponible")
            .setContentText("SucréePOS $version está lista para descargar.")
            .setStyle(Notification.BigTextStyle().bigText("Hay una nueva versión de SucréePOS ($version). Toca Descargar para mantener la app actualizada."))
            .setCategory(Notification.CATEGORY_STATUS)
            .setPriority(Notification.PRIORITY_HIGH)
            .setAutoCancel(true)
            .addAction(Notification.Action.Builder(null, "Descargar", download).build())
            .setContentIntent(download)
            .build()
        manager.notify(NOTIFICATION_ID, notification)
    }
}
