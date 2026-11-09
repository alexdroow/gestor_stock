package cl.sucree.pos

import android.app.AlarmManager
import android.app.DownloadManager
import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Activity
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.database.Cursor
import android.net.Uri
import android.os.Build
import android.os.Environment
import android.provider.Settings
import android.widget.Toast
import org.json.JSONObject
import java.net.HttpURLConnection
import java.net.URL
import java.util.Calendar

object PosUpdateManager {
    private const val PAGE_URL = "https://pasteleriasucree.cl/descargar-pos-android"
    private const val PREFS = "pos_app_updates"
    private const val LATEST_VERSION_KEY = "latest_version"
    private const val LATEST_APK_PATH_KEY = "latest_apk_path"
    private const val CHANNEL_ID = "pos_app_updates_v2"
    private const val NOTIFICATION_ID = 1490
    private const val DAILY_REQUEST_CODE = 1491
    private const val DOWNLOAD_ID_KEY = "download_id"
    private const val PENDING_INSTALL_ID_KEY = "pending_install_id"

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
            val manager = context.getSystemService(DownloadManager::class.java)
            val previousId = prefs.getLong(DOWNLOAD_ID_KEY, -1L)
            if (previousId > 0L) manager.remove(previousId)
            val downloadId = manager.enqueue(request)
            prefs.edit().putLong(DOWNLOAD_ID_KEY, downloadId).apply()
            true
        }.getOrDefault(false)
    }

    fun onDownloadComplete(context: Context, downloadId: Long) {
        val prefs = context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
        if (downloadId <= 0L || prefs.getLong(DOWNLOAD_ID_KEY, -1L) != downloadId) return
        val manager = context.getSystemService(DownloadManager::class.java)
        val query = DownloadManager.Query().setFilterById(downloadId)
        val status = runCatching {
            manager.query(query)?.use { cursor ->
                if (!cursor.moveToFirst()) null else readDownloadResult(cursor)
            }
        }.getOrNull()
        if (status == null || !status.success || status.size in 0L until 100_000L) {
            prefs.edit().remove(DOWNLOAD_ID_KEY).apply()
            Toast.makeText(context, "No se pudo descargar la actualizacion completa. Comprueba tu conexion e intenta otra vez.", Toast.LENGTH_LONG).show()
            return
        }
        val uri = manager.getUriForDownloadedFile(downloadId)
        val isApkZip = uri != null && runCatching {
            context.contentResolver.openInputStream(uri)?.use { input ->
                input.read() == 0x50 && input.read() == 0x4b
            } == true
        }.getOrDefault(false)
        if (!isApkZip) {
            prefs.edit().remove(DOWNLOAD_ID_KEY).apply()
            Toast.makeText(context, "El archivo descargado no parece ser una APK valida. No se instalara.", Toast.LENGTH_LONG).show()
            return
        }
        prefs.edit().remove(DOWNLOAD_ID_KEY).putLong(PENDING_INSTALL_ID_KEY, downloadId).apply()
        notifyReadyToInstall(context, downloadId)
    }

    fun handleInstallRequest(activity: Activity, intent: Intent?): Boolean {
        if (intent?.action != PosUpdateReceiver.ACTION_INSTALL_UPDATE) return false
        val downloadId = intent.getLongExtra(PosUpdateReceiver.EXTRA_DOWNLOAD_ID, -1L)
        if (downloadId <= 0L) return true
        installDownloadedUpdate(activity, downloadId)
        return true
    }

    fun handleInstallPermissionResult(activity: Activity, requestCode: Int, resultCode: Int) {
        if (requestCode != PosUpdateReceiver.INSTALL_PERMISSION_REQUEST) return
        val prefs = activity.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
        val downloadId = prefs.getLong(PENDING_INSTALL_ID_KEY, -1L)
        prefs.edit().remove(PENDING_INSTALL_ID_KEY).apply()
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O && activity.packageManager.canRequestPackageInstalls()) {
            if (downloadId > 0L) launchInstaller(activity, downloadId)
        } else {
            Toast.makeText(activity, "Para instalar la actualizacion, permite a SucreePOS instalar aplicaciones desde esta fuente.", Toast.LENGTH_LONG).show()
        }
    }

    private data class DownloadResult(val success: Boolean, val size: Long)

    private fun readDownloadResult(cursor: Cursor): DownloadResult {
        val status = cursor.getInt(cursor.getColumnIndexOrThrow(DownloadManager.COLUMN_STATUS))
        val size = cursor.getLong(cursor.getColumnIndexOrThrow(DownloadManager.COLUMN_TOTAL_SIZE_BYTES))
        return DownloadResult(status == DownloadManager.STATUS_SUCCESSFUL, size)
    }

    private fun installDownloadedUpdate(activity: Activity, downloadId: Long) {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O && !activity.packageManager.canRequestPackageInstalls()) {
            activity.getSharedPreferences(PREFS, Context.MODE_PRIVATE).edit()
                .putLong(PENDING_INSTALL_ID_KEY, downloadId).apply()
            Toast.makeText(activity, "Autoriza a SucreePOS a instalar la actualizacion. Al volver, abriremos el instalador.", Toast.LENGTH_LONG).show()
            val settingsIntent = Intent(Settings.ACTION_MANAGE_UNKNOWN_APP_SOURCES, Uri.parse("package:${activity.packageName}"))
            try {
                activity.startActivityForResult(settingsIntent, PosUpdateReceiver.INSTALL_PERMISSION_REQUEST)
            } catch (_: Exception) {
                activity.getSharedPreferences(PREFS, Context.MODE_PRIVATE).edit().remove(PENDING_INSTALL_ID_KEY).apply()
                Toast.makeText(activity, "Abre Ajustes de Android y permite instalar aplicaciones desconocidas para SucreePOS.", Toast.LENGTH_LONG).show()
            }
            return
        }
        launchInstaller(activity, downloadId)
    }

    private fun launchInstaller(context: Context, downloadId: Long) {
        val uri = context.getSystemService(DownloadManager::class.java).getUriForDownloadedFile(downloadId)
        if (uri == null) {
            Toast.makeText(context, "No se encontro el archivo descargado. Vuelve a descargar la actualizacion.", Toast.LENGTH_LONG).show()
            return
        }
        try {
            val install = Intent(Intent.ACTION_VIEW).apply {
                setDataAndType(uri, "application/vnd.android.package-archive")
                addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION or Intent.FLAG_ACTIVITY_NEW_TASK)
            }
            context.startActivity(install)
        } catch (_: Exception) {
            Toast.makeText(context, "Android no pudo abrir el instalador. Busca el APK en Descargas e intentalo de nuevo.", Toast.LENGTH_LONG).show()
        }
    }

    private fun notifyReadyToInstall(context: Context, downloadId: Long) {
        if (Build.VERSION.SDK_INT >= 33 &&
            context.checkSelfPermission(android.Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED
        ) {
            Toast.makeText(context, "Descarga completa. Abre SucreePOS para continuar con la instalacion.", Toast.LENGTH_LONG).show()
            return
        }
        val manager = context.getSystemService(NotificationManager::class.java)
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O && manager.getNotificationChannel(CHANNEL_ID) == null) {
            manager.createNotificationChannel(NotificationChannel(CHANNEL_ID, "Actualizaciones de SucreePOS", NotificationManager.IMPORTANCE_HIGH))
        }
        val openInstaller = Intent(context, MainActivity::class.java).apply {
            action = PosUpdateReceiver.ACTION_INSTALL_UPDATE
            putExtra(PosUpdateReceiver.EXTRA_DOWNLOAD_ID, downloadId)
            addFlags(Intent.FLAG_ACTIVITY_CLEAR_TOP or Intent.FLAG_ACTIVITY_SINGLE_TOP)
        }
        val contentIntent = PendingIntent.getActivity(
            context, NOTIFICATION_ID, openInstaller,
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE,
        )
        val notification = Notification.Builder(context, CHANNEL_ID)
            .setSmallIcon(R.drawable.ic_sucree)
            .setContentTitle("Actualizacion descargada")
            .setContentText("Toca para abrir el instalador de SucreePOS.")
            .setCategory(Notification.CATEGORY_STATUS)
            .setAutoCancel(true)
            .setContentIntent(contentIntent)
            .addAction(Notification.Action.Builder(null, "Instalar", contentIntent).build())
            .build()
        runCatching { manager.notify(NOTIFICATION_ID + 1, notification) }
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
