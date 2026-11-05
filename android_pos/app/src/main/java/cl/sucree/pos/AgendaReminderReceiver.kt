package cl.sucree.pos

import android.app.AlarmManager
import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.media.AudioAttributes
import android.media.RingtoneManager
import android.net.Uri
import android.os.Build
import android.provider.Settings
import android.widget.Toast
import org.json.JSONArray
import org.json.JSONObject

class AgendaReminderReceiver : BroadcastReceiver() {
    override fun onReceive(context: Context, intent: Intent?) {
        val action = intent?.action ?: return
        if (action == Intent.ACTION_BOOT_COMPLETED) rescheduleSaved(context)
        else if (action.startsWith("$ACTION_FIRE:")) showReminder(context, intent)
    }

    companion object {
        const val ACTION_FIRE = "cl.sucree.pos.action.AGENDA_REMINDER"
        // Use a fresh channel so Android applies sound/vibration settings even on upgraded installs.
        private const val CHANNEL_ID = "agenda_personal_reminders_v2"
        private const val PREFS = "agenda_personal_reminders"
        private const val REMINDERS_KEY = "active_reminders"

        fun syncFromAgenda(context: Context, reminders: JSONArray) {
            val prefs = context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
            val previous = try { JSONArray(prefs.getString(REMINDERS_KEY, "[]") ?: "[]") } catch (_: Exception) { JSONArray() }
            val currentIds = mutableSetOf<Int>()
            for (i in 0 until reminders.length()) {
                val item = reminders.optJSONObject(i) ?: continue
                val id = item.optInt("id", 0)
                val due = item.optLong("due_at_ms", 0L)
                if (id <= 0 || due <= System.currentTimeMillis()) continue
                currentIds.add(id)
            }
            for (i in 0 until previous.length()) {
                val oldId = previous.optJSONObject(i)?.optInt("id", 0) ?: 0
                if (oldId > 0 && oldId !in currentIds) cancel(context, oldId)
            }
            prefs.edit().putString(REMINDERS_KEY, reminders.toString()).apply()
            for (i in 0 until reminders.length()) {
                val item = reminders.optJSONObject(i) ?: continue
                schedule(context, item)
            }

            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S && reminders.length() > 0 &&
                !context.getSystemService(AlarmManager::class.java).canScheduleExactAlarms()
            ) {
                val asked = prefs.getBoolean("exact_access_prompted", false)
                if (!asked && context is MainActivity) {
                    prefs.edit().putBoolean("exact_access_prompted", true).apply()
                    Toast.makeText(context, "Activa las alarmas exactas para recibir recordatorios puntuales.", Toast.LENGTH_LONG).show()
                    try {
                        context.startActivity(Intent(Settings.ACTION_REQUEST_SCHEDULE_EXACT_ALARM, Uri.parse("package:${context.packageName}")))
                    } catch (_: Exception) {
                        Toast.makeText(context, "Puedes activar Alarmas y recordatorios desde Ajustes de Android.", Toast.LENGTH_LONG).show()
                    }
                }
            }
            if (reminders.length() > 0 && context is MainActivity) {
                context.ensureAgendaReminderNotificationPermission()
            }
        }

        fun rescheduleSaved(context: Context) {
            val raw = context.getSharedPreferences(PREFS, Context.MODE_PRIVATE).getString(REMINDERS_KEY, "[]") ?: "[]"
            val reminders = try { JSONArray(raw) } catch (_: Exception) { JSONArray() }
            for (i in 0 until reminders.length()) {
                val item = reminders.optJSONObject(i) ?: continue
                schedule(context, item)
            }
        }

        private fun schedule(context: Context, reminder: JSONObject) {
            val id = reminder.optInt("id", 0)
            val due = reminder.optLong("due_at_ms", 0L)
            if (id <= 0 || due <= System.currentTimeMillis()) return
            val alarmManager = context.getSystemService(AlarmManager::class.java)
            val pending = pendingIntent(context, id, reminder)
            try {
                if (Build.VERSION.SDK_INT < Build.VERSION_CODES.S || alarmManager.canScheduleExactAlarms()) {
                    alarmManager.setExactAndAllowWhileIdle(AlarmManager.RTC_WAKEUP, due, pending)
                } else {
                    alarmManager.setAndAllowWhileIdle(AlarmManager.RTC_WAKEUP, due, pending)
                }
            } catch (_: SecurityException) {
                alarmManager.setAndAllowWhileIdle(AlarmManager.RTC_WAKEUP, due, pending)
            }
        }

        private fun pendingIntent(context: Context, id: Int, reminder: JSONObject): PendingIntent {
            val fire = Intent(context, AgendaReminderReceiver::class.java).apply {
                action = "$ACTION_FIRE:$id"
                putExtra("reminder_id", id)
                putExtra("reminder_title", reminder.optString("title", "Recordatorio personal"))
                putExtra("reminder_body", reminder.optString("body", ""))
            }
            return PendingIntent.getBroadcast(
                context, id, fire,
                PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE,
            )
        }

        private fun cancel(context: Context, id: Int) {
            val fire = Intent(context, AgendaReminderReceiver::class.java).apply { action = "$ACTION_FIRE:$id" }
            PendingIntent.getBroadcast(
                context, id, fire,
                PendingIntent.FLAG_NO_CREATE or PendingIntent.FLAG_IMMUTABLE,
            )?.let { context.getSystemService(AlarmManager::class.java).cancel(it); it.cancel() }
        }

        private fun showReminder(context: Context, intent: Intent) {
            if (Build.VERSION.SDK_INT >= 33 &&
                context.checkSelfPermission(android.Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED
            ) return
            val manager = context.getSystemService(NotificationManager::class.java)
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                val sound = RingtoneManager.getDefaultUri(RingtoneManager.TYPE_NOTIFICATION)
                val audio = AudioAttributes.Builder()
                    .setUsage(AudioAttributes.USAGE_NOTIFICATION_EVENT)
                    .setContentType(AudioAttributes.CONTENT_TYPE_SONIFICATION)
                    .build()
                manager.createNotificationChannel(NotificationChannel(
                    CHANNEL_ID, "Recordatorios personales", NotificationManager.IMPORTANCE_HIGH,
                ).apply {
                    description = "Avisos de recordatorios personales de Agenda Sucree POS."
                    enableVibration(true)
                    vibrationPattern = longArrayOf(0, 350, 180, 350)
                    setSound(sound, audio)
                })
            }
            val id = intent.getIntExtra("reminder_id", 0)
            val openAgenda = Intent(context, MainActivity::class.java).apply {
                flags = Intent.FLAG_ACTIVITY_CLEAR_TOP or Intent.FLAG_ACTIVITY_SINGLE_TOP
                putExtra("show_reminders", true)
            }
            val contentIntent = PendingIntent.getActivity(
                context, id + 300_000, openAgenda,
                PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE,
            )
            val title = intent.getStringExtra("reminder_title").orEmpty().ifBlank { "Recordatorio personal" }
            val body = intent.getStringExtra("reminder_body").orEmpty().ifBlank { "Es hora de revisar tu recordatorio." }
            manager.notify(id, Notification.Builder(context, CHANNEL_ID)
                .setSmallIcon(R.drawable.ic_sucree)
                .setContentTitle(title)
                .setContentText(body)
                .setStyle(Notification.BigTextStyle().bigText(body))
                .setCategory(Notification.CATEGORY_REMINDER)
                .setDefaults(Notification.DEFAULT_SOUND or Notification.DEFAULT_VIBRATE)
                .setVibrate(longArrayOf(0, 350, 180, 350))
                .setPriority(Notification.PRIORITY_HIGH)
                .setAutoCancel(true)
                .setContentIntent(contentIntent)
                .build())
        }
    }
}
