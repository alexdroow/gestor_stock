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
import android.os.Build
import org.json.JSONArray
import org.json.JSONObject
import java.net.HttpURLConnection
import java.net.URL
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class OrderAlertReceiver : BroadcastReceiver() {
    override fun onReceive(context: Context, intent: Intent?) {
        when (intent?.action) {
            Intent.ACTION_BOOT_COMPLETED -> {
                val prefs = context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
                if (!prefs.getString(COOKIE_KEY, null).isNullOrBlank()) schedulePoll(context, 5_000L)
                repeatSavedAlerts(context)
            }
            ACTION_POLL -> pollInBackground(context, this)
            ACTION_REPEAT -> repeatAlert(context, intent)
        }
    }

    companion object {
        const val ACTION_POLL = "cl.sucree.pos.action.POLL_NEW_ORDERS"
        const val ACTION_REPEAT = "cl.sucree.pos.action.REPEAT_ORDER_ALERT"
        private const val PREFS = "pos_order_alerts"
        private const val COOKIE_KEY = "admin_cookie"
        private const val ALERTS_KEY = "active_alerts"
        private const val ACK_KEY = "acknowledged_alerts"
        private const val ORDER_IDS_KEY = "known_order_ids"
        private const val QUOTE_IDS_KEY = "known_quote_ids"
        private const val ORDER_BASELINE_KEY = "order_baseline_created"
        private const val QUOTE_BASELINE_KEY = "quote_baseline_created"
        private const val CHANNEL_ID = "pos_attention_alerts"
        private const val POLL_INTERVAL_MS = 5 * 60 * 1000L

        fun startMonitoring(context: Context, cookie: String) {
            if (cookie.isBlank() || cookie.length > 16_000) return
            context.getSharedPreferences(PREFS, Context.MODE_PRIVATE).edit().putString(COOKIE_KEY, cookie).apply()
            schedulePoll(context, 1_000L)
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S &&
                !context.getSystemService(AlarmManager::class.java).canScheduleExactAlarms()
            ) {
                val prefs = context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
                if (!prefs.getBoolean("exact_access_prompted", false) && context is MainActivity) {
                    prefs.edit().putBoolean("exact_access_prompted", true).apply()
                    try {
                        android.widget.Toast.makeText(context, "Para repetir los avisos cada 5 minutos, habilita Alarmas y recordatorios.", android.widget.Toast.LENGTH_LONG).show()
                        context.startActivity(Intent(android.provider.Settings.ACTION_REQUEST_SCHEDULE_EXACT_ALARM, android.net.Uri.parse("package:${context.packageName}")))
                    } catch (_: Exception) {
                        android.widget.Toast.makeText(context, "Activa Alarmas y recordatorios para recibir avisos puntuales.", android.widget.Toast.LENGTH_LONG).show()
                    }
                }
            }
        }

        fun notifyNew(context: Context, type: String, id: String, title: String, body: String) {
            if (type !in setOf("order", "quote") || id.isBlank() || id.length > 80) return
            val key = "$type:$id"
            val prefs = context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
            if (readSet(prefs.getString(ACK_KEY, "[]")).contains(key)) return
            val alerts = readObjectArray(prefs.getString(ALERTS_KEY, "[]"))
            if (alerts.any { it.optString("key") == key }) return
            alerts.add(JSONObject().apply {
                put("key", key)
                put("type", type)
                put("id", id)
                put("title", title.take(120))
                put("body", body.take(300))
            })
            prefs.edit().putString(ALERTS_KEY, JSONArray(alerts).toString()).apply()
            showAlert(context, key)
            scheduleRepeat(context, key, POLL_INTERVAL_MS)
        }

        fun syncActive(context: Context, type: String, activeIds: JSONArray) {
            if (type !in setOf("order", "quote")) return
            val activeKeys = (0 until activeIds.length()).mapNotNull { i ->
                activeIds.optString(i).takeIf { it.isNotBlank() }?.let { "$type:$it" }
            }.toSet()
            val prefs = context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
            val alerts = readObjectArray(prefs.getString(ALERTS_KEY, "[]"))
            val kept = mutableListOf<JSONObject>()
            val acked = readSet(prefs.getString(ACK_KEY, "[]"))
            val keptAcked = mutableSetOf<String>()
            alerts.forEach { item ->
                val key = item.optString("key")
                if (key.startsWith("$type:") && key !in activeKeys) {
                    cancelRepeat(context, key)
                    context.getSystemService(NotificationManager::class.java).cancel(key.hashCode())
                } else {
                    kept.add(item)
                }
            }
            acked.forEach { if (!it.startsWith("$type:") || it in activeKeys) keptAcked.add(it) }
            prefs.edit().putString(ALERTS_KEY, JSONArray(kept).toString()).putString(ACK_KEY, JSONArray(keptAcked.toList()).toString()).apply()
        }

        fun acknowledge(context: Context, key: String?) {
            if (key.isNullOrBlank() || !key.startsWith("order:") && !key.startsWith("quote:")) return
            val prefs = context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
            val alerts = readObjectArray(prefs.getString(ALERTS_KEY, "[]"))
            val kept = alerts.filter { it.optString("key") != key }
            val acked = readSet(prefs.getString(ACK_KEY, "[]")).apply { add(key) }
            prefs.edit().putString(ALERTS_KEY, JSONArray(kept).toString()).putString(ACK_KEY, JSONArray(acked.toList()).toString()).apply()
            cancelRepeat(context, key)
            context.getSystemService(NotificationManager::class.java).cancel(key.hashCode())
        }

        private fun pollInBackground(context: Context, receiver: BroadcastReceiver) {
            schedulePoll(context, POLL_INTERVAL_MS)
            val pending = receiver.goAsync()
            Thread {
                try {
                    val completed = CountDownLatch(2)
                    listOf(
                        { pollEndpoint(context, "/api/tienda/admin/pedidos-chat-activos", "orders") },
                        { pollEndpoint(context, "/api/agenda/eventos", "agenda") },
                    ).forEach { poll ->
                        Thread { try { poll() } catch (_: Exception) { } finally { completed.countDown() } }.start()
                    }
                    completed.await(7, TimeUnit.SECONDS)
                } catch (_: Exception) {
                } finally {
                    pending.finish()
                }
            }.start()
        }

        private fun pollEndpoint(context: Context, path: String, kind: String) {
            val prefs = context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
            val cookie = prefs.getString(COOKIE_KEY, null)?.takeIf { it.isNotBlank() } ?: return
            val connection = (URL("https://pasteleriasucree.cl$path").openConnection() as HttpURLConnection).apply {
                requestMethod = "GET"
                connectTimeout = 2_500
                readTimeout = 3_500
                setRequestProperty("Cookie", cookie)
                setRequestProperty("Accept", "application/json")
                setRequestProperty("User-Agent", "SucreePOS-Android")
            }
            try {
                if (connection.responseCode !in 200..299) return
                val body = connection.inputStream.bufferedReader(Charsets.UTF_8).use { it.readText() }
                val root = JSONObject(body)
                if (root.optBoolean("success", true).not()) return
                if (kind == "orders") processOrders(context, root.optJSONArray("pedidos") ?: JSONArray())
                else processQuotes(context, root.optJSONArray("eventos") ?: JSONArray())
            } finally {
                connection.disconnect()
            }
        }

        private fun processOrders(context: Context, orders: JSONArray) {
            val prefs = context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
            val known = readSet(prefs.getString(ORDER_IDS_KEY, "[]"))
            val baseline = prefs.getBoolean(ORDER_BASELINE_KEY, false)
            val active = JSONArray()
            val allIds = mutableSetOf<String>()
            for (i in 0 until orders.length()) {
                val order = orders.optJSONObject(i) ?: continue
                val id = order.optString("id").takeIf { it.isNotBlank() } ?: continue
                allIds.add(id)
                val state = order.optString("pedido_estado").lowercase()
                if (state in setOf("recibido", "confirmado")) {
                    active.put(id)
                    if (baseline && id !in known) {
                        notifyNew(context, "order", id, "Nuevo pedido #$id", "${order.optString("cliente_nombre", "Cliente")} · Requiere atención")
                    }
                }
            }
            syncActive(context, "order", active)
            val nextKnown = (known + allIds).toList().sorted().takeLast(2_000)
            prefs.edit().putString(ORDER_IDS_KEY, JSONArray(nextKnown.toList()).toString()).putBoolean(ORDER_BASELINE_KEY, true).apply()
        }

        private fun processQuotes(context: Context, events: JSONArray) {
            val prefs = context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
            val known = readSet(prefs.getString(QUOTE_IDS_KEY, "[]"))
            val baseline = prefs.getBoolean(QUOTE_BASELINE_KEY, false)
            val active = JSONArray()
            val allIds = mutableSetOf<String>()
            for (i in 0 until events.length()) {
                val event = events.optJSONObject(i) ?: continue
                val id = event.optString("id").takeIf { it.isNotBlank() } ?: continue
                allIds.add(id)
                val pending = event.optString("estado").equals("pendiente", true) &&
                    !event.optString("tipo").equals("bloqueo", true) &&
                    event.optString("motivo").contains("reserva cliente tienda online", true)
                if (pending) {
                    active.put(id)
                    if (baseline && id !in known) {
                        val customer = event.optString("cliente", "Cliente")
                        val date = event.optString("fecha")
                        notifyNew(context, "quote", id, "Nueva cotización de torta", "$customer · $date · Requiere confirmación")
                    }
                }
            }
            syncActive(context, "quote", active)
            val nextKnown = (known + allIds).toList().sorted().takeLast(5_000)
            prefs.edit().putString(QUOTE_IDS_KEY, JSONArray(nextKnown.toList()).toString()).putBoolean(QUOTE_BASELINE_KEY, true).apply()
        }

        private fun showAlert(context: Context, key: String) {
            if (Build.VERSION.SDK_INT >= 33 && context.checkSelfPermission(android.Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED) return
            val item = readObjectArray(context.getSharedPreferences(PREFS, Context.MODE_PRIVATE).getString(ALERTS_KEY, "[]"))
                .firstOrNull { it.optString("key") == key } ?: return
            val type = item.optString("type")
            val id = item.optString("id")
            val manager = context.getSystemService(NotificationManager::class.java)
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                manager.createNotificationChannel(NotificationChannel(CHANNEL_ID, "Pedidos y cotizaciones", NotificationManager.IMPORTANCE_HIGH).apply {
                    description = "Avisos repetidos de pedidos de tienda y cotizaciones de tortas sin atender."
                    enableVibration(true)
                })
            }
            val open = Intent(context, MainActivity::class.java).apply {
                flags = Intent.FLAG_ACTIVITY_CLEAR_TOP or Intent.FLAG_ACTIVITY_SINGLE_TOP
                putExtra("attention_key", key)
                putExtra("show_orders", type == "order")
                putExtra("show_agenda_quotes", type == "quote")
            }
            val contentIntent = PendingIntent.getActivity(context, key.hashCode(), open, PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE)
            val title = item.optString("title", "Pedido pendiente")
            val body = item.optString("body", "Abre Sucrée POS para atenderlo.")
            manager.cancel(key.hashCode())
            manager.notify(key.hashCode(), Notification.Builder(context, CHANNEL_ID)
                .setSmallIcon(R.drawable.ic_sucree)
                .setContentTitle(title)
                .setContentText(body)
                .setStyle(Notification.BigTextStyle().bigText("$body\nToca para abrir y atender. Si sigue sin abrirse, volveremos a avisarte en 5 minutos."))
                .setCategory(Notification.CATEGORY_EVENT)
                .setAutoCancel(true)
                .setContentIntent(contentIntent)
                .build())
        }

        private fun repeatAlert(context: Context, intent: Intent) {
            val key = intent.getStringExtra("alert_key") ?: return
            val exists = readObjectArray(context.getSharedPreferences(PREFS, Context.MODE_PRIVATE).getString(ALERTS_KEY, "[]"))
                .any { it.optString("key") == key }
            if (!exists) return
            showAlert(context, key)
            scheduleRepeat(context, key, POLL_INTERVAL_MS)
        }

        private fun repeatSavedAlerts(context: Context) {
            val alerts = readObjectArray(context.getSharedPreferences(PREFS, Context.MODE_PRIVATE).getString(ALERTS_KEY, "[]"))
            alerts.forEach { item ->
                val key = item.optString("key")
                if (key.isNotBlank()) scheduleRepeat(context, key, 2_000L)
            }
        }

        private fun schedulePoll(context: Context, delay: Long) {
            val intent = Intent(context, OrderAlertReceiver::class.java).setAction(ACTION_POLL)
            val pending = PendingIntent.getBroadcast(context, 81001, intent, PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE)
            val manager = context.getSystemService(AlarmManager::class.java)
            val at = System.currentTimeMillis() + delay
            if (Build.VERSION.SDK_INT < Build.VERSION_CODES.S || manager.canScheduleExactAlarms()) manager.setExactAndAllowWhileIdle(AlarmManager.RTC_WAKEUP, at, pending)
            else manager.setAndAllowWhileIdle(AlarmManager.RTC_WAKEUP, at, pending)
        }

        private fun scheduleRepeat(context: Context, key: String, delay: Long) {
            val intent = Intent(context, OrderAlertReceiver::class.java).apply {
                action = ACTION_REPEAT
                putExtra("alert_key", key)
            }
            val pending = PendingIntent.getBroadcast(context, key.hashCode(), intent, PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE)
            val manager = context.getSystemService(AlarmManager::class.java)
            val at = System.currentTimeMillis() + delay
            if (Build.VERSION.SDK_INT < Build.VERSION_CODES.S || manager.canScheduleExactAlarms()) manager.setExactAndAllowWhileIdle(AlarmManager.RTC_WAKEUP, at, pending)
            else manager.setAndAllowWhileIdle(AlarmManager.RTC_WAKEUP, at, pending)
        }

        private fun cancelRepeat(context: Context, key: String) {
            val intent = Intent(context, OrderAlertReceiver::class.java).apply { action = ACTION_REPEAT; putExtra("alert_key", key) }
            PendingIntent.getBroadcast(context, key.hashCode(), intent, PendingIntent.FLAG_NO_CREATE or PendingIntent.FLAG_IMMUTABLE)?.let {
                context.getSystemService(AlarmManager::class.java).cancel(it)
                it.cancel()
            }
        }

        private fun readSet(raw: String?): MutableSet<String> = try {
            val array = JSONArray(raw ?: "[]")
            (0 until array.length()).mapNotNull { array.optString(it).takeIf(String::isNotBlank) }.toMutableSet()
        } catch (_: Exception) { mutableSetOf() }

        private fun readObjectArray(raw: String?): MutableList<JSONObject> = try {
            val array = JSONArray(raw ?: "[]")
            (0 until array.length()).mapNotNull { array.optJSONObject(it) }.toMutableList()
        } catch (_: Exception) { mutableListOf() }
    }
}
