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
import android.os.Build
import org.json.JSONArray
import org.json.JSONObject
import java.net.HttpURLConnection
import java.net.URL
import java.text.SimpleDateFormat
import java.util.Locale
import java.util.TimeZone
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

data class PosMonitoringStatus(
    val pendingOrders: Int,
    val pendingQuotes: Int,
    val lastUpdatedMillis: Long,
    val connected: Boolean,
    val ordersAvailable: Boolean,
    val quotesAvailable: Boolean,
    val connectedCustomers: Int = 0,
    val activeStoreCarts: Int = 0,
    val storeActivityAvailable: Boolean = false,
)

class OrderAlertReceiver : BroadcastReceiver() {
    override fun onReceive(context: Context, intent: Intent?) {
        when (intent?.action) {
            Intent.ACTION_BOOT_COMPLETED -> {
                val prefs = context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
                if (prefs.getBoolean(MONITOR_KEY, false)) schedulePoll(context, 5_000L)
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
        private const val MONITOR_KEY = "monitor_enabled"
        private const val ALERTS_KEY = "active_alerts"
        private const val ACK_KEY = "acknowledged_alerts"
        private const val ORDER_IDS_KEY = "known_order_ids"
        private const val QUOTE_IDS_KEY = "known_quote_ids"
        private const val STORE_CART_IDS_KEY = "known_active_store_cart_ids"
        private const val STORE_CART_BASELINE_KEY = "store_cart_baseline_created"
        private const val STORE_CONNECTED_KEY = "store_connected_customers"
        private const val STORE_CART_COUNT_KEY = "store_active_cart_count"
        private const val STORE_ACTIVITY_AVAILABLE_KEY = "store_activity_available"
        private const val UNRESOLVED_QUOTES_KEY = "new_unconfirmed_quote_ids"
        private const val ORDER_BASELINE_KEY = "order_baseline_created"
        private const val QUOTE_BASELINE_KEY = "quote_baseline_created"
        private const val CHANNEL_ID = "pos_attention_alerts_v3"
        private const val STORE_ACTIVITY_CHANNEL_ID = "pos_store_activity_v1"
        private const val POLL_INTERVAL_MS = 60 * 1000L
        private const val ALERT_REPEAT_INTERVAL_MS = 5 * 60 * 1000L
        private const val RECENT_RESERVATION_WINDOW_MS = 15 * 60 * 1000L
        private const val ORDERS_COUNT_KEY = "pending_orders_count"
        private const val QUOTES_COUNT_KEY = "pending_quotes_count"
        private const val LAST_UPDATE_KEY = "monitor_last_update"
        private const val CONNECTED_KEY = "monitor_connected"
        private const val ORDERS_AVAILABLE_KEY = "monitor_orders_available"
        private const val QUOTES_AVAILABLE_KEY = "monitor_quotes_available"
        private val pollInProgress = AtomicBoolean(false)
        private val pollExecutor = Executors.newFixedThreadPool(3)

        fun startMonitoring(context: Context, cookie: String) {
            if (cookie.length > 16_000) return
            val prefs = context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
            prefs.edit().putBoolean(MONITOR_KEY, true).apply()
            if (cookie.isNotBlank()) prefs.edit().putString(COOKIE_KEY, cookie).apply()
            val service = Intent(context, PosMonitoringService::class.java)
            try {
                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) context.startForegroundService(service)
                else context.startService(service)
            } catch (_: Exception) {
                // The scheduled poll remains available if Android rejects foreground startup.
            }
            schedulePoll(context, POLL_INTERVAL_MS)
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
            scheduleRepeat(context, key, ALERT_REPEAT_INTERVAL_MS)
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
                    refreshStatus(context)
                } catch (_: Exception) {
                } finally {
                    pending.finish()
                }
            }.start()
        }

        fun refreshStatus(context: Context): PosMonitoringStatus {
            val prefs = context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
            val cookie = prefs.getString(COOKIE_KEY, null)?.takeIf { it.isNotBlank() }
            if (!pollInProgress.compareAndSet(false, true)) return readMonitoringStatus(context)
            try {
                val ordersFuture: Future<JSONArray?>? = cookie?.let {
                    pollExecutor.submit<JSONArray?> { fetchEndpoint("/api/tienda/admin/pedidos-chat-activos", it, "pedidos") }
                }
                val agendaFuture = pollExecutor.submit<JSONArray?> { fetchEndpoint("/api/pos/monitor/agenda", cookie, "eventos") }
                val activityFuture: Future<JSONObject?>? = cookie?.let {
                    pollExecutor.submit<JSONObject?> { fetchActivityEndpoint(it) }
                }
                val orders = ordersFuture?.let { runCatching { it.get(10, TimeUnit.SECONDS) }.getOrNull() }
                val agenda = runCatching { agendaFuture.get(10, TimeUnit.SECONDS) }.getOrNull()
                val activity = activityFuture?.let { runCatching { it.get(10, TimeUnit.SECONDS) }.getOrNull() }
                var successful = false
                if (orders != null) {
                    prefs.edit().putInt(ORDERS_COUNT_KEY, processOrders(context, orders)).apply()
                    successful = true
                }
                if (agenda != null) {
                    prefs.edit().putInt(QUOTES_COUNT_KEY, processQuotes(context, agenda)).apply()
                    successful = true
                }
                if (activity != null) {
                    val summary = activity.optJSONObject("resumen") ?: JSONObject()
                    processStoreActivity(context, activity)
                    prefs.edit()
                        .putInt(STORE_CONNECTED_KEY, summary.optInt("conectados", 0).coerceAtLeast(0))
                        .putInt(STORE_CART_COUNT_KEY, summary.optInt("carritos_activos", 0).coerceAtLeast(0))
                        .putBoolean(STORE_ACTIVITY_AVAILABLE_KEY, true)
                        .apply()
                    successful = true
                } else {
                    prefs.edit().putBoolean(STORE_ACTIVITY_AVAILABLE_KEY, false).apply()
                }
                prefs.edit()
                    .putBoolean(ORDERS_AVAILABLE_KEY, orders != null)
                    .putBoolean(QUOTES_AVAILABLE_KEY, agenda != null)
                    .putBoolean(CONNECTED_KEY, successful)
                    .apply()
                if (successful) prefs.edit().putLong(LAST_UPDATE_KEY, System.currentTimeMillis()).apply()
                return readMonitoringStatus(context)
            } finally {
                pollInProgress.set(false)
            }
        }

        fun readMonitoringStatus(context: Context): PosMonitoringStatus {
            val prefs = context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
            return PosMonitoringStatus(
                pendingOrders = prefs.getInt(ORDERS_COUNT_KEY, 0),
                pendingQuotes = prefs.getInt(QUOTES_COUNT_KEY, 0),
                lastUpdatedMillis = prefs.getLong(LAST_UPDATE_KEY, 0L),
                connected = prefs.getBoolean(CONNECTED_KEY, false),
                ordersAvailable = prefs.getBoolean(ORDERS_AVAILABLE_KEY, false),
                quotesAvailable = prefs.getBoolean(QUOTES_AVAILABLE_KEY, false),
                connectedCustomers = prefs.getInt(STORE_CONNECTED_KEY, 0),
                activeStoreCarts = prefs.getInt(STORE_CART_COUNT_KEY, 0),
                storeActivityAvailable = prefs.getBoolean(STORE_ACTIVITY_AVAILABLE_KEY, false),
            )
        }

        private fun fetchActivityEndpoint(cookie: String): JSONObject? {
            val connection = (URL("https://pasteleriasucree.cl/api/tienda/admin/actividad").openConnection() as HttpURLConnection).apply {
                requestMethod = "GET"
                connectTimeout = 2_000
                readTimeout = 8_000
                useCaches = false
                setRequestProperty("Cookie", cookie)
                setRequestProperty("Accept", "application/json")
                setRequestProperty("Cache-Control", "no-cache")
                setRequestProperty("User-Agent", "SucreePOS-Android")
            }
            try {
                if (connection.responseCode !in 200..299) return null
                val root = JSONObject(connection.inputStream.bufferedReader(Charsets.UTF_8).use { it.readText() })
                if (!root.optBoolean("success", false)) return null
                return root
            } finally {
                connection.disconnect()
            }
        }

        private fun processStoreActivity(context: Context, activity: JSONObject) {
            val sessions = activity.optJSONArray("carritos_detalle") ?: JSONArray()
            val activeIds = linkedSetOf<String>()
            for (i in 0 until sessions.length()) {
                val session = sessions.optJSONObject(i) ?: continue
                session.optString("session_id").takeIf { it.isNotBlank() }?.let(activeIds::add)
            }

            val prefs = context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
            val previouslyActive = readSet(prefs.getString(STORE_CART_IDS_KEY, "[]"))
            val baselineCreated = prefs.getBoolean(STORE_CART_BASELINE_KEY, false)
            val summary = activity.optJSONObject("resumen") ?: JSONObject()
            val connected = summary.optInt("conectados", 0).coerceAtLeast(0)
            val activeCarts = summary.optInt("carritos_activos", 0).coerceAtLeast(0)
            if (baselineCreated) {
                (activeIds - previouslyActive).forEach { sessionId ->
                    notifyStoreCartStarted(context, sessionId, connected, activeCarts)
                }
            }
            prefs.edit()
                .putString(STORE_CART_IDS_KEY, JSONArray(activeIds.toList()).toString())
                .putBoolean(STORE_CART_BASELINE_KEY, true)
                .apply()
        }

        private fun notifyStoreCartStarted(context: Context, sessionId: String, connected: Int, activeCarts: Int) {
            if (Build.VERSION.SDK_INT >= 33 && context.checkSelfPermission(android.Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED) return
            val notificationKey = "store_cart:$sessionId:${System.currentTimeMillis()}"

            val manager = context.getSystemService(NotificationManager::class.java)
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                manager.createNotificationChannel(NotificationChannel(
                    STORE_ACTIVITY_CHANNEL_ID,
                    "Actividad de la tienda",
                    NotificationManager.IMPORTANCE_HIGH,
                ).apply {
                    description = "Aviso cuando un cliente empieza a preparar un carrito en la tienda."
                    enableVibration(true)
                    vibrationPattern = longArrayOf(0, 250, 120, 250)
                    val sound = RingtoneManager.getDefaultUri(RingtoneManager.TYPE_NOTIFICATION)
                    val audio = AudioAttributes.Builder()
                        .setUsage(AudioAttributes.USAGE_NOTIFICATION)
                        .setContentType(AudioAttributes.CONTENT_TYPE_SONIFICATION)
                        .build()
                    setSound(sound, audio)
                })
            }
            val open = Intent(context, MainActivity::class.java).apply {
                flags = Intent.FLAG_ACTIVITY_CLEAR_TOP or Intent.FLAG_ACTIVITY_SINGLE_TOP
            }
            val contentIntent = PendingIntent.getActivity(
                context, notificationKey.hashCode(), open,
                PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE,
            )
            val notification = Notification.Builder(context, STORE_ACTIVITY_CHANNEL_ID)
                .setSmallIcon(R.drawable.ic_sucree)
                .setContentTitle("Cliente preparando un carrito")
                .setContentText("$connected conectado(s) · $activeCarts carrito(s) activo(s) en tienda.")
                .setStyle(Notification.BigTextStyle().bigText("Un cliente empezó a preparar su carrito en la tienda. Hay $connected cliente(s) conectado(s) y $activeCarts carrito(s) activo(s). Abre Sucrée POS para revisar la actividad."))
                .setCategory(Notification.CATEGORY_EVENT)
                .setVisibility(Notification.VISIBILITY_PRIVATE)
                .setAutoCancel(true)
                .setContentIntent(contentIntent)
                .build()
            manager.notify(notificationKey.hashCode(), notification)
        }

        private fun fetchEndpoint(path: String, cookie: String?, key: String): JSONArray? {
            val connection = (URL("https://pasteleriasucree.cl$path").openConnection() as HttpURLConnection).apply {
                requestMethod = "GET"
                connectTimeout = 2_000
                readTimeout = 8_000
                useCaches = false
                if (cookie != null) setRequestProperty("Cookie", cookie)
                setRequestProperty("Accept", "application/json")
                setRequestProperty("Cache-Control", "no-cache")
                setRequestProperty("User-Agent", "SucreePOS-Android")
            }
            try {
                if (connection.responseCode !in 200..299) return null
                val body = connection.inputStream.bufferedReader(Charsets.UTF_8).use { it.readText() }
                val root = JSONObject(body)
                if (!root.optBoolean("success", true)) return null
                return root.optJSONArray(key) ?: JSONArray()
            } finally {
                connection.disconnect()
            }
        }

        private fun processOrders(context: Context, orders: JSONArray): Int {
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
            return active.length()
        }

        private fun processQuotes(context: Context, events: JSONArray): Int {
            val prefs = context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
            val known = readSet(prefs.getString(QUOTE_IDS_KEY, "[]"))
            val unresolved = readSet(prefs.getString(UNRESOLVED_QUOTES_KEY, "[]"))
            val baseline = prefs.getBoolean(QUOTE_BASELINE_KEY, false)
            val active = JSONArray()
            val allIds = mutableSetOf<String>()
            for (i in 0 until events.length()) {
                val event = events.optJSONObject(i) ?: continue
                val id = event.optString("id").takeIf { it.isNotBlank() } ?: continue
                allIds.add(id)
                val type = event.optString("tipo").trim().lowercase()
                val motive = event.optString("motivo")
                val onlineReservation = motive.contains("reserva cliente tienda online", true) ||
                    event.optString("codigo_operacion").startsWith("OPA-TI-", true)
                val isCocktail = type.contains("cocteler") || type == "cocktail"
                val cocktailReservation = isCocktail && (onlineReservation || event.optString("ingredientes").contains("reserva de cocteleria", true))
                val eventState = event.optString("estado").trim().lowercase()
                val pending = eventState in setOf("pendiente", "borrador") &&
                    type != "bloqueo" && (onlineReservation || cocktailReservation)
                if (pending) {
                    active.put(id)
                    val firstPollRecentReservation = !baseline && onlineReservation &&
                        isRecentlyCreatedReservation(event.optString("creado"))
                    if ((baseline && id !in known) || firstPollRecentReservation) {
                        unresolved.add(id)
                        val customer = event.optString("cliente", "Cliente")
                        val date = event.optString("fecha")
                        val title = if (isCocktail) "Nuevo pedido de cocteleria" else "Nueva cotizacion de torta"
                        notifyNew(context, "quote", id, title, "$customer · $date · Requiere confirmacion")
                    }
                } else {
                    unresolved.remove(id)
                }
            }
            syncActive(context, "quote", active)
            val activeIds = (0 until active.length()).map { active.optString(it) }.toSet()
            unresolved.retainAll(activeIds)
            val nextKnown = (known + allIds).toList().sorted().takeLast(5_000)
            prefs.edit()
                .putString(QUOTE_IDS_KEY, JSONArray(nextKnown.toList()).toString())
                .putString(UNRESOLVED_QUOTES_KEY, JSONArray(unresolved.toList()).toString())
                .putBoolean(QUOTE_BASELINE_KEY, true)
                .apply()
            return unresolved.size
        }

        private fun isRecentlyCreatedReservation(created: String): Boolean {
            if (created.isBlank()) return false
            return runCatching {
                val format = SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US).apply {
                    timeZone = TimeZone.getTimeZone("UTC")
                    isLenient = false
                }
                val createdAt = format.parse(created)?.time ?: return false
                val age = System.currentTimeMillis() - createdAt
                age in 0..RECENT_RESERVATION_WINDOW_MS
            }.getOrDefault(false)
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
                    description = "Avisos con sonido y vibración de pedidos y reservas pendientes. Puedes ajustar estos avisos en Android."
                    enableVibration(true)
                    vibrationPattern = longArrayOf(0, 350, 180, 350)
                    val sound = RingtoneManager.getDefaultUri(RingtoneManager.TYPE_NOTIFICATION)
                    val audio = AudioAttributes.Builder()
                        .setUsage(AudioAttributes.USAGE_NOTIFICATION)
                        .setContentType(AudioAttributes.CONTENT_TYPE_SONIFICATION)
                        .build()
                    setSound(sound, audio)
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
            val notification = Notification.Builder(context, CHANNEL_ID)
                .setSmallIcon(R.drawable.ic_sucree)
                .setContentTitle(title)
                .setContentText(body)
                .setStyle(Notification.BigTextStyle().bigText("$body\nToca para abrir y atender. Si sigue sin abrirse, volveremos a avisarte en 5 minutos."))
                .setCategory(Notification.CATEGORY_EVENT)
                .setVisibility(Notification.VISIBILITY_PUBLIC)
                .setPriority(Notification.PRIORITY_MAX)
                .setAutoCancel(true)
                .setContentIntent(contentIntent)
                .build()
            manager.notify(key.hashCode(), notification)
        }

        private fun repeatAlert(context: Context, intent: Intent) {
            val key = intent.getStringExtra("alert_key") ?: return
            val exists = readObjectArray(context.getSharedPreferences(PREFS, Context.MODE_PRIVATE).getString(ALERTS_KEY, "[]"))
                .any { it.optString("key") == key }
            if (!exists) return
            showAlert(context, key)
            scheduleRepeat(context, key, ALERT_REPEAT_INTERVAL_MS)
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
