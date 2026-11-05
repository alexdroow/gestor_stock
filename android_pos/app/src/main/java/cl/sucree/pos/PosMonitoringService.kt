package cl.sucree.pos

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.Context
import android.content.Intent
import android.content.pm.ServiceInfo
import android.os.Build
import android.os.Handler
import android.os.IBinder
import android.os.Looper
import android.text.format.DateFormat
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

class PosMonitoringService : Service() {
    private val mainHandler = Handler(Looper.getMainLooper())
    private val scheduler = Executors.newSingleThreadScheduledExecutor()
    private var pollTask: ScheduledFuture<*>? = null

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        createMonitoringChannel()
        startAsForeground(createNotification(OrderAlertReceiver.readMonitoringStatus(this)))
        if (pollTask == null || pollTask?.isCancelled == true) {
            pollTask = scheduler.scheduleAtFixedRate({
                val status = OrderAlertReceiver.refreshStatus(applicationContext)
                mainHandler.post {
                    getSystemService(NotificationManager::class.java)
                        .notify(NOTIFICATION_ID, createNotification(status))
                }
            }, 0L, POLL_INTERVAL_SECONDS, TimeUnit.SECONDS)
        }
        return START_STICKY
    }

    private fun startAsForeground(notification: Notification) {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
            startForeground(NOTIFICATION_ID, notification, ServiceInfo.FOREGROUND_SERVICE_TYPE_DATA_SYNC)
        } else {
            startForeground(NOTIFICATION_ID, notification)
        }
    }

    private fun createMonitoringChannel() {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.O) return
        val manager = getSystemService(NotificationManager::class.java)
        if (manager.getNotificationChannel(CHANNEL_ID) != null) return
        manager.createNotificationChannel(NotificationChannel(
            CHANNEL_ID,
            "SucréePOS en segundo plano",
            NotificationManager.IMPORTANCE_LOW,
        ).apply {
            description = "Estado persistente del monitoreo de pedidos y reservas."
            setSound(null, null)
            enableVibration(false)
            setShowBadge(false)
        })
    }

    private fun createNotification(status: PosMonitoringStatus): Notification {
        val openApp = activityPendingIntent()
        val lastUpdate = if (status.lastUpdatedMillis > 0L) {
            "Actualizado ${DateFormat.getTimeFormat(this).format(status.lastUpdatedMillis)}"
        } else {
            "Esperando primera actualización"
        }
        val summary = if (status.connected) {
            val orders = if (status.ordersAvailable) status.pendingOrders.toString() else "--"
            val quotes = if (status.quotesAvailable) status.pendingQuotes.toString() else "--"
            "Pedidos pendientes: $orders · Reservas por confirmar: $quotes"
        } else "Sin conexión. Se volverá a comprobar automáticamente."
        val builder = Notification.Builder(this, CHANNEL_ID)
            .setSmallIcon(R.drawable.ic_sucree)
            .setContentTitle("SucréePOS · monitoreo activo")
            .setContentText(summary)
            .setSubText(lastUpdate)
            .setStyle(Notification.BigTextStyle().bigText("$summary\n$lastUpdate\nActualización automática cada 5 segundos."))
            .setCategory(Notification.CATEGORY_SERVICE)
            .setVisibility(Notification.VISIBILITY_PRIVATE)
            .setOngoing(true)
            .setOnlyAlertOnce(true)
            .setContentIntent(openApp)

        builder.addAction(Notification.Action.Builder(
            null,
            "Pedidos (${status.pendingOrders})",
            activityPendingIntent(showOrders = true),
        ).build())
        builder.addAction(Notification.Action.Builder(
            null,
            "Por confirmar (${status.pendingQuotes})",
            activityPendingIntent(showAgenda = true),
        ).build())
        return builder.build().apply {
            flags = flags or Notification.FLAG_NO_CLEAR or Notification.FLAG_ONGOING_EVENT
        }
    }

    private fun activityPendingIntent(showOrders: Boolean = false, showAgenda: Boolean = false): PendingIntent {
        val launch = Intent(this, MainActivity::class.java).apply {
            flags = Intent.FLAG_ACTIVITY_CLEAR_TOP or Intent.FLAG_ACTIVITY_SINGLE_TOP
            putExtra("show_orders", showOrders)
            putExtra("show_agenda_quotes", showAgenda)
        }
        val requestCode = when {
            showOrders -> REQUEST_OPEN_ORDERS
            showAgenda -> REQUEST_OPEN_AGENDA
            else -> REQUEST_OPEN_APP
        }
        return PendingIntent.getActivity(
            this,
            requestCode,
            launch,
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE,
        )
    }

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onDestroy() {
        pollTask?.cancel(true)
        scheduler.shutdownNow()
        mainHandler.removeCallbacksAndMessages(null)
        super.onDestroy()
    }

    companion object {
        private const val CHANNEL_ID = "pos_monitoring"
        private const val NOTIFICATION_ID = 1407
        private const val POLL_INTERVAL_SECONDS = 5L
        private const val REQUEST_OPEN_APP = 1410
        private const val REQUEST_OPEN_ORDERS = 1411
        private const val REQUEST_OPEN_AGENDA = 1412
    }
}
