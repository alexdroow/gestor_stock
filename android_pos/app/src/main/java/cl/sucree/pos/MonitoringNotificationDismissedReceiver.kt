package cl.sucree.pos

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.os.Build

class MonitoringNotificationDismissedReceiver : BroadcastReceiver() {
    override fun onReceive(context: Context, intent: Intent?) {
        if (intent?.action != ACTION_RESTORE) return
        val enabled = context.getSharedPreferences("pos_order_alerts", Context.MODE_PRIVATE)
            .getBoolean("monitor_enabled", false)
        if (!enabled) return

        val service = Intent(context, PosMonitoringService::class.java)
        runCatching {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) context.startForegroundService(service)
            else context.startService(service)
        }
    }

    companion object {
        const val ACTION_RESTORE = "cl.sucree.pos.action.RESTORE_MONITOR_NOTIFICATION"
    }
}
