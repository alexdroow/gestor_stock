package cl.sucree.pos

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent

class PosUpdateReceiver : BroadcastReceiver() {
    override fun onReceive(context: Context, intent: Intent?) {
        when (intent?.action) {
            Intent.ACTION_BOOT_COMPLETED -> PosUpdateManager.scheduleDaily(context)
            ACTION_DAILY_CHECK -> {
                PosUpdateManager.scheduleDaily(context)
                val pending = goAsync()
                PosUpdateManager.check(context, announce = true) { pending.finish() }
            }
            ACTION_DOWNLOAD_UPDATE -> PosUpdateManager.downloadAvailableUpdate(context)
        }
    }

    companion object {
        const val ACTION_DAILY_CHECK = "cl.sucree.pos.action.DAILY_UPDATE_CHECK"
        const val ACTION_DOWNLOAD_UPDATE = "cl.sucree.pos.action.DOWNLOAD_UPDATE"
    }
}
