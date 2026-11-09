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
            android.app.DownloadManager.ACTION_DOWNLOAD_COMPLETE -> {
                val downloadId = intent.getLongExtra(android.app.DownloadManager.EXTRA_DOWNLOAD_ID, -1L)
                PosUpdateManager.onDownloadComplete(context, downloadId)
            }
        }
    }

    companion object {
        const val ACTION_DAILY_CHECK = "cl.sucree.pos.action.DAILY_UPDATE_CHECK"
        const val ACTION_DOWNLOAD_UPDATE = "cl.sucree.pos.action.DOWNLOAD_UPDATE"
        const val ACTION_INSTALL_UPDATE = "cl.sucree.pos.action.INSTALL_UPDATE"
        const val EXTRA_DOWNLOAD_ID = "update_download_id"
        const val INSTALL_PERMISSION_REQUEST = 5104
    }
}
