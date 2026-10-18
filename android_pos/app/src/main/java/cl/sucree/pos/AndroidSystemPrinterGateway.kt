package cl.sucree.pos

import android.content.Context
import android.print.PrintAttributes
import android.print.PrintManager
import android.os.Build
import android.webkit.WebView
import android.webkit.WebViewClient

data class ConfirmedSaleTicket(
    val orderId: String,
    val title: String,
    val summary: String,
)

class AndroidSystemPrinterGateway(private val context: Context) {
    fun printAgendaTicket(orderId: String, html: String, onStatus: (String) -> Unit) {
        if (isIminSwift2()) {
            IminSwift2PrinterGateway(context).printHtmlTicket(orderId, html, onStatus)
            return
        }

        val printView = WebView(context)
        printView.settings.apply {
            javaScriptEnabled = false
            allowFileAccess = false
            allowContentAccess = false
        }
        printView.webViewClient = object : WebViewClient() {
            override fun onPageFinished(view: WebView?, url: String?) {
                printView.webViewClient = WebViewClient()
                try {
                    val attributes = PrintAttributes.Builder()
                        .setMediaSize(PrintAttributes.MediaSize("SU CREE_58MM", "58 mm", 2283, 11000))
                        .setResolution(PrintAttributes.Resolution("SU CREE_203DPI", "203 dpi", 203, 203))
                        .setMinMargins(PrintAttributes.Margins(0, 0, 0, 0))
                        .setColorMode(PrintAttributes.COLOR_MODE_MONOCHROME)
                        .build()
                    val job = (context.getSystemService(Context.PRINT_SERVICE) as PrintManager).print(
                        "Sucree_Agenda_$orderId",
                        printView.createPrintDocumentAdapter("Sucrée Agenda"),
                        attributes,
                    )
                    onStatus("Ticket de Agenda enviado a Android Print.")
                    val handler = android.os.Handler(android.os.Looper.getMainLooper())
                    val startedAt = System.currentTimeMillis()
                    val poll = object : Runnable {
                        override fun run() {
                            when {
                                job.isCompleted -> {
                                    onStatus("Impresión del pedido #$orderId completada.")
                                    printView.destroy()
                                }
                                job.isFailed || job.isCancelled -> {
                                    onStatus("No se completó la impresión del pedido #$orderId.")
                                    printView.destroy()
                                }
                                System.currentTimeMillis() - startedAt >= 120_000 -> printView.destroy()
                                else -> handler.postDelayed(this, 1000)
                            }
                        }
                    }
                    handler.post(poll)
                } catch (error: Exception) {
                    printView.destroy()
                    onStatus("No se pudo iniciar la impresión: ${error.localizedMessage ?: "error del sistema"}")
                }
            }
        }
        printView.loadDataWithBaseURL("https://pasteleriasucree.cl/", html, "text/html", "UTF-8", null)
    }

    fun print(ticket: ConfirmedSaleTicket, onStatus: (String) -> Unit) {
        val preferences = context.getSharedPreferences("printer_jobs", Context.MODE_PRIVATE)
        val countKey = "order_${ticket.orderId}_attempts"
        val attempt = preferences.getInt(countKey, 0) + 1
        val copyLabel = if (attempt == 1) "ORIGINAL" else "COPIA / REIMPRESIÓN #${attempt - 1}"
        if (isIminSwift2()) {
            IminSwift2PrinterGateway(context).print(ticket, copyLabel, onStatus)
            preferences.edit().putInt(countKey, attempt).apply()
            return
        }
        val jobKey = "order_${ticket.orderId}_attempt_$attempt"
        val printView = WebView(context)

        printView.settings.apply {
            javaScriptEnabled = false
            allowFileAccess = false
            allowContentAccess = false
        }
        printView.webViewClient = object : WebViewClient() {
            override fun onPageFinished(view: WebView?, url: String?) {
                printView.webViewClient = WebViewClient()
                try {
                    val manager = context.getSystemService(Context.PRINT_SERVICE) as PrintManager
                    val attributes = PrintAttributes.Builder()
                        .setMediaSize(PrintAttributes.MediaSize("SU CREE_58MM", "58 mm", 2283, 11000))
                        .setResolution(PrintAttributes.Resolution("SU CREE_203DPI", "203 dpi", 203, 203))
                        .setMinMargins(PrintAttributes.Margins(80, 80, 80, 80))
                        .setColorMode(PrintAttributes.COLOR_MODE_MONOCHROME)
                        .build()
                    val job = manager.print(
                        "Sucree_Comanda_${ticket.orderId}_$attempt",
                        printView.createPrintDocumentAdapter("Sucrée POS"),
                        attributes,
                    )
                    preferences.edit()
                        .putInt(countKey, attempt)
                        .putString(jobKey, "QUEUED")
                        .apply()
                    onStatus("Comanda enviada a Android Print como $copyLabel.")
                    observeJob(job, printView, ticket.orderId, preferences, jobKey, onStatus)
                } catch (error: Exception) {
                    printView.destroy()
                    onStatus("No se pudo iniciar la impresión: ${error.localizedMessage ?: "error del sistema"}")
                }
            }
        }
        printView.loadDataWithBaseURL(
            "https://pasteleriasucree.cl/",
            ticketHtml(ticket, copyLabel),
            "text/html",
            "UTF-8",
            null,
        )
    }

    private fun isIminSwift2(): Boolean {
        val device = listOf(Build.MANUFACTURER, Build.BRAND, Build.MODEL, Build.DEVICE)
            .joinToString(" ") { it.orEmpty() }
            .lowercase()
        return "imin" in device && "swift" in device
    }

    private fun observeJob(
        job: android.print.PrintJob,
        printView: WebView,
        orderId: String,
        preferences: android.content.SharedPreferences,
        jobKey: String,
        onStatus: (String) -> Unit,
    ) {
        val handler = android.os.Handler(android.os.Looper.getMainLooper())
        val startedAt = System.currentTimeMillis()
        var previousState = ""
        val poll = object : Runnable {
            override fun run() {
                val state = when {
                    job.isCompleted -> "COMPLETED"
                    job.isFailed -> "FAILED"
                    job.isCancelled -> "CANCELLED"
                    job.isBlocked -> "BLOCKED"
                    else -> "QUEUED"
                }
                preferences.edit().putString(jobKey, state).apply()
                if (state != previousState) when (state) {
                    "COMPLETED" -> {
                        onStatus("Impresión completada para el pedido #$orderId.")
                        printView.destroy()
                    }
                    "FAILED" -> {
                        onStatus("Falló la impresión. La venta sigue registrada; puedes reintentar y saldrá marcada como copia.")
                        printView.destroy()
                    }
                    "CANCELLED" -> {
                        onStatus("Impresión cancelada. La venta sigue registrada.")
                        printView.destroy()
                    }
                    "BLOCKED" -> onStatus("Impresora bloqueada (por ejemplo, sin papel). Revisa el servicio de impresión y reintenta.")
                    else -> if (previousState == "BLOCKED") onStatus("El servicio de impresión volvió a procesar el trabajo.")
                }
                previousState = state
                if (state in setOf("COMPLETED", "FAILED", "CANCELLED")) return
                if (System.currentTimeMillis() - startedAt < 120_000) {
                    handler.postDelayed(this, 1_000)
                } else {
                    printView.destroy()
                }
            }
        }
        handler.post(poll)
    }

    private fun ticketHtml(ticket: ConfirmedSaleTicket, copyLabel: String): String {
        val escapedTitle = escapeHtml(ticket.title)
        val escapedSummary = escapeHtml(ticket.summary)
        val escapedCopy = escapeHtml(copyLabel)
        return """
            <!doctype html><html><head><meta charset="utf-8">
            <meta name="viewport" content="width=device-width,initial-scale=1">
            <style>
              @page { size: 58mm 279mm; margin: 2mm; }
              * { box-sizing: border-box; }
              body { width: 54mm; margin: 0 auto; color: #000; font: 9pt/1.35 monospace; overflow-wrap: anywhere; }
              header { text-align: center; border-bottom: 1px dashed #000; padding-bottom: 3mm; margin-bottom: 3mm; }
              h1 { font: bold 14pt Georgia, serif; margin: 0 0 1mm; }
              h2 { font: bold 9pt monospace; margin: 2mm 0 0; }
              .copy { font-weight: bold; margin-top: 2mm; }
              .summary { white-space: pre-wrap; }
              footer { border-top: 1px dashed #000; margin-top: 3mm; padding-top: 2mm; text-align: center; font-size: 8pt; }
            </style></head><body>
            <header><h1>SUCRÉE PASTELERÍA</h1><div>${if (ticket.title.contains("manual", ignoreCase = true)) "COMPROBANTE DE VENTA" else "COMANDA DE PREPARACIÓN"}</div>
            <h2>$escapedTitle</h2><div class="copy">$escapedCopy</div>
            <div>${java.text.SimpleDateFormat("dd/MM/yyyy HH:mm", java.util.Locale("es", "CL")).format(java.util.Date())}</div></header>
            <main class="summary">$escapedSummary</main>
            <footer>Venta confirmada en el sistema Sucrée.<br>No volver a registrar para reimprimir.</footer>
            </body></html>
        """.trimIndent()
    }

    private fun escapeHtml(value: String): String = value
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("\"", "&quot;")
        .replace("'", "&#39;")
}
