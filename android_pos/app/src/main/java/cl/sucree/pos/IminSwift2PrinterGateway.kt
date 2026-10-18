package cl.sucree.pos

import android.content.Context
import android.graphics.Bitmap
import android.graphics.Canvas
import android.graphics.Color
import android.view.View
import android.view.ViewGroup
import android.widget.FrameLayout
import android.webkit.WebView
import android.webkit.WebViewClient
import com.imin.printer.InitPrinterCallback
import com.imin.printer.INeoPrinterCallback
import com.imin.printer.PrinterHelper
import org.json.JSONObject
import org.json.JSONTokener
import kotlin.math.ceil
import kotlin.math.roundToInt

class IminSwift2PrinterGateway(context: Context) {
    private val webViewContext = context
    private val appContext = context.applicationContext
    private val helper = PrinterHelper.getInstance()

    fun printHtmlTicket(orderId: String, html: String, onStatus: (String) -> Unit) {
        val density = appContext.resources.displayMetrics.density
        val cssWidth = 58.0 * 96.0 / 25.4
        val renderWidth = (cssWidth * density).roundToInt().coerceAtLeast(1)
        val parent = (webViewContext as? android.app.Activity)
            ?.findViewById<ViewGroup>(android.R.id.content)
            ?.getChildAt(0) as? ViewGroup
        if (parent == null) {
            onStatus("No se pudo preparar el renderizador del ticket en este dispositivo.")
            return
        }
        val anchor = parent.findViewWithTag<View>("sucree_pos_main_webview")
        val insertAt = if (anchor != null) parent.indexOfChild(anchor).coerceAtLeast(0) else 0
        val webView = WebView(webViewContext).apply {
            setBackgroundColor(Color.WHITE)
            isVerticalScrollBarEnabled = false
            isHorizontalScrollBarEnabled = false
            overScrollMode = View.OVER_SCROLL_NEVER
            isFocusable = false
            isFocusableInTouchMode = false
            setLayerType(View.LAYER_TYPE_SOFTWARE, null)
            settings.apply {
                javaScriptEnabled = true
                domStorageEnabled = false
                allowFileAccess = false
                allowContentAccess = false
                blockNetworkLoads = true
                useWideViewPort = false
                loadWithOverviewMode = false
                defaultTextEncodingName = "UTF-8"
                textZoom = 100
            }
            layoutParams = FrameLayout.LayoutParams(renderWidth, parent.height.coerceAtLeast(1))
        }
        parent.addView(webView, insertAt, webView.layoutParams)

        fun cleanup() {
            (webView.parent as? ViewGroup)?.removeView(webView)
            webView.stopLoading()
            webView.destroy()
        }

        webView.webViewClient = object : WebViewClient() {
            override fun onPageFinished(view: WebView?, url: String?) {
                webView.postDelayed({
                    webView.evaluateJavascript(
                        "JSON.stringify({width:document.body.getBoundingClientRect().width,height:Math.max(document.body.scrollHeight,document.documentElement.scrollHeight)})",
                    ) { result ->
                    try {
                        val dimensionsJson = JSONTokener(result ?: "null").nextValue() as? String
                            ?: throw IllegalStateException("No se pudo medir el ticket")
                        val dimensions = JSONObject(dimensionsJson)
                        val cssPageWidth = dimensions.optDouble("width", 0.0)
                        val cssPageHeight = dimensions.optDouble("height", 0.0)
                        if (cssPageWidth < 100 || cssPageHeight < 100) {
                            throw IllegalStateException("El contenido del ticket no quedó listo para imprimir")
                        }
                        val pageWidth = (cssPageWidth * density).roundToInt()
                        val pageHeight = (cssPageHeight * density).roundToInt()
                        val outputWidth = 384
                        val outputHeight = ceil(pageHeight * outputWidth.toDouble() / pageWidth).toInt().coerceAtLeast(1)
                        if (outputHeight > 24_000) throw IllegalArgumentException("El ticket es demasiado largo para imprimir de una vez")

                        webView.layoutParams = FrameLayout.LayoutParams(pageWidth, pageHeight)
                        webView.post {
                            webView.measure(
                                View.MeasureSpec.makeMeasureSpec(pageWidth, View.MeasureSpec.EXACTLY),
                                View.MeasureSpec.makeMeasureSpec(pageHeight, View.MeasureSpec.EXACTLY),
                            )
                            webView.layout(0, 0, pageWidth, pageHeight)
                            webView.postDelayed({
                                try {
                                    val scale = outputWidth.toFloat() / pageWidth
                                    val chunks = mutableListOf<Bitmap>()
                                    var top = 0
                                    while (top < outputHeight) {
                                        val chunkHeight = minOf(1024, outputHeight - top)
                                        val bitmap = Bitmap.createBitmap(outputWidth, chunkHeight, Bitmap.Config.ARGB_8888)
                                        val canvas = Canvas(bitmap)
                                        canvas.drawColor(Color.WHITE)
                                        canvas.scale(scale, scale)
                                        canvas.translate(0f, -top / scale)
                                        webView.draw(canvas)
                                        chunks.add(bitmap)
                                        top += chunkHeight
                                    }
                                    val hasInk = chunks.any { bitmap ->
                                        var y = 0
                                        var found = false
                                        while (y < bitmap.height && !found) {
                                            var x = 0
                                            while (x < bitmap.width && !found) {
                                                val color = bitmap.getPixel(x, y)
                                                found = Color.red(color) < 235 || Color.green(color) < 235 || Color.blue(color) < 235
                                                x += 8
                                            }
                                            y += 8
                                        }
                                        found
                                    }
                                    cleanup()
                                    if (!hasInk) {
                                        chunks.forEach { it.recycle() }
                                        onStatus("El ticket se generó vacío; no se envió a la impresora. Intenta nuevamente.")
                                    } else {
                                        sendAgendaBitmap(orderId, chunks, onStatus)
                                    }
                                } catch (error: Exception) {
                                    cleanup()
                                    onStatus("No se pudo dibujar el ticket 58 mm: ${error.localizedMessage ?: "error de renderizado"}")
                                }
                            }, 120)
                        }
                    } catch (error: Exception) {
                        cleanup()
                        onStatus("No se pudo preparar el ticket 58 mm: ${error.localizedMessage ?: "error de formato"}")
                    }
                    }
                }, 120)
            }
        }
        webView.loadDataWithBaseURL("https://pasteleriasucree.cl/", html, "text/html", "UTF-8", null)
    }

    private fun sendAgendaBitmap(orderId: String, bitmaps: List<Bitmap>, onStatus: (String) -> Unit) {
        var finished = false
        fun releaseBitmaps() {
            if (finished) return
            finished = true
            bitmaps.forEach { if (!it.isRecycled) it.recycle() }
        }
        fun send() {
            try {
                helper.setConnectInternalPrinter(true)
                helper.setPageFormat(1)
                helper.printMultiBitmapWithAlign(bitmaps, 1, object : INeoPrinterCallback() {
                    override fun onRunResult(isSuccess: Boolean) {
                        onStatus(if (isSuccess) "Ticket 58 mm del pedido #$orderId enviado a la impresora integrada." else "La impresora integrada no pudo completar el ticket.")
                        releaseBitmaps()
                    }
                    override fun onReturnString(result: String?) = Unit
                    override fun onRaiseException(code: Int, message: String?) {
                        onStatus("Error de impresora iMin ($code): ${message.orEmpty()}")
                        releaseBitmaps()
                    }
                    override fun onPrintResult(code: Int, message: String?) {
                        if (code !in setOf(0, 1, 48)) {
                            onStatus("Estado de impresora iMin ($code): ${message.orEmpty()}")
                        }
                    }
                })
            } catch (error: Exception) {
                releaseBitmaps()
                onStatus("No se pudo enviar el ticket a la impresora iMin: ${error.localizedMessage ?: "error del servicio"}")
            }
        }

        try {
            if (PrinterHelper.getNeoPrinterService() != null) send()
            else {
                val bound = helper.initPrinterService(appContext, object : InitPrinterCallback {
                    override fun onConnected() = send()
                    override fun onDisconnected() {
                        releaseBitmaps()
                        onStatus("Se perdió la conexión con la impresora integrada iMin.")
                    }
                })
                if (!bound) {
                    releaseBitmaps()
                    onStatus("No se pudo conectar con el servicio de impresora iMin.")
                }
            }
        } catch (error: Exception) {
            releaseBitmaps()
            onStatus("Impresora iMin no disponible: ${error.localizedMessage ?: "verifica el dispositivo"}")
        }
    }

    fun print(ticket: ConfirmedSaleTicket, copyLabel: String, onStatus: (String) -> Unit) {
        val body = buildString {
            appendLine("SUCRÉE PASTELERÍA")
            appendLine(if (ticket.title.contains("manual", ignoreCase = true)) "COMPROBANTE DE VENTA" else "COMANDA DE PREPARACIÓN")
            appendLine(ticket.title)
            appendLine(copyLabel)
            appendLine(java.text.SimpleDateFormat("dd/MM/yyyy HH:mm", java.util.Locale("es", "CL")).format(java.util.Date()))
            appendLine("--------------------------------")
            appendLine(ticket.summary)
            appendLine("--------------------------------")
            appendLine("Venta registrada en Sucrée POS")
            appendLine("\n\n")
        }

        fun sendToPrinter() {
            try {
                helper.setConnectInternalPrinter(true)
                helper.setPageFormat(1) // 58 mm
                helper.printText(body, object : INeoPrinterCallback() {
                    override fun onRunResult(isSuccess: Boolean) {
                        onStatus(if (isSuccess) "Ticket de 58 mm enviado a la impresora iMin." else "La impresora iMin no pudo completar el ticket.")
                    }

                    override fun onReturnString(result: String?) = Unit

                    override fun onRaiseException(code: Int, message: String?) {
                        onStatus("Error de impresora iMin ($code): ${message.orEmpty()}")
                    }

                    override fun onPrintResult(code: Int, message: String?) {
                        if (code !in setOf(0, 1, 48)) {
                            onStatus("Estado de impresora iMin ($code): ${message.orEmpty()}")
                        }
                    }
                })
            } catch (error: Exception) {
                onStatus("No se pudo imprimir en iMin: ${error.localizedMessage ?: "error del servicio"}")
            }
        }

        try {
            if (PrinterHelper.getNeoPrinterService() != null) {
                sendToPrinter()
            } else {
                val bound = helper.initPrinterService(appContext, object : InitPrinterCallback {
                    override fun onConnected() = sendToPrinter()
                    override fun onDisconnected() {
                        onStatus("Se perdió la conexión con la impresora integrada iMin.")
                    }
                })
                if (!bound) onStatus("No se pudo conectar con el servicio de impresora iMin.")
            }
        } catch (error: Exception) {
            onStatus("Impresora iMin no disponible: ${error.localizedMessage ?: "verifica el dispositivo"}")
        }
    }
}
