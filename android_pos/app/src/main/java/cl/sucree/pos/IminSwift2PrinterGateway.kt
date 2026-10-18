package cl.sucree.pos

import android.content.Context
import com.imin.printer.InitPrinterCallback
import com.imin.printer.INeoPrinterCallback
import com.imin.printer.PrinterHelper

class IminSwift2PrinterGateway(context: Context) {
    private val appContext = context.applicationContext
    private val helper = PrinterHelper.getInstance()

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
