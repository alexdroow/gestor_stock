package cl.sucree.pos

import android.annotation.SuppressLint
import android.app.Activity
import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.content.ActivityNotFoundException
import android.content.Intent
import android.content.Context
import android.content.res.ColorStateList
import android.graphics.Color
import android.net.Uri
import android.media.AudioAttributes
import android.media.RingtoneManager
import android.os.Build
import android.os.Bundle
import android.os.Handler
import android.os.Looper
import android.Manifest
import android.content.pm.PackageManager
import android.view.Gravity
import android.view.View
import android.view.ViewGroup
import android.webkit.CookieManager
import android.webkit.SafeBrowsingResponse
import android.webkit.SslErrorHandler
import android.webkit.WebChromeClient
import android.webkit.WebResourceError
import android.webkit.WebResourceRequest
import android.webkit.WebSettings
import android.webkit.WebView
import android.webkit.WebViewClient
import android.webkit.JavascriptInterface
import android.widget.Button
import android.widget.FrameLayout
import android.widget.LinearLayout
import android.widget.ProgressBar
import android.widget.TextView
import android.widget.Toast
import org.json.JSONObject
import org.json.JSONTokener
import android.net.http.SslError

class MainActivity : Activity() {
    private lateinit var webView: WebView
    private lateinit var loading: ProgressBar
    private lateinit var connectionPanel: LinearLayout
    private lateinit var printButton: Button
    private val uiHandler = Handler(Looper.getMainLooper())
    private val printer by lazy { AndroidSystemPrinterGateway(this) }
    private val saleScreenPoll = object : Runnable {
        override fun run() {
            if (!::webView.isInitialized || isFinishing) return
            webView.evaluateJavascript(
                "(() => { const s=document.getElementById('success-screen'); return !!s && !s.hidden && !!document.getElementById('success-title')?.textContent?.match(/#\\s*\\d+/); })()",
            ) { result ->
                printButton.visibility = if (result == "true") View.VISIBLE else View.GONE
                uiHandler.postDelayed(this, 900)
            }
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        window.statusBarColor = Color.rgb(255, 248, 239)
        window.navigationBarColor = Color.rgb(255, 248, 239)
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            window.decorView.systemUiVisibility =
                View.SYSTEM_UI_FLAG_LIGHT_STATUS_BAR or View.SYSTEM_UI_FLAG_LIGHT_NAVIGATION_BAR
        } else {
            window.decorView.systemUiVisibility = View.SYSTEM_UI_FLAG_LIGHT_STATUS_BAR
        }

        buildScreen()
        configureWebView()
        createOrderNotificationChannel()
        requestNotificationPermissionIfNeeded()
        if (savedInstanceState == null) {
            webView.loadUrl(posStartUrl(intent))
        } else {
            webView.restoreState(savedInstanceState)
        }
    }

    private fun buildScreen() {
        val root = FrameLayout(this).apply {
            setBackgroundColor(Color.rgb(251, 245, 237))
        }
        webView = WebView(this).apply {
            setBackgroundColor(Color.rgb(251, 245, 237))
        }
        loading = ProgressBar(this, null, android.R.attr.progressBarStyleHorizontal).apply {
            isIndeterminate = true
            visibility = View.GONE
            progressDrawable?.setTint(Color.rgb(233, 105, 11))
        }
        connectionPanel = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            gravity = Gravity.CENTER
            setPadding(dp(28), dp(24), dp(28), dp(24))
            setBackgroundColor(Color.rgb(251, 245, 237))
            visibility = View.GONE
            addView(TextView(this@MainActivity).apply {
                text = "No fue posible conectar con Sucrée"
                textSize = 20f
                setTextColor(Color.rgb(56, 32, 22))
                gravity = Gravity.CENTER
            }, LinearLayout.LayoutParams(-1, -2))
            addView(TextView(this@MainActivity).apply {
                text = "Revisa la conexión a Internet e inténtalo nuevamente."
                textSize = 14f
                setTextColor(Color.rgb(128, 111, 99))
                gravity = Gravity.CENTER
                setPadding(0, dp(10), 0, dp(18))
            }, LinearLayout.LayoutParams(-1, -2))
            addView(Button(this@MainActivity).apply {
                text = "Reintentar"
                setOnClickListener { webView.reload() }
            })
        }
        printButton = Button(this).apply {
            text = "Imprimir comanda"
            setTextColor(Color.WHITE)
            backgroundTintList = ColorStateList.valueOf(Color.rgb(233, 105, 11))
            visibility = View.GONE
            elevation = dp(8).toFloat()
            setOnClickListener { printConfirmedOrder() }
        }

        root.addView(webView, FrameLayout.LayoutParams(-1, -1))
        root.addView(connectionPanel, FrameLayout.LayoutParams(-1, -1))
        root.addView(loading, FrameLayout.LayoutParams(-1, dp(3), Gravity.TOP))
        root.addView(printButton, FrameLayout.LayoutParams(-2, dp(48), Gravity.TOP or Gravity.END).apply {
            setMargins(0, dp(8), dp(10), 0)
        })
        setContentView(root)
    }

    @SuppressLint("SetJavaScriptEnabled")
    private fun configureWebView() {
        CookieManager.getInstance().apply {
            setAcceptCookie(true)
            setAcceptThirdPartyCookies(webView, false)
        }

        webView.settings.apply {
            javaScriptEnabled = true
            domStorageEnabled = true
            cacheMode = WebSettings.LOAD_DEFAULT
            mixedContentMode = WebSettings.MIXED_CONTENT_NEVER_ALLOW
            allowFileAccess = false
            allowContentAccess = false
            javaScriptCanOpenWindowsAutomatically = false
            setSupportMultipleWindows(false)
            builtInZoomControls = false
            displayZoomControls = false
            mediaPlaybackRequiresUserGesture = true
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) safeBrowsingEnabled = true
        }
        webView.addJavascriptInterface(PosBridge(), "SucreePosAndroid")
        WebView.setWebContentsDebuggingEnabled(false)
        webView.webChromeClient = object : WebChromeClient() {
            override fun onProgressChanged(view: WebView?, progress: Int) {
                loading.visibility = if (progress in 1..99) View.VISIBLE else View.GONE
            }
        }
        webView.webViewClient = object : WebViewClient() {
            override fun shouldOverrideUrlLoading(
                view: WebView?,
                request: WebResourceRequest,
            ): Boolean = routeUrl(request.url)

            @Suppress("DEPRECATION")
            override fun shouldOverrideUrlLoading(view: WebView?, url: String?): Boolean {
                val uri = url?.let { Uri.parse(it) } ?: return true
                return routeUrl(uri)
            }

            override fun onPageStarted(view: WebView?, url: String?, favicon: android.graphics.Bitmap?) {
                connectionPanel.visibility = View.GONE
                loading.visibility = View.VISIBLE
            }

            override fun onPageFinished(view: WebView?, url: String?) {
                loading.visibility = View.GONE
                connectionPanel.visibility = View.GONE
                CookieManager.getInstance().flush()
                uiHandler.removeCallbacks(saleScreenPoll)
                uiHandler.post(saleScreenPoll)
            }

            override fun onReceivedError(
                view: WebView?,
                request: WebResourceRequest?,
                error: WebResourceError?,
            ) {
                if (request?.isForMainFrame == true) showConnectionError()
            }

            override fun onReceivedSslError(view: WebView?, handler: SslErrorHandler?, error: SslError?) {
                handler?.cancel()
                showConnectionError()
            }

            override fun onSafeBrowsingHit(
                view: WebView?,
                request: WebResourceRequest?,
                threatType: Int,
                callback: SafeBrowsingResponse?,
            ) {
                callback?.backToSafety(true)
            }
        }
    }

    private fun routeUrl(uri: Uri): Boolean {
        if (isTrustedPosUrl(uri)) return false
        if (uri.scheme.equals("intent", ignoreCase = true)) return launchIntentUrl(uri.toString())
        if (uri.scheme.equals("tel", ignoreCase = true) ||
            uri.scheme.equals("mailto", ignoreCase = true) ||
            isWhatsAppHost(uri.host)
        ) {
            return openExternal(uri)
        }
        return true
    }

    private fun isTrustedPosUrl(uri: Uri): Boolean {
        val validHost = uri.host?.lowercase() in setOf("pasteleriasucree.cl", "www.pasteleriasucree.cl")
        return uri.scheme.equals("https", ignoreCase = true) && validHost && (uri.port == -1 || uri.port == 443)
    }

    private fun isWhatsAppHost(host: String?): Boolean =
        host?.lowercase() in setOf("wa.me", "api.whatsapp.com", "web.whatsapp.com", "chat.whatsapp.com")

    private fun launchIntentUrl(value: String): Boolean {
        val intent = try {
            Intent.parseUri(value, Intent.URI_INTENT_SCHEME)
        } catch (_: Exception) {
            return true
        }
        if (intent.`package` !in setOf("com.whatsapp", "com.whatsapp.w4b")) return true
        return try {
            intent.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
            startActivity(intent)
            true
        } catch (_: ActivityNotFoundException) {
            val fallback = intent.getStringExtra("browser_fallback_url")?.let { Uri.parse(it) }
            if (fallback != null && isWhatsAppHost(fallback.host)) {
                openExternal(fallback)
            } else {
                Toast.makeText(this, "Instala WhatsApp para continuar.", Toast.LENGTH_LONG).show()
            }
            true
        } catch (_: Exception) {
            true
        }
    }

    private fun openExternal(uri: Uri): Boolean {
        return try {
            startActivity(Intent(Intent.ACTION_VIEW, uri))
            true
        } catch (_: ActivityNotFoundException) {
            Toast.makeText(this, "No hay una aplicación disponible para abrir este enlace.", Toast.LENGTH_LONG).show()
            true
        }
    }

    private fun showConnectionError() {
        loading.visibility = View.GONE
        connectionPanel.visibility = View.VISIBLE
    }

    private fun printConfirmedOrder() {
        val extractTicket = """(() => {
            const screen = document.getElementById('success-screen');
            const title = document.getElementById('success-title')?.textContent?.trim() || '';
            const summary = document.getElementById('success-ticket-summary')?.textContent?.trim()
                || document.getElementById('success-summary')?.innerText?.trim() || '';
            const id = title.match(/#\s*(\d+)/)?.[1];
            if (!screen || screen.hidden || !id || !summary) return null;
            return JSON.stringify({ orderId: id, title, summary });
        })()""".trimIndent()
        webView.evaluateJavascript(extractTicket) { encodedResult ->
            val ticketJson = try {
                JSONTokener(encodedResult ?: "null").nextValue() as? String
            } catch (_: Exception) {
                null
            }
            if (ticketJson == null) {
                Toast.makeText(this, "Primero confirma la venta en el sistema.", Toast.LENGTH_LONG).show()
                return@evaluateJavascript
            }
            try {
                val json = JSONObject(ticketJson)
                printer.print(
                    ConfirmedSaleTicket(
                        orderId = json.getString("orderId"),
                        title = json.getString("title"),
                        summary = json.getString("summary"),
                    ),
                ) { status ->
                    Toast.makeText(this, status, Toast.LENGTH_LONG).show()
                }
            } catch (_: Exception) {
                Toast.makeText(this, "No se pudo preparar la comanda de esta venta.", Toast.LENGTH_LONG).show()
            }
        }
    }

    private fun posStartUrl(source: Intent): String =
        if (source.getBooleanExtra("show_orders", false)) "${BuildConfig.POS_URL}?tab=orders" else BuildConfig.POS_URL

    private fun createOrderNotificationChannel() {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.O) return
        val sound = RingtoneManager.getDefaultUri(RingtoneManager.TYPE_NOTIFICATION)
        val audio = AudioAttributes.Builder()
            .setUsage(AudioAttributes.USAGE_NOTIFICATION)
            .setContentType(AudioAttributes.CONTENT_TYPE_SONIFICATION)
            .build()
        val channel = NotificationChannel(
            ORDER_CHANNEL_ID,
            "Nuevos pedidos Sucrée",
            NotificationManager.IMPORTANCE_HIGH,
        ).apply {
            description = "Avisos cuando ingresa un pedido al POS."
            enableVibration(true)
            setSound(sound, audio)
        }
        getSystemService(NotificationManager::class.java).createNotificationChannel(channel)
    }

    private fun requestNotificationPermissionIfNeeded() {
        if (Build.VERSION.SDK_INT < 33 ||
            checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS) == PackageManager.PERMISSION_GRANTED
        ) return
        val prefs = getSharedPreferences("pos_notifications", Context.MODE_PRIVATE)
        if (!prefs.getBoolean("permission_asked", false)) {
            prefs.edit().putBoolean("permission_asked", true).apply()
            requestPermissions(arrayOf(Manifest.permission.POST_NOTIFICATIONS), NOTIFICATION_PERMISSION_REQUEST)
        }
    }

    private fun showNewOrderNotification(orderId: String, customerName: String) {
        if (Build.VERSION.SDK_INT >= 33 &&
            checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED
        ) return
        val manager = getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
        val openOrders = Intent(this, MainActivity::class.java).apply {
            flags = Intent.FLAG_ACTIVITY_CLEAR_TOP or Intent.FLAG_ACTIVITY_SINGLE_TOP
            putExtra("show_orders", true)
        }
        val pendingIntent = PendingIntent.getActivity(
            this,
            orderId.hashCode(),
            openOrders,
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE,
        )
        val notification = Notification.Builder(this, ORDER_CHANNEL_ID)
            .setSmallIcon(R.drawable.ic_sucree)
            .setContentTitle("Nuevo pedido #$orderId")
            .setContentText(customerName.ifBlank { "Pedido nuevo en el POS" })
            .setCategory(Notification.CATEGORY_EVENT)
            .setAutoCancel(true)
            .setContentIntent(pendingIntent)
            .build()
        manager.notify(orderId.hashCode(), notification)
    }

    private inner class PosBridge {
        @JavascriptInterface
        fun printOrder(payload: String) {
            runOnUiThread {
                try {
                    val json = JSONObject(payload)
                    printer.print(
                        ConfirmedSaleTicket(
                            orderId = json.getString("orderId"),
                            title = json.optString("title", "Pedido"),
                            summary = json.optString("summary", ""),
                        ),
                    ) { status -> Toast.makeText(this@MainActivity, status, Toast.LENGTH_LONG).show() }
                } catch (_: Exception) {
                    Toast.makeText(this@MainActivity, "No se pudo preparar el ticket.", Toast.LENGTH_LONG).show()
                }
            }
        }

        @JavascriptInterface
        fun onNewOrder(orderId: String, customerName: String) {
            runOnUiThread { showNewOrderNotification(orderId.take(30), customerName.take(100)) }
        }
    }

    override fun onNewIntent(intent: Intent) {
        super.onNewIntent(intent)
        setIntent(intent)
        if (intent.getBooleanExtra("show_orders", false)) {
            webView.loadUrl(posStartUrl(intent))
        }
    }

    override fun onSaveInstanceState(outState: Bundle) {
        webView.saveState(outState)
        super.onSaveInstanceState(outState)
    }

    @Deprecated("Use the POS navigation history when available")
    override fun onBackPressed() {
        if (webView.canGoBack()) webView.goBack() else super.onBackPressed()
    }

    override fun onPause() {
        uiHandler.removeCallbacks(saleScreenPoll)
        webView.onPause()
        super.onPause()
    }

    override fun onResume() {
        super.onResume()
        webView.onResume()
        if (webView.url != null) uiHandler.post(saleScreenPoll)
    }

    override fun onDestroy() {
        uiHandler.removeCallbacksAndMessages(null)
        (webView.parent as? ViewGroup)?.removeView(webView)
        webView.stopLoading()
        webView.destroy()
        super.onDestroy()
    }

    private fun dp(value: Int): Int = (value * resources.displayMetrics.density).toInt()

    companion object {
        private const val ORDER_CHANNEL_ID = "sucree_pos_orders"
        private const val NOTIFICATION_PERMISSION_REQUEST = 5102
    }
}
