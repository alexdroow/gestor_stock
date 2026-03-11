(function () {
  function parseJSON(raw, fallback) {
    try {
      return JSON.parse(raw);
    } catch (_) {
      return fallback;
    }
  }

  function pageKey(suffix) {
    var path = String(window.location.pathname || "/")
      .replace(/\/+$/, "")
      .replace(/[^a-zA-Z0-9/_-]/g, "");
    if (!path) path = "/";
    var cleanSuffix = String(suffix || "state").replace(/[^a-zA-Z0-9_-]/g, "_");
    return "ux:" + path + ":" + cleanSuffix;
  }

  function getStore() {
    try {
      return window.sessionStorage;
    } catch (_) {
      return null;
    }
  }

  function saveState(key, value) {
    var store = getStore();
    if (!store || !key) return false;
    try {
      store.setItem(key, JSON.stringify(value || {}));
      return true;
    } catch (_) {
      return false;
    }
  }

  function loadState(key, consume) {
    var store = getStore();
    if (!store || !key) return null;
    var raw = store.getItem(key);
    if (!raw) return null;
    if (consume !== false) {
      store.removeItem(key);
    }
    return parseJSON(raw, null);
  }

  function reloadKeepingState(key, state) {
    saveState(key, state || {});
    window.location.reload();
  }

  function restoreScroll(state) {
    if (!state || typeof state.scroll_y === "undefined") return;
    var y = Number(state.scroll_y || 0);
    if (!Number.isFinite(y) || y <= 0) return;
    window.requestAnimationFrame(function () {
      window.scrollTo(0, y);
    });
  }

  function wantsJSONResponse() {
    return {
      Accept: "application/json",
      "X-Requested-With": "fetch",
    };
  }

  async function submitFormJSON(formEl, options) {
    if (!formEl) {
      throw new Error("Formulario no encontrado");
    }
    var opts = options || {};
    var url = opts.url || formEl.getAttribute("action") || window.location.pathname;
    var method = (opts.method || formEl.getAttribute("method") || "POST").toUpperCase();
    var body = opts.body || new FormData(formEl);
    var headers = Object.assign({}, wantsJSONResponse(), opts.headers || {});

    var response = await fetch(url, {
      method: method,
      body: body,
      headers: headers,
    });
    var data = await response.json().catch(function () {
      return { success: false, error: "Respuesta invalida del servidor" };
    });
    if (!response.ok || data.success === false) {
      throw new Error((data && data.error) || "No se pudo completar la operacion");
    }
    return data;
  }

  function wireGetFormPersistence() {
    var forms = document.querySelectorAll("form[method='GET'], form[method='get']");
    if (!forms.length) return;

    forms.forEach(function (form, idx) {
      var key = pageKey("getform_" + idx);
      var restore = loadState(key, true);
      if (restore && restore.values) {
        Object.keys(restore.values).forEach(function (name) {
          var el = form.querySelector("[name='" + name + "']");
          if (!el) return;
          el.value = restore.values[name];
        });
      }

      form.addEventListener("submit", function () {
        var values = {};
        var fields = form.querySelectorAll("input[name], select[name], textarea[name]");
        fields.forEach(function (field) {
          values[field.name] = field.value;
        });
        saveState(key, { values: values, scroll_y: window.scrollY || 0 });
      });
    });
  }

  function wireModalOutsideProtection() {
    document.addEventListener(
      "click",
      function (event) {
        var target = event.target;
        if (!target || !target.classList || !target.classList.contains("modal-overlay")) {
          return;
        }

        // Seguridad por defecto: evita cierres accidentales al hacer click fuera.
        // Si una pantalla quiere permitirlo explicitamente:
        // <div class="modal-overlay" data-allow-outside-close="true">
        var allowOutsideClose = String(target.getAttribute("data-allow-outside-close") || "")
          .trim()
          .toLowerCase();
        if (allowOutsideClose === "true" || allowOutsideClose === "1") {
          return;
        }

        event.preventDefault();
        event.stopImmediatePropagation();
      },
      true
    );
  }

  function ensureToastStyles() {
    if (document.getElementById("appux-toast-styles")) return;
    var style = document.createElement("style");
    style.id = "appux-toast-styles";
    style.textContent =
      ".appux-toast-root{position:fixed;top:20px;right:20px;z-index:99999;display:flex;flex-direction:column;gap:10px;max-width:min(92vw,420px);pointer-events:none}" +
      ".appux-toast{pointer-events:auto;display:flex;align-items:flex-start;gap:10px;padding:12px 14px;border-radius:12px;border:1px solid #e2e8f0;background:#ffffff;color:#0f172a;box-shadow:0 10px 25px rgba(2,6,23,.14);font-size:13px;line-height:1.4;animation:appux-toast-in .2s ease-out}" +
      ".appux-toast .icon{font-size:16px;line-height:1;margin-top:1px}" +
      ".appux-toast .msg{flex:1;word-break:break-word}" +
      ".appux-toast .close{border:none;background:transparent;color:inherit;cursor:pointer;font-size:15px;line-height:1;opacity:.65}" +
      ".appux-toast.success{border-color:#86efac;background:#f0fdf4;color:#166534}" +
      ".appux-toast.error{border-color:#fecaca;background:#fef2f2;color:#991b1b}" +
      ".appux-toast.warning{border-color:#fed7aa;background:#fff7ed;color:#9a3412}" +
      ".appux-toast.info{border-color:#bfdbfe;background:#eff6ff;color:#1e3a8a}" +
      "body.dark-mode .appux-toast{background:#1e293b;border-color:#475569;color:#e2e8f0;box-shadow:0 12px 28px rgba(2,6,23,.45)}" +
      "body.dark-mode .appux-toast.success{background:#052e1a;border-color:#14532d;color:#bbf7d0}" +
      "body.dark-mode .appux-toast.error{background:#3b0a0a;border-color:#7f1d1d;color:#fecaca}" +
      "body.dark-mode .appux-toast.warning{background:#3b2302;border-color:#7c2d12;color:#fde68a}" +
      "body.dark-mode .appux-toast.info{background:#0b264a;border-color:#1d4ed8;color:#bfdbfe}" +
      "@keyframes appux-toast-in{from{opacity:0;transform:translateY(-6px)}to{opacity:1;transform:translateY(0)}}";
    document.head.appendChild(style);
  }

  function ensureToastRoot() {
    ensureToastStyles();
    var root = document.getElementById("appux-toast-root");
    if (!root) {
      root = document.createElement("div");
      root.id = "appux-toast-root";
      root.className = "appux-toast-root";
      document.body.appendChild(root);
    }
    return root;
  }

  function cleanupAlertText(text) {
    var value = String(text || "").trim();
    value = value.replace(/^\[ERROR\]\s*/i, "");
    value = value.replace(/^Error:\s*/i, "");
    value = value.replace(/^\[OK\]\s*/i, "");
    value = value.replace(/^OK\s*/i, "");
    return value || "Operacion completada";
  }

  function inferToastType(message, explicitType) {
    var type = String(explicitType || "").trim().toLowerCase();
    if (type) return type;
    var raw = String(message || "").toLowerCase();
    if (raw.indexOf("[error]") >= 0 || raw.indexOf("error") === 0) return "error";
    if (raw.indexOf("no se pudo") >= 0 || raw.indexOf("fallo") >= 0) return "error";
    if (raw.indexOf("alerta") >= 0 || raw.indexOf("atencion") >= 0) return "warning";
    if (
      raw.indexOf("guard") >= 0 ||
      raw.indexOf("actualiz") >= 0 ||
      raw.indexOf("agreg") >= 0 ||
      raw.indexOf("elimin") >= 0 ||
      raw.indexOf("exito") >= 0
    ) {
      return "success";
    }
    return "info";
  }

  function iconByType(type) {
    if (type === "success") return "OK";
    if (type === "error") return "!";
    if (type === "warning") return "WARN";
    return "i";
  }

  function toast(message, options) {
    if (!document.body) return;
    var opts = options || {};
    var type = inferToastType(message, opts.type);
    var timeout = Number(opts.timeout || 3500);
    if (!Number.isFinite(timeout) || timeout < 1200) timeout = 1200;

    var root = ensureToastRoot();
    var node = document.createElement("div");
    node.className = "appux-toast " + type;
    node.setAttribute("role", "status");
    node.innerHTML =
      '<span class="icon" aria-hidden="true">' +
      iconByType(type) +
      '</span><span class="msg"></span><button type="button" class="close" aria-label="Cerrar">×</button>';

    var msgEl = node.querySelector(".msg");
    if (msgEl) msgEl.textContent = cleanupAlertText(message);

    var remove = function () {
      if (!node.parentNode) return;
      node.parentNode.removeChild(node);
    };

    var closeBtn = node.querySelector(".close");
    if (closeBtn) closeBtn.addEventListener("click", remove);

    root.appendChild(node);
    window.setTimeout(remove, timeout);
    return node;
  }

  var nativeAlert = typeof window.alert === "function" ? window.alert.bind(window) : null;

  function prettyAlert(message) {
    try {
      if (!document.body) {
        if (nativeAlert) nativeAlert(String(message || ""));
        return;
      }
      toast(message, {});
    } catch (_) {
      if (nativeAlert) nativeAlert(String(message || ""));
    }
  }

  window.AppUX = {
    pageKey: pageKey,
    saveState: saveState,
    loadState: loadState,
    reloadKeepingState: reloadKeepingState,
    restoreScroll: restoreScroll,
    submitFormJSON: submitFormJSON,
    wireGetFormPersistence: wireGetFormPersistence,
    wireModalOutsideProtection: wireModalOutsideProtection,
    toast: toast,
    toastSuccess: function (message, options) {
      return toast(message, Object.assign({}, options || {}, { type: "success" }));
    },
    toastError: function (message, options) {
      return toast(message, Object.assign({}, options || {}, { type: "error" }));
    },
    toastWarning: function (message, options) {
      return toast(message, Object.assign({}, options || {}, { type: "warning" }));
    },
    toastInfo: function (message, options) {
      return toast(message, Object.assign({}, options || {}, { type: "info" }));
    },
    nativeAlert: nativeAlert,
  };

  window.alert = prettyAlert;

  document.addEventListener("DOMContentLoaded", function () {
    wireGetFormPersistence();
    wireModalOutsideProtection();
  });
})();
