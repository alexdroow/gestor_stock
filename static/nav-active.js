(function () {
  function norm(path) {
    var value = String(path || "").split("?")[0].split("#")[0];
    if (!value) return "/";
    if (value.length > 1 && value.endsWith("/")) value = value.slice(0, -1);
    return value || "/";
  }

  var current = norm(window.location.pathname);
  var links = document.querySelectorAll(".nav-menu .nav-item");
  links.forEach(function (link) {
    var href = norm(link.getAttribute("href"));
    var isHistorialVentas = current === "/historial-ventas" && href === "/ventas";
    var active = href === current || isHistorialVentas;
    link.classList.toggle("active", active);
    if (active) {
      link.setAttribute("aria-current", "page");
    } else {
      link.removeAttribute("aria-current");
    }
  });

  function updateAlertBadge() {
    var badge = document.getElementById("nav-alertas");
    if (!badge) return;

    fetch("/api/alertas/contador")
      .then(function (r) { return r.json(); })
      .then(function (data) {
        var total = Number((data && data.total) || 0);
        badge.textContent = String(total);
        badge.style.display = total > 0 ? "inline-flex" : "none";
      })
      .catch(function () {
        // Mantener estado actual del badge si falla la consulta.
      });
  }

  updateAlertBadge();
  window.setInterval(updateAlertBadge, 30000);
})();
