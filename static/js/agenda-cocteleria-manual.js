(() => {
  const $ = (selector) => document.querySelector(selector);
  const esc = (value) => String(value ?? '').replace(/[&<>"']/g, (char) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[char]));
  const money = (value) => '$' + Number(value || 0).toLocaleString('es-CL');
  let products = [];
  let lines = [];
  let extras = [];
  let days = [];
  let dayOffset = 0;
  let selectedDate = '';
  let selectedHour = '';
  let saving = false;
  let savedReservation = null;

  function status(message, isError = false) {
    const node = $('#agenda-coct-status');
    if (!node) return;
    node.textContent = message || '';
    node.style.color = isError ? '#a93220' : '';
  }

  function productIdsForAvailability() {
    const ids = new Set(lines.map((line) => Number(line.model_id)));
    const current = Number($('#agenda-coct-product')?.value || 0);
    if (current) ids.add(current);
    return [...ids].filter(Boolean);
  }

  function renderDays() {
    const host = $('#agenda-coct-days');
    if (!host) return;
    const visible = days.slice(dayOffset, dayOffset + 14);
    host.innerHTML = visible.map((day) => {
      const date = String(day.fecha || '');
      const parsed = new Date(`${date}T12:00:00`);
      const label = new Intl.DateTimeFormat('es-CL', { weekday: 'short', day: '2-digit', month: '2-digit' }).format(parsed);
      const hasSlots = (day.horas || []).some((slot) => slot.disponible);
      return `<button type="button" class="agenda-cocktail-day ${date === selectedDate ? 'selected' : ''}" data-date="${esc(date)}" ${hasSlots ? '' : 'disabled'}><span>${esc(label)}</span><small>${hasSlots ? 'Disponible' : 'Sin cupos'}</small></button>`;
    }).join('') || '<p class="agenda-cocktail-empty">No hay fechas disponibles en el período consultado.</p>';
    host.querySelectorAll('[data-date]').forEach((button) => button.addEventListener('click', () => selectDay(button.dataset.date)));
    $('#agenda-coct-prev').disabled = dayOffset <= 0;
    $('#agenda-coct-next').disabled = dayOffset + 14 >= days.length;
  }

  function renderHours() {
    const host = $('#agenda-coct-hours');
    const day = days.find((entry) => entry.fecha === selectedDate);
    if (!host) return;
    const slots = (day?.horas || []).filter((slot) => slot.disponible);
    host.innerHTML = slots.map((slot) => `<button type="button" class="agenda-cocktail-hour ${slot.hora_inicio === selectedHour ? 'selected' : ''}" data-hour="${esc(slot.hora_inicio)}">${esc(slot.hora_inicio)}</button>`).join('') || '<p class="agenda-cocktail-empty">Selecciona una fecha con horarios disponibles.</p>';
    host.querySelectorAll('[data-hour]').forEach((button) => button.addEventListener('click', () => {
      selectedHour = button.dataset.hour;
      renderHours();
    }));
  }

  function selectDay(date) {
    selectedDate = date;
    const day = days.find((entry) => entry.fecha === date);
    const first = (day?.horas || []).find((slot) => slot.disponible);
    selectedHour = first?.hora_inicio || '';
    renderDays();
    renderHours();
  }

  async function loadAvailability() {
    const message = $('#agenda-coct-calendar-message');
    if (message) message.textContent = 'Consultando fechas y horarios disponibles...';
    const params = new URLSearchParams({ tipo: 'cocteleria', admin_scope: 'agenda_manual' });
    productIdsForAvailability().forEach((id) => params.append('producto_ids', String(id)));
    const response = await fetch(`/api/tienda/agenda/disponibilidad?${params.toString()}`);
    const data = await response.json();
    if (!response.ok || !data.success) throw new Error(data.error || 'No se pudo cargar el calendario.');
    days = Array.isArray(data.dias) ? data.dias : [];
    const previousDate = selectedDate;
    if (previousDate && days.some((day) => day.fecha === previousDate && (day.horas || []).some((slot) => slot.disponible))) {
      selectedDate = previousDate;
      if (!days.find((day) => day.fecha === selectedDate)?.horas?.some((slot) => slot.hora_inicio === selectedHour && slot.disponible)) selectedHour = '';
    } else {
      const firstDay = days.find((day) => (day.horas || []).some((slot) => slot.disponible));
      selectedDate = firstDay?.fecha || '';
      selectedHour = '';
      dayOffset = Math.max(0, days.findIndex((day) => day.fecha === selectedDate));
      dayOffset = Math.floor(dayOffset / 14) * 14;
    }
    if (message) message.textContent = data.enabled ? 'Las fechas y horas reflejan los cupos de la agenda.' : 'La agenda de reservas está desactivada; activa su disponibilidad para mostrar fechas.';
    renderDays();
    renderHours();
  }

  function renderProductOptions() {
    const select = $('#agenda-coct-product');
    select.innerHTML = '<option value="">Selecciona un producto</option>' + products.map((product) => `<option value="${product.id}">${esc(product.name)} · ${product.category === 'dulce' ? 'Dulce' : 'Salado'}</option>`).join('');
  }

  function renderPresentations() {
    const product = products.find((item) => Number(item.id) === Number($('#agenda-coct-product').value));
    const select = $('#agenda-coct-presentation');
    const presentations = (product?.presentations || []).filter((item) => Number(item.active));
    select.innerHTML = presentations.length
      ? '<option value="">Selecciona una presentación</option>' + presentations.map((item) => `<option value="${item.id}">${esc(item.label)} · ${money(item.price_clp)}</option>`).join('')
      : '<option value="">Selecciona un producto primero</option>';
    select.disabled = !presentations.length;
    $('#agenda-coct-quantity').min = String(product?.min_quantity || 1);
    $('#agenda-coct-quantity').max = String(product?.max_quantity || 999);
    $('#agenda-coct-quantity').value = String(product?.min_quantity || 1);
  }

  function renderLines() {
    const host = $('#agenda-coct-lines');
    host.innerHTML = lines.length ? lines.map((line, index) => `<div class="agenda-cocktail-line"><span><strong>${esc(line.product_name)}</strong><small>${esc(line.presentation_label)} · ${money(line.unit_price_clp)} c/u</small></span><input type="number" min="1" max="${line.max_quantity}" value="${line.quantity}" data-quantity="${index}" aria-label="Cantidad de ${esc(line.product_name)}"><button type="button" class="agenda-cocktail-remove" data-remove-line="${index}" aria-label="Quitar ${esc(line.product_name)}">×</button></div>`).join('') : '<p class="agenda-cocktail-empty">Aún no agregas productos.</p>';
    host.querySelectorAll('[data-quantity]').forEach((input) => input.addEventListener('change', () => {
      const line = lines[Number(input.dataset.quantity)];
      line.quantity = Math.max(line.min_quantity, Math.min(line.max_quantity, Number(input.value || line.min_quantity)));
      input.value = String(line.quantity);
      renderTotal();
    }));
    host.querySelectorAll('[data-remove-line]').forEach((button) => button.addEventListener('click', () => {
      lines.splice(Number(button.dataset.removeLine), 1);
      renderLines(); renderTotal(); loadAvailability().catch((error) => status(error.message, true));
    }));
    renderTotal();
  }

  function renderExtras() {
    const host = $('#agenda-coct-extras');
    host.innerHTML = extras.map((extra, index) => `<div class="agenda-cocktail-extra"><input data-extra-description="${index}" maxlength="100" placeholder="Descripción del cargo" value="${esc(extra.description)}"><input data-extra-value="${index}" type="number" min="0" step="1" placeholder="Valor CLP" value="${extra.value || ''}"><button type="button" class="agenda-cocktail-remove" data-remove-extra="${index}" aria-label="Quitar cargo">×</button></div>`).join('');
    host.querySelectorAll('[data-extra-description]').forEach((input) => input.addEventListener('input', () => { extras[Number(input.dataset.extraDescription)].description = input.value; }));
    host.querySelectorAll('[data-extra-value]').forEach((input) => input.addEventListener('input', () => { extras[Number(input.dataset.extraValue)].value = Math.max(0, Number(input.value || 0)); renderTotal(); }));
    host.querySelectorAll('[data-remove-extra]').forEach((button) => button.addEventListener('click', () => { extras.splice(Number(button.dataset.removeExtra), 1); renderExtras(); renderTotal(); }));
  }

  function renderTotal() {
    const productsTotal = lines.reduce((sum, line) => sum + Number(line.unit_price_clp) * Number(line.quantity), 0);
    const extrasTotal = extras.reduce((sum, item) => sum + Math.max(0, Number(item.value || 0)), 0);
    $('#agenda-coct-total').textContent = money(productsTotal + extrasTotal);
  }

  function reservationSummary(payload, code, id) {
    const productsTotal = payload.items.reduce((sum, item) => sum + Number(item.unit_price_clp) * Number(item.quantity), 0);
    const extrasTotal = payload.extras.reduce((sum, item) => sum + Number(item.value || 0), 0);
    const total = productsTotal + extrasTotal;
    const deposit = Number(payload.abono || 0);
    const delivery = payload.entrega_tipo === 'despacho' ? 'Despacho' : 'Retiro en tienda';
    const details = $('#agenda-coct-confirmation-details');
    details.innerHTML = `
      <div class="agenda-cocktail-confirmation-card"><span>Cliente</span><strong>${esc(payload.nombre)}</strong><small>${esc(payload.telefono)}${payload.email ? ` · ${esc(payload.email)}` : ''}</small></div>
      <div class="agenda-cocktail-confirmation-card"><span>Fecha y hora</span><strong>${esc(payload.fecha)} · ${esc(payload.hora_inicio)}</strong><small>${delivery}${payload.entrega_tipo === 'despacho' && payload.direccion ? ` · ${esc(payload.direccion)}` : ''}</small></div>
      <div class="agenda-cocktail-confirmation-card agenda-cocktail-confirmation-items"><span>Productos</span>${payload.items.map((item) => `<div><strong>${esc(item.product_name)}</strong><small>${esc(item.presentation_label)} × ${Number(item.quantity)} · ${money(Number(item.unit_price_clp) * Number(item.quantity))}</small></div>`).join('')}${payload.extras.map((item) => `<div><strong>${esc(item.description)}</strong><small>Cargo extra · ${money(item.value)}</small></div>`).join('')}</div>
      ${payload.nota ? `<div class="agenda-cocktail-confirmation-card"><span>Nota</span><strong>${esc(payload.nota)}</strong></div>` : ''}
      <div class="agenda-cocktail-confirmation-total"><span>Total <b>${money(total)}</b></span><span>Abono <b>${money(deposit)}</b></span><span>Saldo <b>${money(Math.max(0, total - deposit))}</b></span></div>`;
    $('#agenda-coct-confirmation-code').textContent = `${code || `Reserva #${id}`} · ${payload.fecha} · ${payload.hora_inicio}`;
    const message = [
      'Hola, te compartimos el resumen de tu reserva de coctelería en Sucrée:',
      `Reserva: ${code || `#${id}`}`,
      `Cliente: ${payload.nombre}`,
      `Fecha: ${payload.fecha} a las ${payload.hora_inicio}`,
      `Entrega: ${delivery}${payload.entrega_tipo === 'despacho' && payload.direccion ? ` - ${payload.direccion}` : ''}`,
      'Productos:',
      ...payload.items.map((item) => `- ${item.product_name} / ${item.presentation_label} x ${item.quantity}: ${money(Number(item.unit_price_clp) * Number(item.quantity))}`),
      ...payload.extras.map((item) => `- ${item.description}: ${money(item.value)}`),
      `Total: ${money(total)}`, `Abono: ${money(deposit)}`, `Saldo: ${money(Math.max(0, total - deposit))}`,
      ...(payload.nota ? [`Nota: ${payload.nota}`] : []),
    ].join('\n');
    const digits = String(payload.telefono || '').replace(/\D/g, '');
    const destination = digits.startsWith('56') ? digits : `56${digits.replace(/^0+/, '')}`;
    $('#agenda-coct-whatsapp').href = `https://wa.me/${destination}?text=${encodeURIComponent(message)}`;
    savedReservation = { ...payload, code, id, total };
    $('#agenda-coct-confirmation').hidden = false;
    $('.agenda-cocktail-grid').hidden = true;
  }

  function ticketHtml(reservation) {
    const rows = reservation.items.map((item) => `<p>${esc(item.product_name)}<br>${esc(item.presentation_label)} x ${Number(item.quantity)}<br>${money(Number(item.unit_price_clp) * Number(item.quantity))}</p>`).join('');
    const extras = reservation.extras.map((item) => `<p>${esc(item.description)}<br>${money(item.value)}</p>`).join('');
    const address = reservation.entrega_tipo === 'despacho' ? reservation.direccion : 'Sucrée Pastelería';
    return `<!doctype html><html lang="es"><head><meta charset="utf-8"><title>Reserva ${esc(reservation.code)}</title><style>@page{size:58mm auto;margin:3mm}*{box-sizing:border-box}body{width:52mm;margin:0 auto;color:#111;font:11px/1.3 Arial,sans-serif;overflow-wrap:anywhere}.center{text-align:center}.brand{font:bold 17px Georgia,serif}.rule{border:0;border-top:1px dashed #111;margin:7px 0}h3{font-size:11px;margin:8px 0 4px;border-bottom:1px solid #111;padding-bottom:3px}p{margin:4px 0}.total{font-weight:bold;border-top:1px dashed #111;padding-top:6px}</style></head><body><header class="center"><div class="brand">SUCRÉE</div><div>PASTELERÍA · RESERVA CÓCTEL</div></header><hr class="rule"><p><b>${esc(reservation.code)}</b></p><p><b>Cliente:</b> ${esc(reservation.nombre)}<br><b>Teléfono:</b> ${esc(reservation.telefono)}</p><h3>FECHA Y ENTREGA</h3><p>${esc(reservation.fecha)} · ${esc(reservation.hora_inicio)}<br>${reservation.entrega_tipo === 'despacho' ? 'Despacho' : 'Retiro en tienda'}<br>${esc(address || '-')}</p><h3>DETALLE</h3>${rows}${extras ? `<h3>VALORES EXTRA</h3>${extras}` : ''}<hr class="rule"><p class="total">Total: ${money(reservation.total)}<br>Abono: ${money(reservation.abono)}<br>Saldo: ${money(Math.max(0, reservation.total - reservation.abono))}</p>${reservation.nota ? `<h3>NOTA</h3><p>${esc(reservation.nota)}</p>` : ''}<hr class="rule"><p class="center">Gracias por preferir Sucrée</p></body></html>`;
  }

  function printReservation() {
    if (!savedReservation) return;
    const html = ticketHtml(savedReservation);
    if (new URLSearchParams(window.location.search).get('pos_embed') === '1' && window.SucreePosAndroid?.printAgendaTicket) {
      window.SucreePosAndroid.printAgendaTicket(String(savedReservation.id), html);
      return;
    }
    const printWindow = window.open('', '_blank', 'width=360,height=720');
    if (!printWindow) return status('Permite las ventanas emergentes para imprimir el comprobante.', true);
    printWindow.document.open(); printWindow.document.write(html); printWindow.document.close();
    setTimeout(() => { printWindow.focus(); printWindow.print(); }, 250);
  }

  async function addLine() {
    const product = products.find((item) => Number(item.id) === Number($('#agenda-coct-product').value));
    const presentation = (product?.presentations || []).find((item) => Number(item.id) === Number($('#agenda-coct-presentation').value));
    if (!product || !presentation) return status('Selecciona un producto y su presentación.', true);
    const quantity = Number($('#agenda-coct-quantity').value || product.min_quantity || 1);
    if (quantity < Number(product.min_quantity) || quantity > Number(product.max_quantity)) return status(`La cantidad de ${product.name} debe estar entre ${product.min_quantity} y ${product.max_quantity}.`, true);
    const existing = lines.find((line) => line.model_id === Number(product.id) && line.presentation_id === Number(presentation.id));
    if (existing) existing.quantity = Math.min(Number(product.max_quantity), existing.quantity + quantity);
    else lines.push({ model_id: Number(product.id), presentation_id: Number(presentation.id), product_name: product.name, presentation_label: presentation.label, quantity, unit_price_clp: Number(presentation.price_clp), expected_price_clp: Number(presentation.price_clp), min_quantity: Number(product.min_quantity), max_quantity: Number(product.max_quantity) });
    renderLines();
    status('Producto añadido al pedido.');
    await loadAvailability();
  }

  async function loadCatalog() {
    const response = await fetch('/api/tienda/cocteleria');
    const data = await response.json();
    if (!response.ok || !data.success) throw new Error(data.error || 'No se pudo cargar el catálogo.');
    products = Array.isArray(data.items) ? data.items : [];
    renderProductOptions();
    renderPresentations();
  }

  async function save() {
    if (saving) return;
    if (!lines.length) return status('Agrega al menos un producto del catálogo.', true);
    if (!selectedDate || !selectedHour) return status('Selecciona una fecha y hora disponible.', true);
    const name = $('#agenda-coct-customer').value.trim();
    const phone = $('#agenda-coct-phone').value.trim();
    if (name.length < 2 || !phone) return status('Completa el nombre y teléfono del cliente.', true);
    const delivery = $('#agenda-coct-delivery').value;
    const address = $('#agenda-coct-address').value.trim();
    if (delivery === 'despacho' && !address) return status('Ingresa la dirección de despacho.', true);
    const deposit = Math.max(0, Number($('#agenda-coct-deposit').value || 0));
    const payload = {
      fecha: selectedDate, hora_inicio: selectedHour, nombre: name, telefono: phone,
      email: $('#agenda-coct-email').value.trim(), entrega_tipo: delivery, direccion: address,
      nota: $('#agenda-coct-note').value.trim(), abono: deposit, items: lines,
      extras: extras.filter((extra) => extra.description.trim() && Number(extra.value) > 0),
    };
    saving = true;
    const button = $('#agenda-coct-save');
    button.disabled = true; button.textContent = 'Guardando...'; status('Guardando reserva y generando su detalle...');
    try {
      const response = await fetch('/api/agenda/cocteleria/manual', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
      const data = await response.json();
      if (!response.ok || !data.success) throw new Error(data.error || 'No se pudo guardar la reserva.');
      if (typeof cargarEventos === 'function') await cargarEventos();
      window.cerrarAgendaCocteleriaManual();
      if (typeof seleccionarDiaCalendario === 'function') seleccionarDiaCalendario(payload.fecha);
      if (typeof abrirDetallePedidoAgenda === 'function') abrirDetallePedidoAgenda(Number(data.id));
    } catch (error) {
      status(error.message || 'No se pudo guardar la reserva.', true);
      button.disabled = false; button.textContent = 'Guardar reserva';
    } finally {
      saving = false;
    }
  }

  window.abrirAgendaCocteleriaManual = async function () {
    const modal = $('#agenda-cocteleria-modal');
    modal.classList.add('is-open'); modal.setAttribute('aria-hidden', 'false'); document.body.style.overflow = 'hidden';
    lines = []; extras = []; days = []; selectedDate = ''; selectedHour = ''; dayOffset = 0; saving = false;
    savedReservation = null;
    $('#agenda-coct-customer').value = ''; $('#agenda-coct-phone').value = ''; $('#agenda-coct-email').value = '';
    $('#agenda-coct-delivery').value = 'retiro'; $('#agenda-coct-address').value = ''; $('#agenda-coct-note').value = ''; $('#agenda-coct-deposit').value = '0';
    $('#agenda-coct-address-wrap').hidden = true; $('#agenda-coct-pdf').hidden = true;
    $('#agenda-coct-confirmation').hidden = true; $('.agenda-cocktail-grid').hidden = false;
    const saveButton = $('#agenda-coct-save'); saveButton.disabled = false; saveButton.textContent = 'Guardar reserva';
    renderLines(); renderExtras(); status('');
    try { await loadCatalog(); await loadAvailability(); } catch (error) { status(error.message, true); }
  };

  window.cerrarAgendaCocteleriaManual = function () {
    const modal = $('#agenda-cocteleria-modal');
    modal.classList.remove('is-open'); modal.setAttribute('aria-hidden', 'true'); document.body.style.overflow = '';
  };

  document.addEventListener('DOMContentLoaded', () => {
    $('#agenda-coct-product')?.addEventListener('change', renderPresentations);
    $('#agenda-coct-add')?.addEventListener('click', () => addLine().catch((error) => status(error.message, true)));
    $('#agenda-coct-add-extra')?.addEventListener('click', () => { extras.push({ description: '', value: 0 }); renderExtras(); });
    $('#agenda-coct-save')?.addEventListener('click', save);
    $('#agenda-coct-print')?.addEventListener('click', printReservation);
    $('#agenda-coct-delivery')?.addEventListener('change', () => { $('#agenda-coct-address-wrap').hidden = $('#agenda-coct-delivery').value !== 'despacho'; });
    $('#agenda-coct-prev')?.addEventListener('click', () => { dayOffset = Math.max(0, dayOffset - 14); renderDays(); });
    $('#agenda-coct-next')?.addEventListener('click', () => { dayOffset = Math.min(Math.max(0, days.length - 14), dayOffset + 14); renderDays(); });
    $('#agenda-coct-product')?.addEventListener('change', () => loadAvailability().catch((error) => status(error.message, true)));
    $('#agenda-coct-deposit')?.addEventListener('input', renderTotal);
    $('#agenda-coct-modal')?.addEventListener('click', (event) => { if (event.target.id === 'agenda-coct-modal') window.cerrarAgendaCocteleriaManual(); });
    document.addEventListener('keydown', (event) => { if (event.key === 'Escape' && $('#agenda-cocteleria-modal')?.classList.contains('is-open')) window.cerrarAgendaCocteleriaManual(); });
  });
})();
