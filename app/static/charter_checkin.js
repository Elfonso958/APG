(() => {
  const app = document.getElementById("charterCheckinApp");
  if (!app) return;
  const $ = (id) => document.getElementById(id);
  const els = { day: $("dayInput"), refresh: $("refreshBtn"), notice: $("notice"), flightList: $("flightList"), flightCount: $("flightCount"), empty: $("emptyState"), content: $("checkinContent"), title: $("selectedFlightTitle"), meta: $("selectedFlightMeta"), summary: $("summary"), template: $("templateLink"), file: $("manifestFile"), search: $("searchInput"), filter: $("statusFilter"), rows: $("passengerRows"), manifestMeta: $("manifestMeta"), toggleFlights: $("toggleFlightPanel"), addPassenger: $("addPassenger"), seatmapDialog: $("seatmapDialog"), seatmapTitle: $("seatmapTitle"), seatmapHelp: $("seatmapHelp"), seatmapGrid: $("seatmapGrid"), seatmapClose: $("seatmapClose"), scanButton: $("scanBoardingPass"), scanDialog: $("boardingScanDialog"), scanClose: $("boardingScanClose"), scanStatus: $("boardingScanStatus"), scanVideo: $("boardingScanVideo"), scanCode: $("boardingScanCode"), scanProcess: $("processBoardingCode"), checkinDialog: $("checkinDialog"), checkinClose: $("checkinClose"), checkinTitle: $("checkinTitle"), checkinMeta: $("checkinPassengerMeta"), checkinIdentity: $("checkinIdentity"), checkinGivenName: $("checkinGivenName"), checkinSurname: $("checkinSurname"), checkinPassengerType: $("checkinPassengerType"), checkinPnr: $("checkinPnr"), checkinSeat: $("checkinSeat"), checkinSeatmapGrid: $("checkinSeatmapGrid"), checkinBagKg: $("checkinBagKg"), checkinBagPieces: $("checkinBagPieces"), checkinSsrCode: $("checkinSsrCode"), checkinSsrText: $("checkinSsrText"), addCheckinSsr: $("addCheckinSsr"), checkinSsrList: $("checkinSsrList"), checkinComments: $("checkinComments"), confirmCheckin: $("confirmCheckin"), confirmCheckinPrint: $("confirmCheckinPrint") };
  els.closeFlight = $("closeFlightBtn");
  els.reopenFlight = $("reopenFlightBtn");
  els.gate = $("flightGate");
  els.saveGate = $("saveGateBtn");
  els.openGate = $("openGateDialog");
  els.gateDialog = $("gateDialog");
  els.gateDialogClose = $("gateDialogClose");
  els.clearGate = $("clearGateBtn");
  els.scanLastPassenger = $("boardingLastPassenger");
  els.seatChangeAlert = $("seatChangeAlert");
  els.seatChangeMessage = $("seatChangeMessage");
  els.acknowledgeSeatChange = $("acknowledgeSeatChange");
  els.checkinNamePrefix = $("checkinNamePrefix");
  els.checkinPassengerWeight = $("checkinPassengerWeight");
  const workspace = document.querySelector(".workspace");
  const state = { flights: [], flight: null, passengers: [] };
  let scanStream = null, scanTimer = null, scanProcessing = false, scanAudioContext = null, manifestRefreshInFlight = false, pendingSeatChangeCode = "";
  let checkinPassenger = null, checkinSsrs = [];
  const gateWarningShown = new Set();
  const ssrOptions = [["", "Choose an SSR code"], ["WCHR", "Wheelchair — ramp / distance"], ["WCHS", "Wheelchair — steps assistance"], ["WCHC", "Wheelchair — cabin seat transfer"], ["BLND", "Blind or low-vision passenger"], ["DEAF", "Deaf or hard-of-hearing passenger"], ["MAAS", "Meet and assist"], ["DPNA", "Disability / non-visible assistance"], ["UMNR", "Unaccompanied minor"], ["MEDA", "Medical case — clearance may be needed"], ["OXYG", "Supplementary oxygen"], ["EXST", "Extra seat"], ["STCR", "Stretcher"], ["PETC", "Pet in cabin"], ["AVIH", "Animal in hold"], ["WEAP", "Weapon handling"], ["INFT", "Infant accompanying passenger"], ["OTHS", "Other special service"]];
  const esc = (value) => String(value ?? "").replace(/[&<>'"]/g, (c) => ({ "&":"&amp;", "<":"&lt;", ">":"&gt;", "'":"&#39;", '"':"&quot;" }[c]));
  const statusOf = (p) => p.Flown || String(p.Status || "").toLowerCase() === "flown" ? "Flown" : p.Boarded || String(p.Status || "").toLowerCase() === "boarded" ? "Boarded" : /check/i.test(String(p.Status || "")) ? "Checked In" : "Booked";
  const nameOf = (p) => [p.NamePrefix, p.GivenName, p.Surname].filter(Boolean).join(" ").trim() || "Unnamed passenger";
  const passengerWeightForType = (type) => ({ AD:86, T:96, CHD:46, INF:15, UMNR:46 }[String(type || "AD").toUpperCase()] || 86);
  const isCharter = (f) => /charter/.test(`${f.service_type || ""} ${f.flight_type || ""}`.toLowerCase());
  function showNotice(text, error = false) { els.notice.textContent = text; els.notice.hidden = !text; els.notice.classList.toggle("error", error); }
  function setGateSelection(value = "") {
    els.gate.value = String(value || "").trim().toUpperCase();
  }
  function openGateDialog() {
    if (!state.flight) return;
    setGateSelection(state.flight.charter_gate);
    if (!els.gateDialog.open) els.gateDialog.showModal();
  }
  function unlockScanAudio() {
    try {
      const Audio = window.AudioContext || window.webkitAudioContext;
      if (!Audio) return null;
      scanAudioContext ||= new Audio();
      if (scanAudioContext.state === "suspended") scanAudioContext.resume();
      return scanAudioContext;
    } catch (_) { return null; }
  }
  function scanFeedback(success, message) {
    const overlay = document.createElement("div"); overlay.className = `scan-feedback ${success ? "success" : "error"}`; overlay.textContent = message || (success ? "PASSENGER BOARDED" : "SCAN NOT ACCEPTED"); document.body.appendChild(overlay);
    try { const audio = unlockScanAudio(); if (audio) { const oscillator = audio.createOscillator(), gain = audio.createGain(); oscillator.connect(gain); gain.connect(audio.destination); oscillator.frequency.value = success ? 880 : 180; gain.gain.setValueAtTime(.12, audio.currentTime); oscillator.start(); oscillator.stop(audio.currentTime + (success ? .18 : .45)); } } catch (_) {}
    setTimeout(() => overlay.remove(), 1800);
  }
  async function responseJson(response, action) {
    const body = await response.text();
    try { return JSON.parse(body); }
    catch (_) {
      const detail = body.replace(/<[^>]*>/g, " ").replace(/\s+/g, " ").trim().slice(0, 220);
      throw Error(`${action} failed (${response.status}). ${detail || "The server returned an unexpected response."}`);
    }
  }
  function fmtTime(value) { return value ? new Intl.DateTimeFormat("en-NZ", { hour:"2-digit", minute:"2-digit" }).format(new Date(value)) : "Time unavailable"; }
  function renderFlights() {
    els.flightCount.textContent = state.flights.length;
    els.flightList.innerHTML = state.flights.length ? state.flights.map((f) => `<button class="flight-card ${state.flight?.envision_flight_id === f.envision_flight_id ? "active" : ""}" data-id="${esc(f.envision_flight_id)}"><strong>${esc(f.flight_number || f.flight || "Charter")}</strong><span>${esc(f.dep || "")}&nbsp;–&nbsp;${esc(f.ades || f.dest || "")} · ${esc(fmtTime(f.std_nz))}</span><span>${esc(f.reg || "Aircraft TBC")}</span></button>`).join("") : '<p class="empty">No charter flights were returned for this date.</p>';
  }
  function renderSummary() {
    const statuses = { "Booked":0, "Checked In":0, "Boarded":0, "Flown":0 };
    let bagKg = 0;
    state.passengers.forEach((p) => { statuses[statusOf(p)]++; bagKg += Number(p.BaggageWeight || 0); });
    els.summary.innerHTML = [["Passengers",state.passengers.length],["Booked",statuses.Booked],["Checked in",statuses["Checked In"]],["Boarded",statuses.Boarded + statuses.Flown],["Baggage",`${bagKg.toFixed(1)} kg`]].map(([label,value]) => `<div class="metric"><strong>${esc(value)}</strong><span>${label}</span></div>`).join("");
  }
  function seatRowsForFlight(f) {
    const type = String(f?.aircraft_type || "").toUpperCase(), reg = String(f?.reg || "").replace(/-/g, "").toUpperCase();
    if (type.includes("ATR") || reg.startsWith("ZKMC")) return Array.from({ length:17 }, (_, i) => [i + 1, ["A", "B"], ["C", "D"]]);
    if (type.includes("SAAB") || type.includes("SF3") || type.includes("SF340") || reg.startsWith("ZKCI") || reg.startsWith("ZKKR")) {
      if (reg === "ZKCIT") return [[0, [], ["C"]], ...Array.from({ length:10 }, (_, i) => [i + 1, ["A"], ["B", "C"]]), [11, ["A"], ["C", "D"]]];
      if (reg === "ZKCIZ") return [...Array.from({ length:10 }, (_, i) => [i + 1, ["A"], ["B", "C"]]), [11, ["A", "B"], ["C", "D"]]];
      return Array.from({ length:11 }, (_, i) => [i + 1, ["A"], ["B", "C"]]);
    }
    return [];
  }
  function boardingAircraftLabel(f) {
    const aircraftType = String(f?.aircraft_type || "").toUpperCase();
    const registration = String(f?.reg || "").replace(/-/g, "").toUpperCase();
    if (aircraftType.includes("ATR") || registration.startsWith("ZKMC")) return "ATR 72";
    if (aircraftType.includes("SAAB") || aircraftType.includes("SF3") || aircraftType.includes("SF340") || registration.startsWith("ZKCI") || registration.startsWith("ZKKR")) return "Saab 340";
    return String(f?.aircraft_type || "Aircraft TBC");
  }
  function availableSeatFor(p) {
    if (String(p.PassengerType || "").toUpperCase() === "INF") return "";
    const occupied = new Set(state.passengers.filter((x) => x.PassengerId !== p.PassengerId).map((x) => String(x.Seat || "").trim().toUpperCase()).filter(Boolean));
    const seats = seatRowsForFlight(state.flight).flatMap(([row, left, right]) => [...left, ...right].map((letter) => `${row}${letter}`));
    const standardSeats = seats.filter((seat) => !["1A", "0C"].includes(seat));
    return [...standardSeats, ...seats.filter((seat) => ["1A", "0C"].includes(seat))].find((seat) => !occupied.has(seat)) || "";
  }
  function openSeatmap(p) {
    const rows = seatRowsForFlight(state.flight);
    if (!rows.length) { showNotice("Seatmap is not available for this aircraft type. Enter the seat manually.", true); return; }
    const occupied = new Map(state.passengers.filter((x) => x.PassengerId !== p.PassengerId && x.Seat).map((x) => [String(x.Seat).trim().toUpperCase(), nameOf(x)]));
    els.seatmapTitle.textContent = `Choose a seat · ${nameOf(p)}`;
    els.seatmapHelp.textContent = "Select an available seat. Occupied seats are disabled.";
    const block = (row, letters, side) => `<div class="seatmap-block ${side}">${letters.map((letter) => { const seat = `${row}${letter}`, name = occupied.get(seat), current = String(p.Seat || "").toUpperCase() === seat, reserve = ["1A", "0C"].includes(seat); return `<button class="seatmap-seat ${current ? "current" : ""} ${name ? "occupied" : ""} ${reserve ? "reserve" : ""}" type="button" data-seat="${seat}" ${name ? `disabled title="${esc(name)}"` : reserve ? 'title="Reserve seat — allocated automatically last"' : ""}>${seat}</button>`; }).join("")}</div>`;
    els.seatmapGrid.innerHTML = rows.map(([row, left, right]) => `<div class="seatmap-row"><span class="seatmap-row-number">${row}</span>${block(row, left, "left")}<span></span>${block(row, right, "right")}</div>`).join("");
    els.seatmapDialog.showModal();
    els.seatmapGrid.onclick = async (event) => { const btn = event.target.closest("[data-seat]"); if (!btn) return; try { await updatePassenger(p.PassengerId, { Seat: btn.dataset.seat }); els.seatmapDialog.close(); showNotice(`Seat ${btn.dataset.seat} assigned to ${nameOf(p)}.`); } catch (err) { showNotice(err.message, true); } };
  }
  function renderPassengers() {
    const term = els.search.value.trim().toLowerCase(), selectedStatus = els.filter.value;
    const shown = state.passengers.filter((p) => { const status = statusOf(p); return (!selectedStatus || status === selectedStatus) && (!term || `${nameOf(p)} ${p.Seat || ""} ${p.BookingReferenceID || ""} ${p.SSR || ""} ${JSON.stringify(p.Ssrs || "")}`.toLowerCase().includes(term)); });
    els.rows.innerHTML = shown.length ? shown.map((p) => {
      const id = esc(p.PassengerId), status = statusOf(p), cls = status.toLowerCase().replace(" ", "-");
      const checkin = status === "Booked" ? `<button class="button" data-action="checkin" data-id="${id}">Check in</button>` : "";
      const board = status === "Checked In" ? `<button class="button" data-action="board" data-id="${id}">Board</button>` : "";
      const reverse = status !== "Booked" ? `<button class="button secondary" data-action="${status === "Boarded" ? "unboard" : "booked"}" data-id="${id}">${status === "Boarded" ? "Unboard" : "Reverse"}</button>` : "";
      const seatmap = `<button class="button secondary" data-action="seatmap" data-id="${id}">Seat map</button>`;
      return `<tr data-id="${id}"><td class="pax-name"><strong>${esc(nameOf(p))}</strong><span>${esc(p.BookingReferenceID || "No booking reference")}</span></td><td>${esc(p.PassengerType || "AD")}</td><td><input class="seat-input" data-field="Seat" value="${esc(p.Seat || "")}" maxlength="5" aria-label="Seat for ${esc(nameOf(p))}"></td><td><input type="number" min="0" step="0.1" data-field="BaggageWeight" value="${Number(p.BaggageWeight || 0)}" aria-label="Baggage kilograms for ${esc(nameOf(p))}"></td><td><input type="number" min="0" step="1" data-field="BaggagePieces" value="${Number(p.BaggagePieces || 0)}" aria-label="Baggage pieces for ${esc(nameOf(p))}"></td><td>${esc((p.Ssrs || []).map((s) => `${s.Code}${s.FreeText ? ` (${s.FreeText})` : ""}`).join(", ") || p.SSR || "—")}</td><td><span class="status ${cls}">${esc(status)}</span></td><td><div class="row-actions"><button class="button secondary" data-action="save" data-id="${id}">Save</button>${seatmap}${checkin}${board}${reverse}<button class="button secondary" data-action="print" data-id="${id}">Print pass</button></div></td></tr>`;
    }).join("") : '<tr><td class="empty" colspan="8">No passengers match the current filters.</td></tr>';
  }
  function renderPassengerActions() {
    els.rows.querySelectorAll('input[data-field]').forEach((field) => {
      const value = field.value.trim();
      const display = document.createElement("span");
      display.className = "readonly-passenger-field";
      display.textContent = value || "—";
      field.replaceWith(display);
    });
    els.rows.querySelectorAll('[data-action="save"], [data-action="seatmap"]').forEach((button) => button.remove());
    els.rows.querySelectorAll("tr[data-id]").forEach((row) => {
      const actions = row.querySelector(".row-actions");
      if (!actions) return;
      actions.querySelector('[data-action="checkin"]')?.remove();
      const edit = document.createElement("button");
      edit.className = "button secondary";
      edit.type = "button";
      edit.dataset.action = "checkin";
      edit.dataset.id = row.dataset.id;
      const passenger = state.passengers.find((item) => String(item.PassengerId) === String(row.dataset.id));
      edit.textContent = statusOf(passenger || {}) === "Booked" ? "Check in" : "Edit";
      actions.prepend(edit);
    });
  }
  function renderFlight() {
    const f = state.flight; els.empty.hidden = !!f; els.content.hidden = !f; if (!f) return;
    const closed = Boolean(f.charter_flight_closed_at);
    els.title.textContent = `${f.flight_number || f.flight || "Charter"} · ${f.dep || ""}–${f.ades || f.dest || ""}`;
    els.meta.textContent = `${fmtTime(f.std_nz)} · ${f.reg || "Aircraft TBC"}`;
    els.template.href = app.dataset.templateUrl; els.manifestMeta.textContent = closed ? `Flight closed ${new Date(f.charter_flight_closed_at).toLocaleString("en-NZ")}` : (state.passengers.length ? `${state.passengers.length} passenger records` : "No passenger list uploaded");
    els.closeFlight.hidden = closed; els.reopenFlight.hidden = !closed; els.file.disabled = closed; els.addPassenger.disabled = closed; els.scanButton.disabled = closed; setGateSelection(f.charter_gate); els.gate.disabled = closed; els.saveGate.disabled = closed; els.openGate.disabled = closed; els.openGate.textContent = f.charter_gate ? `Gate ${f.charter_gate}` : "Assign gate";
    renderSummary(); renderPassengers(); renderPassengerActions(); renderFlights();
  }
  async function selectFlight(flight) {
    state.flight = flight; state.passengers = []; renderFlight(); showNotice("Loading passenger list…");
    try { const u = new URL(app.dataset.manifestUrl, location.href); u.searchParams.set("flight_id", flight.envision_flight_id); const r = await fetch(u); const d = await r.json(); if (!r.ok || d.ok === false) throw Error(d.error || "Unable to load manifest"); state.passengers = d.passengers || []; els.manifestMeta.textContent = d.uploaded_filename ? `${d.uploaded_filename} · ${state.passengers.length} passengers` : `${state.passengers.length} passenger records`; showNotice(""); renderFlight(); } catch (e) { showNotice(e.message, true); }
  }
  async function refreshOpenFlight() {
    if (!state.flight || document.hidden || manifestRefreshInFlight || els.checkinDialog.open || els.gateDialog.open) return;
    manifestRefreshInFlight = true;
    try {
      const url = new URL(app.dataset.manifestUrl, location.href);
      url.searchParams.set("flight_id", state.flight.envision_flight_id);
      const response = await fetch(url, { cache:"no-store" });
      const data = await responseJson(response, "Live passenger refresh");
      if (!response.ok || data.ok === false) return;
      const nextPassengers = data.passengers || [];
      const changed = JSON.stringify(nextPassengers) !== JSON.stringify(state.passengers)
        || (data.closed_at || null) !== (state.flight.charter_flight_closed_at || null)
        || (data.gate || "") !== (state.flight.charter_gate || "");
      state.passengers = nextPassengers;
      state.flight.charter_flight_closed_at = data.closed_at || null;
      state.flight.charter_gate = data.gate || "";
      if (changed) {
        renderFlight();
        showNotice("Passenger list updated from another check-in station.");
      }
    } catch (_) {
      // Background refresh is intentionally quiet; normal user actions show errors.
    } finally {
      manifestRefreshInFlight = false;
    }
  }
  async function loadFlights() {
    els.refresh.disabled = true; showNotice("Loading charter flights…");
    try { const u = new URL(app.dataset.ganttUrl, location.href); u.searchParams.set("date", els.day.value); const r = await fetch(u); const d = await r.json(); if (!r.ok || d.ok === false) throw Error(d.error || "Unable to load flights"); state.flights = (d.results || []).filter(isCharter); if (state.flight) state.flight = state.flights.find((f) => String(f.envision_flight_id) === String(state.flight.envision_flight_id)) || null; showNotice(""); renderFlights(); renderFlight(); } catch (e) { showNotice(e.message, true); } finally { els.refresh.disabled = false; }
  }
  async function updatePassenger(id, changes) {
    const r = await fetch(app.dataset.passengerUrl, { method:"PATCH", headers:{ "Content-Type":"application/json" }, body:JSON.stringify({ flight_id:state.flight.envision_flight_id, passenger_id:id, ...changes }) }); const d = await r.json(); if (!r.ok || d.ok === false) throw Error(d.error || "Unable to save passenger"); const i = state.passengers.findIndex((p) => p.PassengerId === id); if (i >= 0) state.passengers[i] = d.passenger; renderFlight(); return d.passenger;
  }
  function boardingQrUrl(p) { const url = new URL(app.dataset.boardingQrUrl, location.href); url.searchParams.set("flight_id", state.flight.envision_flight_id); url.searchParams.set("passenger_id", p.PassengerId); return url.href; }
  async function changeFlightClosure(closing) {
    if (!state.flight) return;
    const action = closing ? "close" : "reopen";
    const warning = closing ? "Close this flight? Check-in changes will be locked and the current passenger manifest will be emailed to Flight Operations." : "Reopen this flight for check-in?";
    if (!window.confirm(warning)) return;
    const response = await fetch(closing ? app.dataset.closeFlightUrl : app.dataset.reopenFlightUrl, { method:"POST", headers:{ "Content-Type":"application/json" }, body:JSON.stringify({ flight_id:state.flight.envision_flight_id }) });
    const data = await responseJson(response, `Flight ${action}`);
    if (!response.ok || data.ok === false) throw Error(data.error || `Unable to ${action} flight`);
    state.flight.charter_flight_closed_at = data.closed_at || null;
    renderFlight(); renderFlights(); showNotice(data.message || `Flight ${closing ? "closed" : "reopened"}.`);
  }
  async function saveGate() {
    if (!state.flight) return;
    const response = await fetch(app.dataset.gateUrl, { method:"PATCH", headers:{ "Content-Type":"application/json" }, body:JSON.stringify({ flight_id:state.flight.envision_flight_id, gate:els.gate.value.trim().toUpperCase() }) });
    const data = await responseJson(response, "Save gate");
    if (!response.ok || data.ok === false) throw Error(data.error || "Unable to save gate");
    state.flight.charter_gate = data.gate || "";
    els.gateDialog.close(); renderFlight(); renderFlights(); showNotice(data.gate ? `Gate ${data.gate} assigned.` : "Gate cleared.");
  }
  async function ensureGateBeforePrinting() {
    const flightId = String(state.flight?.envision_flight_id || "");
    if (!flightId || state.flight.charter_gate || gateWarningShown.has(flightId)) return;
    gateWarningShown.add(flightId);
    if (!window.confirm("No gate has been assigned to this flight. Would you like to assign one before printing the boarding pass?")) return;
    openGateDialog();
    throw Error("Choose and save a gate, then complete check-in.");
  }
  async function boardScannedCode(code, acknowledgeSeatChange = false) {
    const response = await fetch(app.dataset.boardScanUrl, { method:"POST", headers:{ "Content-Type":"application/json" }, body:JSON.stringify({ code, acknowledge_seat_change:acknowledgeSeatChange }) });
    const data = await responseJson(response, "Boarding scan");
    if (data.seat_changed) return data;
    if (!response.ok || data.ok === false) throw Error(data.error || "Unable to board passenger");
    if (state.flight && String(state.flight.envision_flight_id) === String(data.flight_id)) { const index = state.passengers.findIndex((p) => p.PassengerId === data.passenger.PassengerId); if (index >= 0) state.passengers[index] = data.passenger; renderFlight(); }
    return data;
  }
  function stopScanner() { if (scanTimer) clearInterval(scanTimer); scanTimer = null; if (scanStream) scanStream.getTracks().forEach((track) => track.stop()); scanStream = null; els.scanVideo.srcObject = null; }
  async function ensureScannerOpen() {
    if (!els.scanDialog.open || !scanStream?.active) await openScanner();
  }
  async function processScanCode(acknowledgeSeatChange = false) { const code = acknowledgeSeatChange ? pendingSeatChangeCode : els.scanCode.value.trim(); if (!code || scanProcessing) return; scanProcessing = true; els.scanProcess.disabled = true; try { const data = await boardScannedCode(code, acknowledgeSeatChange); if (data.seat_changed) { pendingSeatChangeCode = code; els.seatChangeMessage.textContent = `${nameOf(data.passenger || {})}: boarding pass shows seat ${data.printed_seat || "unassigned"}; current seat is ${data.current_seat || "unassigned"}. Tell the passenger their seat has changed, then acknowledge.`; els.seatChangeAlert.hidden = false; els.scanStatus.textContent = "Seat change requires acknowledgement before boarding."; return; } pendingSeatChangeCode = ""; els.seatChangeAlert.hidden = true; els.scanCode.value = ""; const passengerName = nameOf(data.passenger || {}); els.scanLastPassenger.textContent = `Last boarded: ${passengerName}`; scanFeedback(true, data.message || "PASSENGER BOARDED"); els.scanStatus.textContent = `${data.message || "Passenger boarded."} Ready for the next boarding pass.`; showNotice(data.message || "Passenger boarded."); await ensureScannerOpen(); } catch (err) { els.scanCode.value = ""; scanFeedback(false, "SCAN NOT ACCEPTED"); els.scanStatus.textContent = err.message; showNotice(err.message, true); } finally { scanProcessing = false; els.scanProcess.disabled = false; } }
  async function openScanner() {
    unlockScanAudio();
    els.scanCode.value = ""; pendingSeatChangeCode = ""; els.seatChangeAlert.hidden = true; els.scanLastPassenger.textContent = "Last boarded: —"; els.scanStatus.textContent = "Allow camera access, then point it at the boarding-pass QR code."; if (!els.scanDialog.open) els.scanDialog.showModal();
    if (!navigator.mediaDevices?.getUserMedia) { els.scanStatus.textContent = "Camera access is not available in this browser."; return; }
    if (!("BarcodeDetector" in window) && typeof window.jsQR !== "function") { els.scanStatus.textContent = "QR decoding is unavailable in this browser."; return; }
    try {
      if (scanStream?.active) return;
      scanStream = await navigator.mediaDevices.getUserMedia({ video:{ facingMode:{ ideal:"environment" } } });
      els.scanVideo.srcObject = scanStream;
      await els.scanVideo.play();
      const detector = "BarcodeDetector" in window ? new BarcodeDetector({ formats:["qr_code"] }) : null;
      const canvas = detector ? null : document.createElement("canvas");
      const context = canvas?.getContext("2d", { willReadFrequently:true });
      els.scanStatus.textContent = detector ? "Point the camera at the boarding-pass QR code." : "Safari camera scanner ready. Point the camera at the boarding-pass QR code.";
      scanTimer = setInterval(async () => {
        try {
          let code = "";
          if (detector) {
            code = (await detector.detect(els.scanVideo))[0]?.rawValue || "";
          } else if (canvas && context && els.scanVideo.videoWidth) {
            canvas.width = els.scanVideo.videoWidth; canvas.height = els.scanVideo.videoHeight;
            context.drawImage(els.scanVideo, 0, 0, canvas.width, canvas.height);
            code = window.jsQR(context.getImageData(0, 0, canvas.width, canvas.height).data, canvas.width, canvas.height, { inversionAttempts:"dontInvert" })?.data || "";
          }
          if (code) { els.scanCode.value = code; await processScanCode(); }
        } catch (_) {}
      }, 500);
    } catch (err) { els.scanStatus.textContent = `Camera unavailable: ${err.message}.`; }
  }
  function ssrTextValue() { return checkinSsrs.map((ssr) => `${ssr.Code}${ssr.FreeText ? ` (${ssr.FreeText})` : ""}`).join(", "); }
  function renderCheckinSsrs() { els.checkinSsrList.innerHTML = checkinSsrs.length ? checkinSsrs.map((ssr, index) => `<span class="ssr-chip"><strong>${esc(ssr.Code)}</strong>${ssr.FreeText ? ` ${esc(ssr.FreeText)}` : ""}<button type="button" data-ssr-index="${index}" aria-label="Remove ${esc(ssr.Code)}">×</button></span>`).join("") : '<span class="muted">No SSRs recorded.</span>'; }
  function renderCheckinPassengerWeight() { const type = els.checkinPassengerType.value; els.checkinPassengerWeight.textContent = `Passenger weight: ${passengerWeightForType(type)} kg${type === "T" ? " (T adult weight)" : ""}`; }
  function renderCheckinSeatmap() {
    const selected = els.checkinSeat.value.trim().toUpperCase();
    const currentId = checkinPassenger?.PassengerId;
    const occupied = new Map(state.passengers.filter((p) => p.PassengerId !== currentId && p.Seat).map((p) => [String(p.Seat).trim().toUpperCase(), nameOf(p)]));
    const block = (row, letters, side) => `<div class="seatmap-block ${side}">${letters.map((letter) => { const seat = `${row}${letter}`, usedBy = occupied.get(seat), reserve = ["1A", "0C"].includes(seat); return `<button class="seatmap-seat ${seat === selected ? "selected" : ""} ${reserve ? "reserve" : ""}" type="button" data-checkin-seat="${seat}" ${usedBy ? `disabled title="${esc(usedBy)}"` : reserve ? 'title="Reserve seat — allocated automatically last"' : ""}>${seat}</button>`; }).join("")}</div>`;
    els.checkinSeatmapGrid.innerHTML = seatRowsForFlight(state.flight).map(([row, left, right]) => `<div class="seatmap-row"><span class="seatmap-row-number">${row}</span>${block(row, left, "left")}<span></span>${block(row, right, "right")}</div>`).join("") || '<span class="muted">Seatmap unavailable for this aircraft.</span>';
  }
  function openCheckin(p = null) {
    checkinPassenger = p; const passenger = p || { PassengerType:"AD" }; checkinSsrs = Array.isArray(passenger.Ssrs) ? passenger.Ssrs.map((ssr) => ({ Code:String(ssr.Code || "OTHS").toUpperCase(), FreeText:String(ssr.FreeText || "") })) : [];
    els.checkinIdentity.hidden = !!p; els.checkinTitle.textContent = p ? `Check in ${nameOf(p)}` : "Add and check in passenger"; els.checkinMeta.textContent = p ? `${p.PassengerType || "AD"} · ${p.BookingReferenceID || "No booking reference"}` : "Add the passenger details before completing check-in.";
    els.checkinIdentity.hidden = false; els.checkinNamePrefix.value = passenger.NamePrefix || ""; els.checkinGivenName.value = passenger.GivenName || ""; els.checkinSurname.value = passenger.Surname || ""; els.checkinPassengerType.value = passenger.PassengerType || "AD"; els.checkinPnr.value = passenger.BookingReferenceID || ""; renderCheckinPassengerWeight();
    els.checkinSeat.value = String(passenger.Seat || availableSeatFor(passenger) || "").toUpperCase(); els.checkinBagKg.value = Number(passenger.BaggageWeight || 0); els.checkinBagPieces.value = Number(passenger.BaggagePieces || 0); els.checkinComments.value = passenger.Comments || "";
    els.checkinSsrCode.innerHTML = ssrOptions.map(([value, label]) => `<option value="${value}">${esc(label)}</option>`).join(""); els.checkinSsrText.value = ""; renderCheckinSsrs(); renderCheckinSeatmap(); els.checkinDialog.showModal();
  }
  async function completeCheckin() {
    const originalStatus = checkinPassenger ? statusOf(checkinPassenger) : "Booked";
    const originalSeat = String(checkinPassenger?.Seat || "").trim().toUpperCase();
    const isNewCheckin = originalStatus === "Booked";
    if (isNewCheckin) await ensureGateBeforePrinting();
    const seat = els.checkinSeat.value.trim().toUpperCase();
    const passengerType = els.checkinPassengerType.value;
    const values = { Status:isNewCheckin ? "Checked In" : originalStatus, NamePrefix:els.checkinNamePrefix.value, GivenName:els.checkinGivenName.value.trim(), Surname:els.checkinSurname.value.trim(), PassengerType:passengerType, PassengerWeight:passengerWeightForType(passengerType), Seat:seat, BaggageWeight:Number(els.checkinBagKg.value || 0), BaggagePieces:Number(els.checkinBagPieces.value || 0), SSR:ssrTextValue(), Comments:els.checkinComments.value.trim() };
    let passenger;
    if (checkinPassenger) passenger = await updatePassenger(checkinPassenger.PassengerId, values);
    else { const response = await fetch(app.dataset.passengerAddUrl, { method:"POST", headers:{ "Content-Type":"application/json" }, body:JSON.stringify({ flight_id:state.flight.envision_flight_id, GivenName:els.checkinGivenName.value.trim(), Surname:els.checkinSurname.value.trim(), PassengerType:els.checkinPassengerType.value, BookingReferenceID:els.checkinPnr.value.trim(), ...values }) }); const data = await responseJson(response, "Add passenger"); if (!response.ok || data.ok === false) throw Error(data.error || "Unable to add passenger"); passenger = data.passenger; state.passengers.push(passenger); renderFlight(); }
    els.checkinDialog.close();
    if (isNewCheckin) {
      showNotice(`${nameOf(passenger)} checked in${seat ? ` in seat ${seat}` : ""}.`);
      printPass(passenger);
    } else if (originalSeat !== seat) {
      showNotice(`${nameOf(passenger)} moved from ${originalSeat || "an unassigned seat"} to ${seat || "an unassigned seat"}. Collect the existing boarding pass.`);
      if (window.confirm(`Seat changed from ${originalSeat || "unassigned"} to ${seat || "unassigned"}. Collect the existing boarding pass. Print a replacement now?`)) printPass(passenger);
    } else {
      showNotice(`${nameOf(passenger)} updated.`);
    }
  }
  function printPass(p) {
    const f = state.flight, w = window.open("", "_blank", "width=900,height=450");
    if (!w) { showNotice("Allow pop-ups to print a boarding pass.", true); return; }
    const travelDate = f.std_nz ? new Intl.DateTimeFormat("en-NZ", { weekday:"short", day:"2-digit", month:"short", year:"numeric" }).format(new Date(f.std_nz)) : "Date TBC";
    const route = `${esc(f.dep)} - ${esc(f.ades || f.dest)}`, passenger = esc(nameOf(p)), flight = esc(f.flight_number || f.flight), seat = esc(p.Seat || "GATE"), gate = esc(f.charter_gate || "AS DIRECTED"), aircraft = esc(boardingAircraftLabel(f)), qr = esc(boardingQrUrl(p));
    w.document.write(`<!doctype html><html><head><title>Boarding Pass</title><style>@page{size:200mm 80mm;margin:0}*{box-sizing:border-box}html,body{width:200mm;height:80mm;margin:0;background:#fff;font-family:Arial,sans-serif;color:#111}.pass{display:grid;grid-template-columns:150mm 50mm;width:200mm;height:80mm;overflow:hidden;border:1px solid #111}.main{padding:6mm 7mm}.brand{font-size:9pt;font-weight:700;letter-spacing:1.4px;color:#075985}.title{font-size:7pt;letter-spacing:1px;margin-top:1mm;color:#555}.route{font-size:28pt;font-weight:800;letter-spacing:1px;line-height:1;margin:4mm 0}.grid{display:grid;grid-template-columns:1.6fr .75fr .8fr;gap:4mm}.label{font-size:6.5pt;font-weight:700;letter-spacing:.7px;color:#555}.value{font-size:12pt;font-weight:700;margin-top:1mm}.seat .value{font-size:25pt;line-height:.85}.footer{margin-top:4mm;padding-top:3mm;border-top:1px solid #222;font-size:7pt}.stub{padding:6mm 4mm;border-left:1px dashed #111;text-align:center}.stub .route{font-size:16pt;margin:3mm 0}.stub .seat{margin:4mm 0}.qr{width:32mm;height:32mm;display:block;margin:2mm auto 1mm}.scan{font-size:6.5pt;color:#444}@media screen{body{padding:15px;background:#e5e7eb}.pass{box-shadow:0 3px 15px #0003;margin:auto}}</style></head><body><section class="pass"><div class="main"><div class="brand">AIR CHATHAMS</div><div class="title">CHARTER BOARDING PASS</div><div class="route">${route}</div><div class="grid"><div><div class="label">PASSENGER</div><div class="value">${passenger}</div></div><div><div class="label">FLIGHT</div><div class="value">${flight}</div></div><div class="seat"><div class="label">SEAT</div><div class="value">${seat}</div></div><div><div class="label">DATE</div><div class="value">${esc(travelDate)}</div></div><div><div class="label">DEPARTURE</div><div class="value">${esc(fmtTime(f.std_nz))}</div></div><div><div class="label">BOOKING REF</div><div class="value">${esc(p.BookingReferenceID || "-")}</div></div></div><div class="footer">Boarding pass valid for this charter sector only. Present QR code at the gate.</div></div><aside class="stub"><div class="brand">AIR CHATHAMS</div><div class="route">${route}</div><div class="label">FLIGHT</div><div class="value">${flight}</div><div class="seat"><div class="label">SEAT</div><div class="value">${seat}</div></div><img class="qr" src="${qr}" alt="Boarding QR code"><div class="scan">SCAN AT BOARDING</div></aside></section><script>window.onload=()=>window.print()<\/script></body></html>`);
    const printStyle = w.document.createElement("style");
    printStyle.textContent = ".stub{padding:4mm 3mm}.stub .brand{font-size:7pt}.stub .route{font-size:12pt;margin:2mm 0}.stub .value{font-size:10pt}.stub .seat{margin:2mm 0}.stub .seat .value{font-size:18pt;line-height:.9}.stub .qr{width:26mm;height:26mm;margin:1.5mm auto 1mm}.stub .scan{font-size:6pt}";
    w.document.head.append(printStyle);
    w.document.querySelector(".grid")?.insertAdjacentHTML("beforeend", `<div><div class="label">GATE · AIRCRAFT</div><div class="value">${gate} · ${aircraft}</div></div>`);
    w.document.close();
  }
  els.flightList.addEventListener("click", (e) => { const btn = e.target.closest("[data-id]"); if (btn) selectFlight(state.flights.find((f) => String(f.envision_flight_id) === btn.dataset.id)); });
  els.rows.addEventListener("click", async (e) => { const btn = e.target.closest("button[data-action]"); if (!btn) return; const p = state.passengers.find((x) => x.PassengerId === btn.dataset.id); if (!p) return; try { if (btn.dataset.action === "print") return printPass(p); if (btn.dataset.action === "seatmap") return openSeatmap(p); if (btn.dataset.action === "checkin") return openCheckin(p); if (btn.dataset.action === "save") { const tr = btn.closest("tr"); await updatePassenger(p.PassengerId, { Seat:tr.querySelector('[data-field="Seat"]').value.trim().toUpperCase(), BaggageWeight:Number(tr.querySelector('[data-field="BaggageWeight"]').value || 0), BaggagePieces:Number(tr.querySelector('[data-field="BaggagePieces"]').value || 0) }); showNotice("Passenger details saved."); } else { const status = btn.dataset.action === "board" ? "Boarded" : btn.dataset.action === "unboard" ? "Checked In" : "Booked"; await updatePassenger(p.PassengerId, { Status:status }); showNotice(`Passenger marked ${status}.`); } } catch (err) { showNotice(err.message, true); } });
  els.file.addEventListener("change", async () => { const file = els.file.files[0]; if (!file || !state.flight) return; const body = new FormData(); body.append("flight_id", state.flight.envision_flight_id); body.append("flight_number", state.flight.flight_number || state.flight.flight || ""); body.append("dep", state.flight.dep || ""); body.append("ades", state.flight.ades || state.flight.dest || ""); body.append("file", file); showNotice("Uploading passenger list…"); try { const r = await fetch(app.dataset.uploadUrl, { method:"POST", body }); const d = await responseJson(r, "Manifest upload"); if (!r.ok || d.ok === false) throw Error(d.error || "Upload failed"); state.passengers = d.passengers || []; showNotice(`${state.passengers.length} passengers uploaded as Booked.`); renderFlight(); } catch (e) { showNotice(e.message, true); } finally { els.file.value = ""; } });
  els.checkinSeatmapGrid.addEventListener("click", (event) => { const seat = event.target.closest("[data-checkin-seat]")?.dataset.checkinSeat; if (!seat) return; els.checkinSeat.value = seat; renderCheckinSeatmap(); }); els.checkinSeat.addEventListener("input", renderCheckinSeatmap); els.seatmapClose.addEventListener("click", () => els.seatmapDialog.close()); els.toggleFlights.addEventListener("click", () => { workspace.classList.toggle("hide-flights"); els.toggleFlights.textContent = workspace.classList.contains("hide-flights") ? "Show charter flights" : "Hide charter flights"; }); els.addPassenger.addEventListener("click", () => openCheckin()); els.scanButton.addEventListener("click", openScanner); els.scanClose.addEventListener("click", () => { stopScanner(); els.scanDialog.close(); }); els.scanProcess.addEventListener("click", processScanCode); els.scanCode.addEventListener("keydown", (event) => { if (event.key === "Enter") { event.preventDefault(); processScanCode(); } }); els.scanDialog.addEventListener("close", stopScanner); els.checkinClose.addEventListener("click", () => els.checkinDialog.close()); els.addCheckinSsr.addEventListener("click", () => { const code = els.checkinSsrCode.value; if (!code) return; checkinSsrs.push({ Code:code, FreeText:els.checkinSsrText.value.trim() }); els.checkinSsrCode.value = ""; els.checkinSsrText.value = ""; renderCheckinSsrs(); }); els.checkinSsrList.addEventListener("click", (event) => { const button = event.target.closest("[data-ssr-index]"); if (!button) return; checkinSsrs.splice(Number(button.dataset.ssrIndex), 1); renderCheckinSsrs(); }); els.confirmCheckin.addEventListener("click", () => completeCheckin(false).catch((err) => showNotice(err.message, true))); els.confirmCheckinPrint.addEventListener("click", () => completeCheckin(true).catch((err) => showNotice(err.message, true))); els.search.addEventListener("input", renderPassengers); els.filter.addEventListener("change", renderPassengers); els.refresh.addEventListener("click", loadFlights); els.day.addEventListener("change", () => { history.replaceState({}, "", `?date=${els.day.value}`); state.flight = null; state.passengers = []; loadFlights(); }); loadFlights();
  els.checkinPassengerType.addEventListener("change", renderCheckinPassengerWeight);
  els.acknowledgeSeatChange.addEventListener("click", () => processScanCode(true));
  els.closeFlight.addEventListener("click", () => changeFlightClosure(true).catch((err) => showNotice(err.message, true)));
  els.reopenFlight.addEventListener("click", () => changeFlightClosure(false).catch((err) => showNotice(err.message, true)));
  els.openGate.addEventListener("click", openGateDialog);
  els.gateDialogClose.addEventListener("click", () => els.gateDialog.close());
  els.clearGate.addEventListener("click", () => { els.gate.value = ""; saveGate().catch((err) => showNotice(err.message, true)); });
  els.saveGate.addEventListener("click", () => saveGate().catch((err) => showNotice(err.message, true)));
  els.search.addEventListener("input", renderPassengerActions);
  els.filter.addEventListener("change", renderPassengerActions);
  document.addEventListener("visibilitychange", () => { if (!document.hidden) refreshOpenFlight(); });
  window.setInterval(refreshOpenFlight, 5000);
})();
