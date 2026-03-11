(function () {
  const app = document.getElementById("new-gantt-app");
  if (!app) return;

  const apiUrl = app.dataset.apiUrl;
  const apgPushUrl = app.dataset.apgPushUrl;
  const apgResetUrl = app.dataset.apgResetUrl;
  const manifestPreviewUrl = app.dataset.manifestPreviewUrl;
  const envisionActionUrl = app.dataset.envisionActionUrl;
  const envisionCrewUrl = app.dataset.envisionCrewUrl;
  const envisionLineRegistrationsUrl = app.dataset.envisionLineRegistrationsUrl;
  const envisionFlightTypesUrl = app.dataset.envisionFlightTypesUrl;
  const envisionFlightNotesUrl = app.dataset.envisionFlightNotesUrl;
  const envisionFlightNotesUpsertUrl = app.dataset.envisionFlightNotesUpsertUrl;
  const envisionEnvironmentUrl = app.dataset.envisionEnvironmentUrl;
  const envisionFlightRawUrl = app.dataset.envisionFlightRawUrl;
  const envisionTimesUrl = app.dataset.envisionTimesUrl;
  const envisionRegDefectsUrl = app.dataset.envisionRegDefectsUrl;
  const envisionRegMaintenanceUrl = app.dataset.envisionRegMaintenanceUrl;
  const envisionPaxSyncUrl = app.dataset.envisionPaxSyncUrl;
  const saveTimesUrl = app.dataset.saveTimesUrl;
  const initialEnvisionEnvKey = (app.dataset.envisionEnvKey || "base").toLowerCase();
  const envisionTestAvailable = app.dataset.envisionTestAvailable === "1";

  const dayInput = document.getElementById("dayInput");
  const tzSelect = document.getElementById("tzSelect");
  const refreshBtn = document.getElementById("refreshBtn");
  const syncPaxBtn = document.getElementById("syncPaxBtn");
  const zoomInBtn = document.getElementById("zoomInBtn");
  const zoomOutBtn = document.getElementById("zoomOutBtn");
  const autoRefresh = document.getElementById("autoRefresh");
  const crosshairToggle = document.getElementById("crosshairToggle");
  const maintenanceToggle = document.getElementById("maintenanceToggle");
  const themeBtn = document.getElementById("themeBtn");
  const rowsEl = document.getElementById("rows");
  const axisEl = document.getElementById("axis");
  const boardEl = document.querySelector(".board");
  const detailList = document.getElementById("detailList");
  const detailMuted = document.querySelector("#detailCard .muted");
  const loadMessage = document.getElementById("loadMessage");
  const envisionEnvPill = document.getElementById("envisionEnvPill");
  const boardSpinner = document.getElementById("boardSpinner");
  const liveNowBar = document.getElementById("liveNowBar");
  const liveNowLabel = document.getElementById("liveNowLabel");
  const crosshairV = document.getElementById("crosshairV");
  const crosshairH = document.getElementById("crosshairH");

  const btnPreviewManifest = document.getElementById("btnPreviewManifest");
  const btnPaxList = document.getElementById("btnPaxList");
  const btnSubmitApg = document.getElementById("btnSubmitApg");
  const btnResetApg = document.getElementById("btnResetApg");
  const btnSeatmap = document.getElementById("btnSeatmap");
  const btnMovementMsg = document.getElementById("btnMovementMsg");

  const seatmapDialog = document.getElementById("seatmapDialog");
  const seatmapGrid = document.getElementById("seatmapGrid");
  const seatmapInfo = document.getElementById("seatmapInfo");
  const seatmapTitle = document.getElementById("seatmapTitle");
  const paxDialog = document.getElementById("paxDialog");
  const paxTitle = document.getElementById("paxTitle");
  const paxTbody = document.getElementById("paxTbody");
  const envPickerDialog = document.getElementById("envPickerDialog");
  const envBaseBtn = document.getElementById("envBaseBtn");
  const envTestBtn = document.getElementById("envTestBtn");
  const apgStatusDialog = document.getElementById("apgStatusDialog");
  const apgStatusTitle = document.getElementById("apgStatusTitle");
  const apgStatusBody = document.getElementById("apgStatusBody");
  const flightEditDialog = document.getElementById("flightEditDialog");
  const flightEditTitle = document.getElementById("flightEditTitle");
  const flightEditAction = document.getElementById("flightEditAction");
  const flightEditDelayId = document.getElementById("flightEditDelayId");
  const flightEditSubmit = document.getElementById("flightEditSubmit");
  const flightEditResult = document.getElementById("flightEditResult");
  const flightEditSections = document.querySelectorAll(".action-section");
  const movementDialog = document.getElementById("movementDialog");
  const movementTitle = document.getElementById("movementTitle");
  const movementSubmit = document.getElementById("movementSubmit");
  const movementResult = document.getElementById("movementResult");
  const defectsDialog = document.getElementById("defectsDialog");
  const defectsTitle = document.getElementById("defectsTitle");
  const defectsSummary = document.getElementById("defectsSummary");
  const defectsTbody = document.getElementById("defectsTbody");
  const mvEtd = document.getElementById("mvEtd");
  const mvEta = document.getElementById("mvEta");
  const mvDepDate = document.getElementById("mvDepDate");
  const mvArrDate = document.getElementById("mvArrDate");
  const mvOffblocks = document.getElementById("mvOffblocks");
  const mvLanding = document.getElementById("mvLanding");
  const mvAirborne = document.getElementById("mvAirborne");
  const mvOnchocks = document.getElementById("mvOnchocks");
  const mvModeDep = document.getElementById("mvModeDep");
  const mvModeArr = document.getElementById("mvModeArr");
  const mvDelaySection = document.getElementById("mvDelaySection");
  const mvAddDelay = document.getElementById("mvAddDelay");
  const mvDelayRows = document.getElementById("mvDelayRows");
  const mvDelayRequired = document.getElementById("mvDelayRequired");
  const mvDelayAllocated = document.getElementById("mvDelayAllocated");
  const mvDelayRemaining = document.getElementById("mvDelayRemaining");
  const modifyLegDialog = document.getElementById("modifyLegDialog");
  const modifyLegTitle = document.getElementById("modifyLegTitle");
  const mlApiResponses = document.getElementById("mlApiResponses");
  const mlPrev = document.getElementById("mlPrev");
  const mlNext = document.getElementById("mlNext");
  const mlOk = document.getElementById("mlOk");
  const mlReg = document.getElementById("mlReg");
  const mlAirline = document.getElementById("mlAirline");
  const mlFlightNo = document.getElementById("mlFlightNo");
  const mlDate = document.getElementById("mlDate");
  const mlFlightType = document.getElementById("mlFlightType");
  const mlSchedDate = document.getElementById("mlSchedDate");
  const mlDep = document.getElementById("mlDep");
  const mlStd = document.getElementById("mlStd");
  const mlArr = document.getElementById("mlArr");
  const mlSta = document.getElementById("mlSta");
  const mlOpDate = document.getElementById("mlOpDate");
  const mlOut = document.getElementById("mlOut");
  const mlOff = document.getElementById("mlOff");
  const mlEnroute = document.getElementById("mlEnroute");
  const mlOn = document.getElementById("mlOn");
  const mlIn = document.getElementById("mlIn");
  const mlCtot = document.getElementById("mlCtot");
  const mlActDate = document.getElementById("mlActDate");
  const mlActOut = document.getElementById("mlActOut");
  const mlActOff = document.getElementById("mlActOff");
  const mlActOn = document.getElementById("mlActOn");
  const mlActIn = document.getElementById("mlActIn");
  const mlBlock = document.getElementById("mlBlock");
  const mlEpax = document.getElementById("mlEpax");
  const mlApax = document.getElementById("mlApax");
  const mlCargo = document.getElementById("mlCargo");
  const mlRemark = document.getElementById("mlRemark");
  let lineRegistrationCache = null;
  let flightTypeCache = null;
  let registrationCatalogCache = null;
  let activeEnvisionEnv = initialEnvisionEnvKey;
  const ENV_STORAGE_KEY = "new_gantt_envision_env";
  const envisionLocalHmCache = new Map();
  let modifyLegFlight = null;
  let modifyLegNote = null;
  let modifyLegFlightTypeOriginalId = null;

  const feFlightStatusId = document.getElementById("feFlightStatusId");
  const fePlannedFlightTime = document.getElementById("fePlannedFlightTime");
  const feDepartureEstimate = document.getElementById("feDepartureEstimate");
  const feDepartureActual = document.getElementById("feDepartureActual");
  const feDepartureTakeOff = document.getElementById("feDepartureTakeOff");
  const feArrivalEstimate = document.getElementById("feArrivalEstimate");
  const feArrivalLanded = document.getElementById("feArrivalLanded");
  const feArrivalActual = document.getElementById("feArrivalActual");
  const feCalculatedTakeOffTime = document.getElementById("feCalculatedTakeOffTime");

  const feRegIgnoreValidations = document.getElementById("feRegIgnoreValidations");
  const feRegLineId = document.getElementById("feRegLineId");
  const feRegCrewPositions = document.getElementById("feRegCrewPositions");

  const feCancelLineId = document.getElementById("feCancelLineId");
  const feCancelCodeId = document.getElementById("feCancelCodeId");
  const feCancelRemarks = document.getElementById("feCancelRemarks");

  const feDivertPlaceId = document.getElementById("feDivertPlaceId");
  const feDivertArrivalEstimate = document.getElementById("feDivertArrivalEstimate");
  const feDivertRemarks = document.getElementById("feDivertRemarks");

  const feDelayRecordId = document.getElementById("feDelayRecordId");
  const feDelayIsArrival = document.getElementById("feDelayIsArrival");
  const feDelayCodeId = document.getElementById("feDelayCodeId");
  const feDelayCode = document.getElementById("feDelayCode");
  const feDelayCodeDescription = document.getElementById("feDelayCodeDescription");
  const feDelayMinutes = document.getElementById("feDelayMinutes");
  const feDelayRemarks = document.getElementById("feDelayRemarks");

  const statFlights = document.getElementById("statFlights");
  const statPax = document.getElementById("statPax");
  const statNoApg = document.getElementById("statNoApg");
  const statBags = document.getElementById("statBags");

  const minuteInDay = 24 * 60;
  // Wider default hour band so 30–60 min turnarounds remain readable.
  let pxPerMinute = 2.6;
  const airportLabelPadPx = 22;
  const minAirportLabelSpanPx = 74;
  let windowStartMin = 0;
  let windowEndMin = minuteInDay;
  let flights = [];
  let timer = null;
  let selectedId = null;
  let selectedFlight = null;
  let axisScrollEl = null;
  let hasUserZoom = false;
  let editingFlight = null;
  let editInitialByAction = {};
  const regDefectsCache = new Map();
  const regMaintenanceCache = new Map();
  let showMaintenance = true;
  let liveNowTimer = null;
  let movementFlight = null;
  let movementInitial = null;
  let movementMode = "dep";
  let currentTimeZone = "Pacific/Auckland";
  const DELAY_CODE_META = {
    "01": { id: 94, description: "Planned schedule deviation for regular flights" },
    "02": { id: 95, description: "Planned schedule deviation for charter flights" },
    "03": { id: 96, description: "Late bus" },
    "11": { id: 9, description: "Late check-in, acceptance after deadline" },
    "12": { id: 10, description: "Late check-in, congestion in check-in" },
    "13": { id: 98, description: "Check-in error passenger and/or baggage" },
    "14": { id: 99, description: "Oversales booking error" },
    "15": { id: 14, description: "Boarding discrepancies/paging/missing check-in passenger" },
    "16": { id: 16, description: "Commercial publicity/passenger convenience/VIP/Press/TV" },
    "17": { id: 17, description: "Catering order late/incorrect" },
    "18": { id: 18, description: "Baggage processing/sorting" },
    "21": { id: 19, description: "Cargo documentation errors" },
    "22": { id: 20, description: "Late positioning of cargo" },
    "23": { id: 21, description: "Late acceptance of cargo" },
    "24": { id: 22, description: "Inadequate cargo packing/quantity" },
    "25": { id: 23, description: "Oversells, cargo booking error" },
    "26": { id: 24, description: "Late cargo preparation in warehouse" },
    "27": { id: 25, description: "Mail documentation errors" },
    "28": { id: 26, description: "Late mail positioning" },
    "29": { id: 101, description: "Late mail acceptance" },
    "31": { id: 27, description: "Aircraft documentation late/inaccurate" },
    "32": { id: 28, description: "Loading/unloading staff or process delay" },
    "33": { id: 29, description: "Loading equipment lack/breakdown" },
    "34": { id: 30, description: "Servicing equipment/staff delay" },
    "35": { id: 31, description: "Aircraft cleaning" },
    "36": { id: 32, description: "Fueling/defueling" },
    "37": { id: 33, description: "Catering late delivery/loading" },
    "38": { id: 34, description: "ULD lack/serviceability" },
    "39": { id: 35, description: "Technical equipment lack/breakdown" },
    "41": { id: 36, description: "Aircraft defects" },
    "42": { id: 37, description: "Scheduled maintenance late release" },
    "43": { id: 38, description: "Non-scheduled maintenance/additional work" },
    "44": { id: 39, description: "Spares/maintenance equipment delay" },
    "45": { id: 40, description: "AOG spares to another station" },
    "46": { id: 41, description: "Aircraft change technical reasons" },
    "47": { id: 44, description: "No planned standby aircraft (technical)" },
    "48": { id: 102, description: "Scheduled cabin configuration/version adjustments" },
    "51": { id: 45, description: "Damage in flight ops" },
    "52": { id: 46, description: "Damage during ground ops" },
    "55": { id: 48, description: "Departure control" },
    "56": { id: 49, description: "Cargo preparation/documentation" },
    "57": { id: 50, description: "Flight plans" },
    "58": { id: 51, description: "Other automated systems" },
    "59": { id: 103, description: "Operational requirements fuel/load alteration" },
    "60": { id: 104, description: "Late crew boarding/departure procedures" },
    "61": { id: 52, description: "Flight plan/documentation late completion/change" },
    "62": { id: 53, description: "Operational requirements fuel/load alteration" },
    "63": { id: 56, description: "Late crew boarding/departure procedures" },
    "64": { id: 105, description: "Flight deck crew shortage/sickness/FTL" },
    "65": { id: 106, description: "Flight deck crew special request non-operational" },
    "66": { id: 107, description: "Late cabin crew boarding/departure" },
    "67": { id: 119, description: "Cabin crew shortage/sickness/FTL" },
    "71": { id: 62, description: "Departure station below limits" },
    "72": { id: 63, description: "Destination station below limits" },
    "73": { id: 64, description: "Alternate station below limits" },
    "74": { id: 65, description: "Enroute headwind/rerouting/weather avoidance" },
    "75": { id: 66, description: "De-icing/de-snowing aircraft" },
    "76": { id: 67, description: "Runway/taxiway snow/ice/water/sand removal" },
    "77": { id: 68, description: "Ground handling impacted by weather" },
    "81": { id: 7, description: "ATFM due to ATC en-route demand/capacity" },
    "82": { id: 108, description: "ATFM due to ATC staff/equipment en-route" },
    "83": { id: 109, description: "ATFM due to destination restrictions" },
    "84": { id: 110, description: "ATFM due to destination weather" },
    "85": { id: 88, description: "Departure airport restrictions" },
    "86": { id: 89, description: "Immigration/customs/health" },
    "87": { id: 90, description: "Airport facilities/stands/ramp congestion" },
    "88": { id: 91, description: "Destination restrictions with/without ATFM" },
    "89": { id: 111, description: "Departure restrictions ATS/startup/pushback" },
    "91": { id: 112, description: "Load connection awaiting another flight" },
    "92": { id: 113, description: "Through check-in error passenger/baggage" },
    "93": { id: 92, description: "Aircraft rotation late arrival previous sector" },
    "94": { id: 114, description: "Cabin crew rotation awaiting another flight" },
    "95": { id: 115, description: "Crew rotation awaiting another flight" },
    "96": { id: 116, description: "Ops control reroute/diversion/a/c change" },
    "97": { id: 117, description: "Industrial action own airline" },
    "98": { id: 118, description: "Industrial action outside own airline" },
    "99": { id: 93, description: "Other reason not matching above codes" },
  };

  function setBoardLoading(isLoading) {
    if (!boardSpinner) return;
    boardSpinner.hidden = !isLoading;
  }

  function showMessage(text, isError) {
    if (!loadMessage) return;
    if (!text) {
      loadMessage.hidden = true;
      loadMessage.textContent = "";
      loadMessage.classList.remove("error");
      return;
    }
    loadMessage.hidden = false;
    loadMessage.textContent = text;
    loadMessage.classList.toggle("error", !!isError);
  }

  function setActionsEnabled(enabled) {
    [btnPaxList, btnPreviewManifest, btnSubmitApg, btnResetApg, btnSeatmap, btnMovementMsg].forEach((b) => {
      if (b) b.disabled = !enabled;
    });
  }

  function withBusy(button, label, fn) {
    return async function () {
      if (!button) return;
      const original = button.textContent;
      button.disabled = true;
      button.textContent = label;
      try {
        await fn();
      } finally {
        button.disabled = false;
        button.textContent = original;
      }
    };
  }

  function getActiveTimeZone() {
    if (currentTimeZone === "__browser__") return Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC";
    return currentTimeZone || "Pacific/Auckland";
  }

  function getSelectedDayParts() {
    const v = dayInput && dayInput.value ? dayInput.value : app.dataset.day;
    const [y, m, d] = String(v).split("-").map((x) => Number(x));
    return { y, m, d };
  }

  function getRowLabelWidth() {
    const axisLabel = axisEl ? axisEl.querySelector(".axis-label") : null;
    if (axisLabel && axisLabel.offsetWidth) return axisLabel.offsetWidth;
    const rowLabel = rowsEl ? rowsEl.querySelector(".row-label") : null;
    if (rowLabel && rowLabel.offsetWidth) return rowLabel.offsetWidth;
    return 240;
  }

  function getZonedParts(isoString, tz) {
    if (!isoString) return null;
    const d = new Date(isoString);
    if (Number.isNaN(d.getTime())) return null;
    const dtf = new Intl.DateTimeFormat("en-CA", {
      timeZone: tz,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      hourCycle: "h23",
    });
    const p = dtf.formatToParts(d);
    const get = (t) => Number((p.find((x) => x.type === t) || {}).value || 0);
    return { y: get("year"), m: get("month"), d: get("day"), hh: get("hour"), mm: get("minute") };
  }

  function fmtTime(isoString) {
    if (!isoString) return "-";
    const d = new Date(isoString);
    if (Number.isNaN(d.getTime())) return "-";
    return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", hour12: false, timeZone: getActiveTimeZone() });
  }

  function getNowMinuteForSelectedDay() {
    const nowIso = new Date().toISOString();
    const tz = getActiveTimeZone();
    const now = getZonedParts(nowIso, tz);
    const day = getSelectedDayParts();
    if (!now || !day.y || !day.m || !day.d) return null;
    if (now.y !== day.y || now.m !== day.m || now.d !== day.d) return null;
    return now.hh * 60 + now.mm;
  }

  function hideCrosshairs() {
    if (crosshairV) crosshairV.hidden = true;
    if (crosshairH) crosshairH.hidden = true;
  }

  function updateLiveNowBar() {
    if (!liveNowBar || !liveNowLabel || !rowsEl || !boardEl) return;
    const minute = getNowMinuteForSelectedDay();
    const labelWidth = getRowLabelWidth();
    if (minute === null || minute < windowStartMin || minute > windowEndMin) {
      liveNowBar.hidden = true;
      liveNowLabel.hidden = true;
      return;
    }
    const x = labelWidth + minuteToPx(minute) - rowsEl.scrollLeft;
    liveNowBar.style.left = `${x}px`;
    liveNowBar.hidden = false;
    liveNowLabel.hidden = true;
  }

  function flightCode(f) {
    const des = String(f.designator || "").toUpperCase().trim();
    const raw = String(f.flight_number || "").toUpperCase().replace(/\s+/g, "");
    const digits = (raw.match(/(\d+)$/) || [null, raw])[1] || raw;
    return `${des}${digits}`;
  }

  function toLocalInputValue(iso) {
    if (!iso) return "";
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return "";
    const pad = (n) => String(n).padStart(2, "0");
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
  }

  function toIsoFromLocalInput(value) {
    if (!value) return null;
    const d = new Date(value);
    if (Number.isNaN(d.getTime())) return null;
    return d.toISOString();
  }

  function hmFromIso(iso) {
    if (!iso) return "";
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return "";
    const pad = (n) => String(n).padStart(2, "0");
    return `${pad(d.getHours())}:${pad(d.getMinutes())}`;
  }

  function ymdFromIso(iso) {
    if (!iso) return "";
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return "";
    const pad = (n) => String(n).padStart(2, "0");
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
  }

  function parseHmToMinutes(hm) {
    if (!hm || !/^\d{1,2}:\d{2}$/.test(hm)) return null;
    const [h, m] = hm.split(":").map(Number);
    if (Number.isNaN(h) || Number.isNaN(m)) return null;
    return h * 60 + m;
  }

  function movementState() {
    return {
      mode: movementMode,
      etd: mvEtd ? mvEtd.value : "",
      eta: mvEta ? mvEta.value : "",
      dep_date: mvDepDate ? mvDepDate.value : "",
      arr_date: mvArrDate ? mvArrDate.value : "",
      offblocks: mvOffblocks ? mvOffblocks.value : "",
      landing: mvLanding ? mvLanding.value : "",
      airborne: mvAirborne ? mvAirborne.value : "",
      onchocks: mvOnchocks ? mvOnchocks.value : "",
      delays: collectMovementDelays().rows,
    };
  }

  function setMovementMode(mode) {
    movementMode = mode === "arr" ? "arr" : "dep";
    if (mvModeDep) mvModeDep.classList.toggle("active", movementMode === "dep");
    if (mvModeArr) mvModeArr.classList.toggle("active", movementMode === "arr");

    const depOnly = [mvEtd, mvDepDate, mvOffblocks, mvAirborne];
    const arrOnly = [mvEta, mvArrDate, mvLanding, mvOnchocks];
    depOnly.forEach((el) => {
      if (!el) return;
      el.disabled = movementMode !== "dep";
      const wrap = el.closest(".ctrl");
      if (wrap) wrap.style.display = movementMode === "dep" ? "" : "none";
    });
    arrOnly.forEach((el) => {
      if (!el) return;
      el.disabled = movementMode !== "arr";
      const wrap = el.closest(".ctrl");
      if (wrap) wrap.style.display = movementMode === "arr" ? "" : "none";
    });
    if (mvDelaySection) mvDelaySection.style.display = movementMode === "dep" ? "" : "none";
    if (movementResult) movementResult.textContent = "";
    if (movementMode === "dep") recalcMovementDelay();
  }

  function addMovementDelayRow(row = {}) {
    if (!mvDelayRows) return;
    const tr = document.createElement("tr");
    tr.dataset.delayId = row.id != null && row.id !== "" ? String(row.id) : "";
    tr.innerHTML = `
      <td><input class="form-control form-control-sm mv-delay-code" type="text" maxlength="2" value="${row.code || row.delayCode || ""}"></td>
      <td><input class="form-control form-control-sm mv-delay-mins" type="number" min="0" step="1" value="${row.delayMinutes || ""}"></td>
      <td class="mv-delay-desc">${row.description || row.delayCodeDescription || ""}</td>
      <td><input class="form-control form-control-sm mv-delay-remark" type="text" value="${row.remarks || row.comment || ""}"></td>
      <td><button type="button" class="btn btn-ghost mv-delay-del">Remove</button></td>
    `;
    const codeInput = tr.querySelector(".mv-delay-code");
    const minsInput = tr.querySelector(".mv-delay-mins");
    const delBtn = tr.querySelector(".mv-delay-del");
    const descEl = tr.querySelector(".mv-delay-desc");

    function syncCode() {
      let code = String(codeInput.value || "").replace(/\D/g, "").slice(0, 2);
      codeInput.value = code;
      const lookup = code.length === 1 ? `0${code}` : code;
      const meta = DELAY_CODE_META[lookup];
      descEl.textContent = meta ? meta.description : (code ? "Unknown delay code" : "");
      recalcMovementDelay();
    }

    codeInput.addEventListener("input", syncCode);
    minsInput.addEventListener("input", recalcMovementDelay);
    delBtn.addEventListener("click", () => { tr.remove(); recalcMovementDelay(); });

    mvDelayRows.appendChild(tr);
    syncCode();
  }

  function collectMovementDelays() {
    const rows = [];
    let total = 0;
    let hasInvalid = false;
    if (!mvDelayRows) return { rows, total, hasInvalid };
    mvDelayRows.querySelectorAll("tr").forEach((tr) => {
      const delayIdRaw = tr.dataset.delayId || "";
      const codeInput = tr.querySelector(".mv-delay-code");
      const minsInput = tr.querySelector(".mv-delay-mins");
      const remarkInput = tr.querySelector(".mv-delay-remark");
      let code = String(codeInput?.value || "").replace(/\D/g, "").slice(0, 2);
      const mins = Number(minsInput?.value || 0);
      const comment = String(remarkInput?.value || "").trim();
      if (!code && !mins) return;
      const lookup = code.length === 1 ? `0${code}` : code;
      const meta = DELAY_CODE_META[lookup];
      if (!meta || !Number.isFinite(mins) || mins <= 0) {
        hasInvalid = true;
        return;
      }
      total += mins;
      let id = null;
      if (delayIdRaw !== "") {
        const parsed = Number(delayIdRaw);
        id = Number.isFinite(parsed) ? parsed : null;
      }
      rows.push({
        id,
        code: lookup,
        delayCodeId: meta.id,
        delayCode: lookup,
        delayCodeDescription: meta.description,
        delayMinutes: mins,
        isArrival: false,
        comment,
        remarks: comment,
      });
    });
    return { rows, total, hasInvalid };
  }

  function requiredMovementDelayMinutes() {
    if (!movementFlight || movementMode !== "dep") return 0;
    const stdHm = hmFromIso(movementFlight.std_sched_nz || movementFlight.std_nz);
    const offHm = mvOffblocks ? mvOffblocks.value : "";
    const stdMin = parseHmToMinutes(stdHm);
    const offMin = parseHmToMinutes(offHm);
    if (stdMin === null || offMin === null) return 0;
    const diff = offMin - stdMin;
    return diff > 15 ? diff : 0;
  }

  function recalcMovementDelay() {
    if (movementMode !== "dep") return;
    const required = requiredMovementDelayMinutes();
    const { total, rows } = collectMovementDelays();
    const remaining = Math.max(0, required - total);
    if (mvDelayRequired) mvDelayRequired.textContent = String(required);
    if (mvDelayAllocated) mvDelayAllocated.textContent = String(total);
    if (mvDelayRemaining) mvDelayRemaining.textContent = String(remaining);
    if (mvAddDelay) mvAddDelay.disabled = required <= 0;
    if (mvDelaySection) mvDelaySection.style.display = (required > 0 || rows.length > 0) ? "" : "none";
    if (movementSubmit) movementSubmit.disabled = required > 0 && total > required;
    if (movementResult && required > 0 && total > required) {
      movementResult.textContent = `Allocated delay (${total}) exceeds required delay (${required}).`;
    } else if (movementResult && movementResult.textContent && movementResult.textContent.startsWith("Allocated delay")) {
      movementResult.textContent = "";
    }
  }

  async function openMovementDialog() {
    if (!selectedFlight || !movementDialog) return;
    movementFlight = selectedFlight;
    if (movementTitle) {
      movementTitle.textContent = `Movement Message - ${flightCode(movementFlight)} (${movementFlight.envision_flight_id})`;
    }

    if (mvEtd) mvEtd.value = hmFromIso(movementFlight.std_nz);
    if (mvEta) mvEta.value = hmFromIso(movementFlight.sta_nz);
    if (mvDepDate) mvDepDate.value = ymdFromIso(movementFlight.std_sched_nz || movementFlight.std_nz) || dayInput.value || "";
    if (mvArrDate) mvArrDate.value = ymdFromIso(movementFlight.sta_sched_nz || movementFlight.sta_nz) || dayInput.value || "";
    if (mvOffblocks) mvOffblocks.value = hmFromIso(movementFlight.dep_actual_nz);
    if (mvLanding) mvLanding.value = hmFromIso(movementFlight.arr_actual_nz);
    if (mvOnchocks) mvOnchocks.value = hmFromIso(movementFlight.arr_actual_nz);
    if (mvAirborne) mvAirborne.value = "";
    if (movementResult) movementResult.textContent = "";
    if (movementSubmit) movementSubmit.disabled = false;
    if (mvDelayRows) mvDelayRows.innerHTML = "";

    if (movementFlight.envision_flight_id) {
      const hm = await fetchEnvisionLocalHm(movementFlight.envision_flight_id);
      if (hm) {
        if (mvOffblocks && hm.departureActual) mvOffblocks.value = hm.departureActual;
        if (mvAirborne && hm.departureTakeOff) mvAirborne.value = hm.departureTakeOff;
        if (mvLanding && hm.arrivalLanded) mvLanding.value = hm.arrivalLanded;
        if (mvOnchocks && hm.arrivalActual) mvOnchocks.value = hm.arrivalActual;
      }
    }

    const existingDelays = Array.isArray(movementFlight.delays)
      ? movementFlight.delays.filter((d) => !d.isArrival)
      : [];
    if (existingDelays.length) {
      existingDelays.forEach((d) => addMovementDelayRow(d));
    } else {
      addMovementDelayRow();
    }
    setMovementMode("dep");
    movementInitial = movementState();
    movementDialog.showModal();
  }

  async function submitMovementMessage() {
    if (!movementFlight || !saveTimesUrl) return;
    const now = movementState();
    if (samePayload(now, movementInitial || {})) {
      if (movementResult) movementResult.textContent = "No changes detected. Nothing sent to Envision.";
      return;
    }

    const depChanged = !samePayload(
      { etd: now.etd, dep_date: now.dep_date, offblocks: now.offblocks, airborne: now.airborne, delays: now.delays },
      { etd: movementInitial?.etd || "", dep_date: movementInitial?.dep_date || "", offblocks: movementInitial?.offblocks || "", airborne: movementInitial?.airborne || "", delays: movementInitial?.delays || [] },
    );
    const arrChanged = !samePayload(
      { eta: now.eta, arr_date: now.arr_date, landing: now.landing, onchocks: now.onchocks },
      { eta: movementInitial?.eta || "", arr_date: movementInitial?.arr_date || "", landing: movementInitial?.landing || "", onchocks: movementInitial?.onchocks || "" },
    );

    const req = requiredMovementDelayMinutes();
    const delayInfo = collectMovementDelays();
    if (movementMode === "dep" && req > 0) {
      if (delayInfo.hasInvalid) {
        if (movementResult) movementResult.textContent = "Invalid delay row(s). Use known 2-digit delay codes and minutes > 0.";
        return;
      }
      if (delayInfo.total > req) {
        if (movementResult) movementResult.textContent = `Allocated delay (${delayInfo.total}) exceeds required delay (${req}).`;
        return;
      }
    }

    if (movementSubmit) {
      movementSubmit.disabled = true;
      movementSubmit.textContent = "Saving...";
    }

    const out = [];
    try {
      if (movementMode === "dep" && depChanged) {
        const depPayload = {
          mode: "dep",
          envision_flight_id: movementFlight.envision_flight_id,
          std_sched: movementFlight.std_sched_nz,
          dep_date: now.dep_date || null,
          etd: now.etd || null,
          offblocks: now.offblocks || null,
          airborne: now.airborne || null,
          delays: delayInfo.rows,
        };
        const depResp = await fetch(saveTimesUrl, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(depPayload),
        });
        const depJson = await depResp.json();
        out.push({ mode: "dep", ok: depResp.ok && depJson.ok, response: depJson });
        if (!depResp.ok || !depJson.ok) throw new Error(depJson.error || "Departure save failed");
      }
      if (movementMode === "arr" && arrChanged) {
        const arrPayload = {
          mode: "arr",
          envision_flight_id: movementFlight.envision_flight_id,
          sta_sched: movementFlight.sta_sched_nz,
          arr_date: now.arr_date || null,
          eta: now.eta || null,
          landing: now.landing || null,
          onchocks: now.onchocks || null,
          delays: [],
        };
        const arrResp = await fetch(saveTimesUrl, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(arrPayload),
        });
        const arrJson = await arrResp.json();
        out.push({ mode: "arr", ok: arrResp.ok && arrJson.ok, response: arrJson });
        if (!arrResp.ok || !arrJson.ok) throw new Error(arrJson.error || "Arrival save failed");
      }
      movementInitial = now;
      if (movementResult) movementResult.textContent = JSON.stringify({ ok: true, updates: out }, null, 2);
      // Do not block button reset on a background refresh request.
      loadData({ showSpinner: false }).catch((err) => {
        console.warn("Background refresh after movement save failed", err);
      });
    } catch (e) {
      if (movementResult) movementResult.textContent = JSON.stringify({ ok: false, error: e.message, updates: out }, null, 2);
    } finally {
      if (movementSubmit) {
        movementSubmit.disabled = false;
        movementSubmit.textContent = "OK";
      }
    }
  }

  function normalizeForCompare(value) {
    if (Array.isArray(value)) return value.map(normalizeForCompare);
    if (value && typeof value === "object") {
      return Object.keys(value).sort().reduce((acc, k) => {
        acc[k] = normalizeForCompare(value[k]);
        return acc;
      }, {});
    }
    return value;
  }

  function samePayload(a, b) {
    return JSON.stringify(normalizeForCompare(a)) === JSON.stringify(normalizeForCompare(b));
  }

  async function fetchEnvisionLocalHm(flightId, { force = false } = {}) {
    const key = String(flightId || "");
    if (!key || !envisionTimesUrl) return null;
    if (!force && envisionLocalHmCache.has(key)) return envisionLocalHmCache.get(key);
    try {
      const u = `${envisionTimesUrl}?flight_id=${encodeURIComponent(key)}`;
      const resp = await fetch(u, { headers: { Accept: "application/json" } });
      const js = await resp.json();
      if (!resp.ok || !js.ok) return null;
      const hm = js.local_hm || null;
      envisionLocalHmCache.set(key, hm);
      return hm;
    } catch (_err) {
      return null;
    }
  }

  async function fetchEnvisionFlightRaw(flightId) {
    if (!envisionFlightRawUrl) return null;
    try {
      const u = `${envisionFlightRawUrl}?flight_id=${encodeURIComponent(flightId)}`;
      const resp = await fetch(u, { headers: { Accept: "application/json" } });
      const js = await resp.json();
      if (!resp.ok || !js.ok) return null;
      return js.raw || null;
    } catch (_e) {
      return null;
    }
  }

  function defaultEditState(f, rawFlight) {
    const fid = Number(f?.envision_flight_id || 0);
    const raw = rawFlight || {};
    return {
      update_flight: {
        id: Number(raw.id || fid),
        flightStatusId: Number(raw.flightStatusId || 0),
        departureEstimate: raw.departureEstimate || null,
        departureActual: raw.departureActual || null,
        departureTakeOff: raw.departureTakeOff || null,
        arrivalEstimate: raw.arrivalEstimate || null,
        arrivalLanded: raw.arrivalLanded || null,
        arrivalActual: raw.arrivalActual || null,
        plannedFlightTime: Number(raw.plannedFlightTime || 0),
        calculatedTakeOffTime: raw.calculatedTakeOffTime || null,
      },
      change_registration: {
        ignoreValidations: true,
        flightId: fid,
        lineId: 0,
        crewPositions: [{ id: 0, employeeId: 0, crewPositionId: 0 }],
      },
      cancel: {
        flightId: fid,
        cancelledLineId: 0,
        cancelCodeId: 0,
        remarks: "",
      },
      divert: {
        flightId: fid,
        divertedPlaceId: 0,
        arrivalEstimate: raw.arrivalEstimate || null,
        remarks: "",
      },
      delay_get: {
        delayId: "",
      },
      delay_put: {
        delayId: "",
        id: 0,
        flightId: fid,
        isArrival: true,
        delayCodeId: 0,
        delayCode: "",
        delayCodeDescription: "",
        delayMinutes: 0,
        remarks: "",
      },
      delay_post: {
        delayId: "",
        id: 0,
        flightId: fid,
        isArrival: true,
        delayCodeId: 0,
        delayCode: "",
        delayCodeDescription: "",
        delayMinutes: 0,
        remarks: "",
      },
    };
  }

  function setVisibleActionSection(action) {
    flightEditSections.forEach((sec) => {
      const actions = String(sec.dataset.action || "").split(/\s+/).filter(Boolean);
      sec.classList.toggle("active", actions.includes(action));
    });
  }

  function setFormFromAction(action, payload) {
    if (flightEditResult) flightEditResult.textContent = "";
    setVisibleActionSection(action);
    if (!payload) return;
    if (flightEditDelayId) flightEditDelayId.value = payload.delayId || "";

    if (action === "update_flight") {
      feFlightStatusId.value = payload.flightStatusId ?? 0;
      fePlannedFlightTime.value = payload.plannedFlightTime ?? 0;
      feDepartureEstimate.value = toLocalInputValue(payload.departureEstimate);
      feDepartureActual.value = toLocalInputValue(payload.departureActual);
      feDepartureTakeOff.value = toLocalInputValue(payload.departureTakeOff);
      feArrivalEstimate.value = toLocalInputValue(payload.arrivalEstimate);
      feArrivalLanded.value = toLocalInputValue(payload.arrivalLanded);
      feArrivalActual.value = toLocalInputValue(payload.arrivalActual);
      feCalculatedTakeOffTime.value = toLocalInputValue(payload.calculatedTakeOffTime);
      return;
    }
    if (action === "change_registration") {
      feRegIgnoreValidations.checked = !!payload.ignoreValidations;
      feRegLineId.value = payload.lineId ?? 0;
      feRegCrewPositions.value = JSON.stringify(payload.crewPositions || [], null, 2);
      return;
    }
    if (action === "cancel") {
      feCancelLineId.value = payload.cancelledLineId ?? 0;
      feCancelCodeId.value = payload.cancelCodeId ?? 0;
      feCancelRemarks.value = payload.remarks || "";
      return;
    }
    if (action === "divert") {
      feDivertPlaceId.value = payload.divertedPlaceId ?? 0;
      feDivertArrivalEstimate.value = toLocalInputValue(payload.arrivalEstimate);
      feDivertRemarks.value = payload.remarks || "";
      return;
    }
    if (action === "delay_put" || action === "delay_post") {
      feDelayRecordId.value = payload.id ?? 0;
      feDelayIsArrival.checked = !!payload.isArrival;
      feDelayCodeId.value = payload.delayCodeId ?? 0;
      feDelayCode.value = payload.delayCode || "";
      feDelayCodeDescription.value = payload.delayCodeDescription || "";
      feDelayMinutes.value = payload.delayMinutes ?? 0;
      feDelayRemarks.value = payload.remarks || "";
    }
  }

  function readFormPayload(action) {
    if (!editingFlight) return { payload: {}, delayId: null };
    const fid = Number(editingFlight.envision_flight_id || 0);

    if (action === "update_flight") {
      return {
        delayId: null,
        payload: {
          id: fid,
          flightStatusId: Number(feFlightStatusId.value || 0),
          departureEstimate: toIsoFromLocalInput(feDepartureEstimate.value),
          departureActual: toIsoFromLocalInput(feDepartureActual.value),
          departureTakeOff: toIsoFromLocalInput(feDepartureTakeOff.value),
          arrivalEstimate: toIsoFromLocalInput(feArrivalEstimate.value),
          arrivalLanded: toIsoFromLocalInput(feArrivalLanded.value),
          arrivalActual: toIsoFromLocalInput(feArrivalActual.value),
          plannedFlightTime: Number(fePlannedFlightTime.value || 0),
          calculatedTakeOffTime: toIsoFromLocalInput(feCalculatedTakeOffTime.value),
        },
      };
    }
    if (action === "change_registration") {
      let crewPositions = [];
      try {
        crewPositions = JSON.parse(feRegCrewPositions.value || "[]");
      } catch (e) {
        throw new Error(`Crew Positions JSON is invalid: ${e.message}`);
      }
      return {
        delayId: null,
        payload: {
          ignoreValidations: !!feRegIgnoreValidations.checked,
          flightId: fid,
          lineId: Number(feRegLineId.value || 0),
          crewPositions,
        },
      };
    }
    if (action === "cancel") {
      return {
        delayId: null,
        payload: {
          flightId: fid,
          cancelledLineId: Number(feCancelLineId.value || 0),
          cancelCodeId: Number(feCancelCodeId.value || 0),
          remarks: feCancelRemarks.value || "",
        },
      };
    }
    if (action === "divert") {
      return {
        delayId: null,
        payload: {
          flightId: fid,
          divertedPlaceId: Number(feDivertPlaceId.value || 0),
          arrivalEstimate: toIsoFromLocalInput(feDivertArrivalEstimate.value),
          remarks: feDivertRemarks.value || "",
        },
      };
    }
    if (action === "delay_get") {
      const did = flightEditDelayId && flightEditDelayId.value ? Number(flightEditDelayId.value) : null;
      return { delayId: did, payload: { delayId: did || "" } };
    }
    const did = flightEditDelayId && flightEditDelayId.value ? Number(flightEditDelayId.value) : null;
    return {
      delayId: did,
      payload: {
        id: Number(feDelayRecordId.value || 0),
        flightId: fid,
        isArrival: !!feDelayIsArrival.checked,
        delayCodeId: Number(feDelayCodeId.value || 0),
        delayCode: feDelayCode.value || "",
        delayCodeDescription: feDelayCodeDescription.value || "",
        delayMinutes: Number(feDelayMinutes.value || 0),
        remarks: feDelayRemarks.value || "",
        delayId: did || "",
      },
    };
  }

  function refreshEditFormFromAction() {
    if (!editingFlight || !flightEditAction) return;
    const action = flightEditAction.value;
    setFormFromAction(action, editInitialByAction[action]);
  }

  async function openFlightEditDialog(f) {
    if (!flightEditDialog || !flightEditAction) return;
    editingFlight = f || null;
    if (!editingFlight) return;
    if (flightEditTitle) {
      flightEditTitle.textContent = `Envision Flight Update - ${flightCode(editingFlight)} (${editingFlight.envision_flight_id})`;
    }
    if (flightEditResult) flightEditResult.textContent = "Loading existing Envision data...";
    const raw = await fetchEnvisionFlightRaw(editingFlight.envision_flight_id);
    editInitialByAction = defaultEditState(editingFlight, raw);
    flightEditAction.value = "update_flight";
    refreshEditFormFromAction();
    flightEditDialog.showModal();
  }

  async function openModifyLegDialog(f) {
    if (!modifyLegDialog) return;
    modifyLegFlight = f || null;
    modifyLegNote = null;
    modifyLegFlightTypeOriginalId = null;
    const code = flightCode(f);
    const day = String(dayInput?.value || app.dataset.day || "");
    const toHm = (iso) => hmFromIso(iso) || "";
    const toYmd = (iso) => ymdFromIso(iso) || day;
    const startIso = f.dep_actual_nz || f.std_nz || f.std_sched_nz || null;
    const endIso = f.arr_actual_nz || f.sta_nz || f.sta_sched_nz || null;
    let blockText = "";
    if (startIso && endIso) {
      const ms = (new Date(endIso)).getTime() - (new Date(startIso)).getTime();
      if (Number.isFinite(ms) && ms > 0) {
        const mins = Math.round(ms / 60000);
        blockText = `${String(Math.floor(mins / 60)).padStart(2, "0")}:${String(mins % 60).padStart(2, "0")}`;
      }
    }
    if (modifyLegTitle) {
      modifyLegTitle.textContent = `Modify Leg - ${code} (${f.envision_flight_id || "-"})`;
    }
    updateModifyLegNavButtons();
    const hm = await fetchEnvisionLocalHm(f.envision_flight_id);
    const rawFlight = await fetchEnvisionFlightRaw(f.envision_flight_id);

    if (mlReg) {
      populateLineRegistrations(f.reg || "");
    }
    if (mlAirline) mlAirline.value = "Air Chathams";
    if (mlFlightNo) mlFlightNo.value = code;
    if (mlDate) mlDate.value = day;
    await populateFlightTypes({
      selectedId: rawFlight?.flightTypeId,
      selectedText: rawFlight?.flightTypeDescription || rawFlight?.flightType || f.service_type || "Scheduled",
    });

    if (mlSchedDate) mlSchedDate.value = toYmd(f.std_sched_nz || f.std_nz);
    if (mlDep) mlDep.value = f.dep || "";
    if (mlStd) mlStd.value = toHm(f.std_sched_nz);
    if (mlArr) mlArr.value = f.ades || "";
    if (mlSta) mlSta.value = toHm(f.sta_sched_nz);

    if (mlOpDate) mlOpDate.value = toYmd(f.std_nz || f.std_sched_nz);
    if (mlOut) mlOut.value = (hm && hm.departureActual) || toHm(f.dep_actual_nz);
    if (mlOff) mlOff.value = (hm && hm.departureTakeOff) || toHm(f.dep_actual_nz);
    if (mlEnroute) mlEnroute.value = blockText;
    if (mlOn) mlOn.value = (hm && hm.arrivalLanded) || toHm(f.arr_actual_nz);
    if (mlIn) mlIn.value = (hm && hm.arrivalActual) || toHm(f.arr_actual_nz);
    if (mlCtot) mlCtot.value = "";

    if (mlActDate) mlActDate.value = toYmd(f.dep_actual_nz || f.std_nz || f.std_sched_nz);
    if (mlActOut) mlActOut.value = (hm && hm.departureActual) || toHm(f.dep_actual_nz);
    if (mlActOff) mlActOff.value = (hm && hm.departureTakeOff) || toHm(f.dep_actual_nz);
    if (mlActOn) mlActOn.value = (hm && hm.arrivalLanded) || toHm(f.arr_actual_nz);
    if (mlActIn) mlActIn.value = (hm && hm.arrivalActual) || toHm(f.arr_actual_nz);
    if (mlBlock) mlBlock.value = blockText;

    if (mlEpax) mlEpax.value = String(f.pax_count || 0);
    if (mlApax) mlApax.value = String(f.pax_count || 0);
    if (mlCargo) mlCargo.value = String(Math.round(Number(f.bags_kg || 0)));
    if (mlRemark) mlRemark.value = "";

    const notes = await fetchFlightNotes(f.envision_flight_id);
    if (notes.length) {
      const preferred = notes.find((n) => String(n.noteType || "").toUpperCase().includes("TOCC")) || notes[0];
      modifyLegNote = preferred;
      if (mlRemark) mlRemark.value = preferred.text || "";
    }

    if (mlApiResponses) {
      mlApiResponses.textContent = JSON.stringify({
        flight: {
          id: f.envision_flight_id,
          reg: f.reg,
          dep: f.dep,
          ades: f.ades,
          std_sched_nz: f.std_sched_nz,
          sta_sched_nz: f.sta_sched_nz,
          etd_eta: { etd: f.std_nz, eta: f.sta_nz },
          actuals: { atd: f.dep_actual_nz, ata: f.arr_actual_nz },
        },
        delays: Array.isArray(f.delays) ? f.delays : [],
        notes,
        flight_raw: rawFlight,
      }, null, 2);
    }

    modifyLegDialog.showModal();
  }

  async function populateLineRegistrations(selectedReg = "") {
    if (!mlReg) return;
    const target = String(selectedReg || "").toUpperCase();

    function setOptions(items) {
      const opts = Array.isArray(items) ? items : [];
      const normalized = opts.map((it) => {
        const text = String(
          it.flightRegistrationDescription
          || it.registration
          || it.description
          || it.code
          || it.name
          || ""
        ).trim();
        const value = text || String(
          it.registration
          || it.code
          || it.name
          || it.id
          || ""
        ).trim();
        return { value, text: text || value };
      }).filter((x) => x.value);

      mlReg.innerHTML = "";
      if (!normalized.length) {
        const single = target || "";
        mlReg.innerHTML = `<option value="${single}">${single || "No registrations available"}</option>`;
        return;
      }

      normalized.forEach((opt) => {
        const o = document.createElement("option");
        o.value = opt.value;
        o.textContent = opt.text;
        if (target && opt.value.toUpperCase() === target) o.selected = true;
        mlReg.appendChild(o);
      });

      if (target && !normalized.some((o) => o.value.toUpperCase() === target)) {
        const fallback = document.createElement("option");
        fallback.value = selectedReg;
        fallback.textContent = selectedReg;
        fallback.selected = true;
        mlReg.insertBefore(fallback, mlReg.firstChild);
      }
    }

    if (lineRegistrationCache) {
      setOptions(lineRegistrationCache);
      return;
    }

    if (!envisionLineRegistrationsUrl) {
      setOptions([]);
      return;
    }

    mlReg.innerHTML = '<option value="">Loading...</option>';
    try {
      const resp = await fetch(envisionLineRegistrationsUrl, { headers: { Accept: "application/json" } });
      const js = await resp.json();
      const items = (js && (js.items || js.results || js.data)) || [];
      lineRegistrationCache = Array.isArray(items) ? items : [];
      setOptions(lineRegistrationCache);
    } catch (_err) {
      setOptions([]);
    }
  }

  function normalizeRegistrationCatalog(items) {
    const out = [];
    for (const it of Array.isArray(items) ? items : []) {
      const reg = String(
        it.flightRegistrationDescription
        || it.registration
        || it.description
        || it.code
        || it.name
        || "",
      ).trim().toUpperCase();
      const regIdRaw = it.flightRegistrationId ?? it.registrationId ?? it.regId ?? it.id;
      let regId = null;
      try {
        regId = Number(regIdRaw);
      } catch (_e) {
        regId = null;
      }
      if (!reg || !Number.isFinite(regId) || regId <= 0) continue;
      out.push({ reg, regId: Number(regId) });
    }
    const seen = new Set();
    return out.filter((x) => {
      const key = `${x.regId}:${x.reg}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
  }

  async function fetchRegistrationCatalog() {
    if (registrationCatalogCache) return registrationCatalogCache;
    if (Array.isArray(lineRegistrationCache) && lineRegistrationCache.length) {
      registrationCatalogCache = normalizeRegistrationCatalog(lineRegistrationCache);
      return registrationCatalogCache;
    }
    if (!envisionLineRegistrationsUrl) return [];
    try {
      const resp = await fetch(envisionLineRegistrationsUrl, { headers: { Accept: "application/json" } });
      const js = await resp.json();
      const items = (js && (js.items || js.results || js.data)) || [];
      lineRegistrationCache = Array.isArray(items) ? items : [];
      registrationCatalogCache = normalizeRegistrationCatalog(lineRegistrationCache);
      return registrationCatalogCache;
    } catch (_err) {
      return [];
    }
  }

  async function populateFlightTypes({ selectedId = null, selectedText = "" } = {}) {
    if (!mlFlightType) return;
    const targetId = Number(selectedId);
    const hasTargetId = Number.isFinite(targetId) && targetId > 0;
    const targetText = String(selectedText || "").trim().toLowerCase();

    function asOptions(items) {
      const mapped = (Array.isArray(items) ? items : []).map((it) => {
        const id = Number(it.id);
        const type = String(it.type || "").trim();
        const description = String(it.description || "").trim();
        const label = description ? `${type} - ${description}` : type;
        return {
          id,
          type,
          description,
          label: label || String(it.id || ""),
        };
      }).filter((x) => Number.isFinite(x.id) && x.id > 0 && x.label);
      return mapped;
    }

    function setOptions(items) {
      const options = asOptions(items);
      mlFlightType.innerHTML = "";
      if (!options.length) {
        const fallback = document.createElement("option");
        fallback.value = "";
        fallback.textContent = selectedText || "No flight types available";
        mlFlightType.appendChild(fallback);
        modifyLegFlightTypeOriginalId = null;
        return;
      }

      let selectedValue = null;
      for (const item of options) {
        const opt = document.createElement("option");
        opt.value = String(item.id);
        opt.textContent = item.label;
        opt.dataset.type = item.type;
        opt.dataset.description = item.description;
        if (hasTargetId && item.id === targetId) {
          opt.selected = true;
          selectedValue = item.id;
        }
        mlFlightType.appendChild(opt);
      }

      if (!selectedValue && targetText) {
        const match = options.find((o) => {
          const t = o.type.toLowerCase();
          const d = o.description.toLowerCase();
          return t === targetText || d === targetText || `${t} - ${d}` === targetText;
        });
        if (match) {
          mlFlightType.value = String(match.id);
          selectedValue = match.id;
        }
      }

      if (!selectedValue && options.length) {
        mlFlightType.selectedIndex = 0;
        selectedValue = Number(mlFlightType.value || 0) || null;
      }
      modifyLegFlightTypeOriginalId = selectedValue;
    }

    if (flightTypeCache) {
      setOptions(flightTypeCache);
      return;
    }

    if (!envisionFlightTypesUrl) {
      setOptions([]);
      return;
    }

    mlFlightType.innerHTML = '<option value="">Loading...</option>';
    try {
      const resp = await fetch(envisionFlightTypesUrl, { headers: { Accept: "application/json" } });
      const js = await resp.json();
      const items = (js && (js.items || js.results || js.data)) || [];
      flightTypeCache = Array.isArray(items) ? items : [];
      setOptions(flightTypeCache);
    } catch (_err) {
      setOptions([]);
    }
  }

  async function fetchFlightNotes(flightId) {
    const key = String(flightId || "");
    if (!key || !envisionFlightNotesUrl) return [];
    try {
      const u = `${envisionFlightNotesUrl}?flight_id=${encodeURIComponent(key)}&crew_view=false`;
      const resp = await fetch(u, { headers: { Accept: "application/json" } });
      const js = await resp.json();
      if (!resp.ok || !js.ok || !Array.isArray(js.notes)) return [];
      return js.notes;
    } catch (_err) {
      return [];
    }
  }

  function flightsForSameRegistration(flight) {
    if (!flight) return [];
    const reg = String(flight.reg || "").trim().toUpperCase();
    if (!reg) return [];
    return flights
      .filter((f) => !isCancelledFlight(f) && String(f.reg || "").trim().toUpperCase() === reg)
      .sort((a, b) => (flightStartMinutes(a) ?? 99999) - (flightStartMinutes(b) ?? 99999));
  }

  function adjacentFlightForModifyLeg(offset) {
    if (!modifyLegFlight) return null;
    const list = flightsForSameRegistration(modifyLegFlight);
    if (!list.length) return null;
    const idx = list.findIndex((f) => String(f.envision_flight_id) === String(modifyLegFlight.envision_flight_id));
    if (idx < 0) return null;
    const target = idx + offset;
    if (target < 0 || target >= list.length) return null;
    return list[target];
  }

  function updateModifyLegNavButtons() {
    if (mlPrev) mlPrev.disabled = !adjacentFlightForModifyLeg(-1);
    if (mlNext) mlNext.disabled = !adjacentFlightForModifyLeg(1);
  }

  async function saveModifyLegRemark({ closeOnDone = false } = {}) {
    if (!modifyLegFlight) return;
    const text = String(mlRemark.value || "").trim();
    const selectedFlightTypeId = Number(mlFlightType?.value || 0) || null;
    const shouldChangeType = !!(
      selectedFlightTypeId
      && (modifyLegFlightTypeOriginalId === null || Number(modifyLegFlightTypeOriginalId) !== Number(selectedFlightTypeId))
    );

    const output = { ok: true, updates: {} };
    if (!text && !shouldChangeType) {
      output.updates = { message: "No remark or flight type change detected. Nothing saved." };
      if (mlApiResponses) mlApiResponses.textContent = JSON.stringify(output, null, 2);
      if (closeOnDone && modifyLegDialog?.open) modifyLegDialog.close();
      return;
    }

    const btns = [mlOk].filter(Boolean);
    btns.forEach((b) => { b.disabled = true; });
    try {
      if (shouldChangeType && envisionActionUrl) {
        const changeTypePayload = {
          action: "change_type",
          flight_id: modifyLegFlight.envision_flight_id,
          payload: {
            flightId: Number(modifyLegFlight.envision_flight_id),
            typeId: Number(selectedFlightTypeId),
          },
        };
        const typeResp = await fetch(envisionActionUrl, {
          method: "POST",
          headers: { "Content-Type": "application/json", Accept: "application/json" },
          body: JSON.stringify(changeTypePayload),
        });
        const typeJs = await typeResp.json();
        output.updates.flight_type = typeJs;
        if (!typeResp.ok || !typeJs.ok) output.ok = false;
        if (typeResp.ok && typeJs.ok) {
          modifyLegFlightTypeOriginalId = Number(selectedFlightTypeId);
          if (modifyLegFlight) {
            const selectedOpt = mlFlightType?.selectedOptions?.[0];
            if (selectedOpt?.dataset?.type) modifyLegFlight.service_type = selectedOpt.dataset.type;
          }
        }
      }

      if (text && envisionFlightNotesUpsertUrl) {
        const notesPayload = {
          flight_id: modifyLegFlight.envision_flight_id,
          note_id: modifyLegNote?.id ?? null,
          note_type_id: modifyLegNote?.noteTypeId ?? null,
          text,
          is_important: !!(modifyLegNote?.isImportant),
        };
        const notesResp = await fetch(envisionFlightNotesUpsertUrl, {
          method: "POST",
          headers: { "Content-Type": "application/json", Accept: "application/json" },
          body: JSON.stringify(notesPayload),
        });
        const notesJs = await notesResp.json();
        output.updates.remark = notesJs;
        if (!notesResp.ok || !notesJs.ok) output.ok = false;
        if (notesResp.ok && notesJs.ok && notesJs.note) {
          modifyLegNote = notesJs.note;
        }
      } else {
        output.updates.remark = { ok: true, skipped: true, reason: "Empty remark" };
      }

      if (mlApiResponses) mlApiResponses.textContent = JSON.stringify(output, null, 2);
      if (output.ok && closeOnDone && modifyLegDialog?.open) modifyLegDialog.close();
    } catch (err) {
      if (mlApiResponses) mlApiResponses.textContent = JSON.stringify({ ok: false, error: err.message, updates: output.updates }, null, 2);
    } finally {
      btns.forEach((b) => { b.disabled = false; });
    }
  }

  async function submitFlightEdit() {
    if (!editingFlight || !envisionActionUrl || !flightEditAction) return;
    const action = flightEditAction.value;
    let read;
    try {
      read = readFormPayload(action);
    } catch (e) {
      if (flightEditResult) flightEditResult.textContent = e.message;
      return;
    }

    const currentForCompare = action === "delay_get"
      ? { delayId: read.delayId || "" }
      : Object.assign({}, read.payload, { delayId: read.delayId || "" });
    const initialForCompare = editInitialByAction[action] || {};
    if (samePayload(currentForCompare, initialForCompare)) {
      if (flightEditResult) flightEditResult.textContent = "No changes detected. Nothing sent to Envision.";
      return;
    }

    const body = {
      action,
      flight_id: Number(editingFlight.envision_flight_id),
      delay_id: read.delayId,
      payload: read.payload,
    };
    if (flightEditSubmit) {
      flightEditSubmit.disabled = true;
      flightEditSubmit.textContent = "Sending...";
    }
    try {
      const resp = await fetch(envisionActionUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const json = await resp.json();
      if (flightEditResult) flightEditResult.textContent = JSON.stringify(json, null, 2);
      if (resp.ok && json.ok) {
        editInitialByAction[action] = currentForCompare;
        await loadData({ showSpinner: false });
      }
    } catch (e) {
      if (flightEditResult) flightEditResult.textContent = `Request failed: ${e.message}`;
    } finally {
      if (flightEditSubmit) {
        flightEditSubmit.disabled = false;
        flightEditSubmit.textContent = "OK";
      }
    }
  }

  function classifyPaxStatus(p) {
    const raw = String(
      p.DCSStatus || p.DcsStatus || p.Status || p.status || p.CheckInStatus || p.checkInStatus || p.BoardingStatus || p.boardingStatus || ""
    ).toUpperCase();
    if (p.Flown === true || p.flown === true || raw.includes("FLOWN") || raw === "FLWN") return "FLOWN";
    if (p.Boarded === true || p.boarded === true || raw.includes("BOARD") || raw === "BD" || raw === "BRD") return "BOARDED";
    if (p.CheckedIn === true || p.checkedIn === true || raw.includes("CHECK") || raw === "CI" || raw === "CKI" || raw === "CKIN") return "CHECKED";
    return "BOOKED";
  }

  function paxTypeBreakdown(list) {
    const out = { ad: 0, chd: 0, inf: 0 };
    (list || []).forEach((p) => {
      const t = String(p.PassengerType || p.passengerType || "").toUpperCase();
      if (t === "INF" || t === "IN" || t === "INFANT") out.inf += 1;
      else if (t === "CHD" || t === "CH" || t === "C" || t === "CNN" || t === "CHILD") out.chd += 1;
      else out.ad += 1;
    });
    return out;
  }

  function normalizeDelays(list) {
    if (!Array.isArray(list)) return [];
    return list.map((d) => {
      const code = String(d.delayCode || d.code || d.delay_code || "").trim();
      const mins = Number(d.delayMinutes ?? d.delay_minutes ?? 0);
      const remarks = String(d.remarks || d.remark || d.comment || "").trim();
      return { code, mins, remarks };
    }).filter((d) => d.code || d.mins > 0 || d.remarks);
  }

  function minutesFromMidnight(isoString) {
    const tz = getActiveTimeZone();
    const zp = getZonedParts(isoString, tz);
    if (!zp) return null;
    const sd = getSelectedDayParts();
    const dateA = Date.UTC(zp.y, zp.m - 1, zp.d);
    const dateB = Date.UTC(sd.y, sd.m - 1, sd.d);
    const dayOffset = Math.round((dateA - dateB) / 86400000);
    return dayOffset * minuteInDay + zp.hh * 60 + zp.mm;
  }

  function flightStartMinutes(f) {
    return (
      minutesFromMidnight(f.dep_actual_nz) ??
      minutesFromMidnight(f.std_nz) ??
      minutesFromMidnight(f.std_sched_nz)
    );
  }

  function flightEndMinutes(f) {
    return (
      minutesFromMidnight(f.arr_actual_nz) ??
      minutesFromMidnight(f.sta_nz) ??
      minutesFromMidnight(f.sta_sched_nz)
    );
  }

  function flightMainStartIso(f) {
    return f.dep_actual_nz || f.std_nz || f.std_sched_nz || null;
  }

  function flightMainEndIso(f) {
    return f.arr_actual_nz || f.sta_nz || f.sta_sched_nz || null;
  }

  function updateTimeWindowFromFlights() {
    const starts = [];
    const ends = [];
    for (const f of flights) {
      const s = minutesFromMidnight(f.std_sched_nz) ?? flightStartMinutes(f);
      const e = flightEndMinutes(f) ?? (s !== null ? s + 45 : null);
      if (s !== null) starts.push(s);
      if (e !== null) ends.push(e);
    }
    if (!starts.length || !ends.length) {
      windowStartMin = 0;
      windowEndMin = minuteInDay;
      return;
    }
    const minStd = Math.min(...starts);
    const maxEta = Math.max(...ends);
    windowStartMin = Math.max(0, minStd - 120);
    windowEndMin = Math.min(minuteInDay, maxEta + 120);
    if (windowEndMin - windowStartMin < 240) {
      windowEndMin = Math.min(minuteInDay, windowStartMin + 240);
    }
  }

  function minuteToPx(minute) {
    return (minute - windowStartMin) * pxPerMinute;
  }

  function rangeToPixels(startMin, endMin) {
    if (startMin === null || endMin === null) return null;
    const startPxRaw = minuteToPx(startMin);
    const endPxRaw = minuteToPx(endMin);
    const timelinePx = Math.max(1, (windowEndMin - windowStartMin) * pxPerMinute);
    if (endPxRaw <= 0 || startPxRaw >= timelinePx) return null;
    const left = Math.max(0, startPxRaw);
    const right = Math.min(timelinePx, endPxRaw);
    return { left, width: Math.max(1, right - left) };
  }

  function assignLanes(flightList) {
    const laneEnds = [];
    const output = [];
    const sorted = [...flightList].sort((a, b) => {
      const as = flightStartMinutes(a) ?? 9999;
      const bs = flightStartMinutes(b) ?? 9999;
      return as - bs;
    });
    for (const f of sorted) {
      const start = flightStartMinutes(f);
      const end = flightEndMinutes(f) ?? (start !== null ? start + 45 : null);
      if (start === null || end === null) {
        output.push({ f, lane: 0, start, end });
        continue;
      }
      let lane = laneEnds.findIndex((laneEnd) => start >= laneEnd + 6);
      if (lane < 0) {
        lane = laneEnds.length;
        laneEnds.push(end);
      } else {
        laneEnds[lane] = end;
      }
      output.push({ f, lane, start, end });
    }
    return { items: output, laneCount: Math.max(1, laneEnds.length) };
  }

  function isCancelledFlight(f) {
    const s = String(f.flight_status || "").toLowerCase();
    return s.includes("cancel");
  }

  function barColor(status) {
    const s = String(status || "").toLowerCase();
    if (s.includes("planning")) return "#00b43f";
    if (s.includes("onblock")) return "#111111";
    if (s.includes("offblock")) return "#e56c6c";
    if (s.includes("takeoff") || s.includes("depart") || s.includes("airborne")) return "#0078ff";
    if (s.includes("land")) return "#00d8d0";
    if (s.includes("divert")) return "#ffab00";
    if (s.includes("return")) return "#ff8f00";
    if (s.includes("delay")) return "#f57f17";
    if (s.includes("cancel")) return "#d73a49";
    return "#52627a";
  }

  function applyTimelineScale() {
    const span = Math.max(60, windowEndMin - windowStartMin);
    const width = Math.round(span * pxPerMinute);
    app.style.setProperty("--timeline-width", `${width}px`);
  }

  function fitPxPerMinuteToViewport() {
    const board = document.querySelector(".board");
    const span = Math.max(60, windowEndMin - windowStartMin);
    if (!board) return;
    const available = Math.max(480, board.clientWidth - 260); // minus label col + padding
    const fitted = available / span;
    pxPerMinute = Math.max(0.7, Math.min(3.8, Number(fitted.toFixed(2))));
  }

  function buildAxis() {
    axisEl.innerHTML = '<div class="axis-label"></div><div class="axis-scroll"><div class="ticks"></div></div>';
    axisScrollEl = axisEl.querySelector(".axis-scroll");
    const ticks = axisEl.querySelector(".ticks");
    const firstHour = Math.floor(windowStartMin / 60);
    const lastHour = Math.ceil(windowEndMin / 60);
    const fmtHour = (minute) => {
      const dayOffset = Math.floor(minute / minuteInDay);
      const norm = ((minute % minuteInDay) + minuteInDay) % minuteInDay;
      const hh = Math.floor(norm / 60);
      const base = `${String(hh).padStart(2, "0")}:00`;
      if (!dayOffset) return base;
      return `${base} D${dayOffset > 0 ? `+${dayOffset}` : dayOffset}`;
    };
    for (let hour = firstHour; hour <= lastHour; hour += 1) {
      const minute = hour * 60;
      if (minute < windowStartMin || minute > windowEndMin) continue;
      const tick = document.createElement("div");
      tick.className = "tick";
      tick.style.left = `${minuteToPx(minute)}px`;
      tick.textContent = fmtHour(minute);
      ticks.appendChild(tick);
    }
    if (axisScrollEl) axisScrollEl.scrollLeft = rowsEl.scrollLeft || 0;
  }

  function findSelectedFlight() {
    if (!selectedId) return null;
    return flights.find((f) => String(f.envision_flight_id) === String(selectedId)) || null;
  }

  function escapeHtml(value) {
    return String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function showApgStatusModal(title, lines) {
    const safeLines = Array.isArray(lines) ? lines.filter(Boolean) : [];
    if (!apgStatusDialog || !apgStatusTitle || !apgStatusBody || typeof apgStatusDialog.showModal !== "function") {
      alert([title].concat(safeLines).join("\n"));
      return;
    }
    apgStatusTitle.textContent = title || "APG Submission";
    apgStatusBody.innerHTML = safeLines
      .map((line) => `<div>${escapeHtml(String(line))}</div>`)
      .join("");
    apgStatusDialog.showModal();
  }

  function updateEnvisionEnvPill(env) {
    if (!envisionEnvPill || !env) return;
    const key = String(env.key || "base").toLowerCase();
    envisionEnvPill.classList.remove("envision-env-base", "envision-env-test", "envision-env-prod");
    envisionEnvPill.classList.add(`envision-env-${key}`);
    envisionEnvPill.textContent = `Envision: ${env.name || key.toUpperCase()}${env.host ? ` - ${env.host}` : ""}`;
  }

  async function setEnvisionEnvironment(envKey) {
    if (!envisionEnvironmentUrl) return null;
    const resp = await fetch(envisionEnvironmentUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ environment: envKey }),
    });
    const data = await resp.json();
    if (!resp.ok || !data.ok) throw new Error(data.error || `Environment switch failed (${resp.status})`);
    activeEnvisionEnv = String(data.environment?.key || envKey || "base").toLowerCase();
    sessionStorage.setItem(ENV_STORAGE_KEY, activeEnvisionEnv);
    updateEnvisionEnvPill(data.environment);
    return data.environment || null;
  }

  function chooseEnvisionEnvironment() {
    return new Promise((resolve) => {
      if (!envPickerDialog || typeof envPickerDialog.showModal !== "function") {
        resolve(activeEnvisionEnv || "base");
        return;
      }

      const finish = (choice) => {
        if (envBaseBtn) envBaseBtn.removeEventListener("click", onBase);
        if (envTestBtn) envTestBtn.removeEventListener("click", onTest);
        if (envPickerDialog.open) envPickerDialog.close();
        resolve(choice);
      };
      const onBase = () => finish("base");
      const onTest = () => finish("test");

      if (envBaseBtn) envBaseBtn.addEventListener("click", onBase);
      if (envTestBtn) envTestBtn.addEventListener("click", onTest);
      envPickerDialog.showModal();
    });
  }

  async function ensureEnvisionEnvironment() {
    updateEnvisionEnvPill({
      key: activeEnvisionEnv,
      name: activeEnvisionEnv === "test" ? "TEST" : "BASE",
      host: (envisionEnvPill?.textContent || "").split(" - ").slice(1).join(" - "),
    });

    let preferred = (sessionStorage.getItem(ENV_STORAGE_KEY) || "").toLowerCase();
    if (preferred === "test" && !envisionTestAvailable) preferred = "base";
    if (!preferred) {
      preferred = await chooseEnvisionEnvironment();
    }
    if (!preferred) preferred = activeEnvisionEnv || "base";
    await setEnvisionEnvironment(preferred);
  }

  async function fetchEnvisionCrew(envisionFlightId) {
    if (!envisionFlightId || !envisionCrewUrl) return null;
    try {
      const resp = await fetch(`${envisionCrewUrl}?flight_id=${encodeURIComponent(envisionFlightId)}`, {
        headers: { Accept: "application/json" },
      });
      const data = await resp.json();
      if (!resp.ok || !data.ok) return null;
      return Array.isArray(data.crew) ? data.crew : [];
    } catch (err) {
      console.warn("Crew fetch failed", err);
      return null;
    }
  }

  function fmtDefectDate(value) {
    if (!value) return "-";
    const d = new Date(value);
    if (Number.isNaN(d.getTime())) return "-";
    return d.toLocaleString([], {
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      hour12: false,
      timeZone: getActiveTimeZone(),
    });
  }

  function parseEnvisionUtcDate(value) {
    if (!value) return null;
    if (value instanceof Date) {
      return Number.isNaN(value.getTime()) ? null : value;
    }
    let s = String(value).trim();
    if (!s) return null;
    // Envision fields are UTC; when timezone suffix is missing, force UTC.
    if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2}(\.\d{1,7})?)?$/.test(s)) {
      s += "Z";
    }
    const d = new Date(s);
    return Number.isNaN(d.getTime()) ? null : d;
  }

  function fmtDateTimeInSelectedTz(value) {
    const d = parseEnvisionUtcDate(value);
    if (!d) return "-";
    return d.toLocaleString([], {
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      hour12: false,
      timeZone: getActiveTimeZone(),
    });
  }

  function renderDefects(reg, regId, payload) {
    if (!defectsDialog || !defectsTitle || !defectsSummary || !defectsTbody) return;
    const defects = Array.isArray(payload?.defects) ? payload.defects : [];
    const openCount = Number(payload?.open_count || 0);
    const totalCount = Number(payload?.total_count || defects.length);

    defectsTitle.textContent = `Aircraft Defects - ${reg || "Unknown"}`;
    defectsSummary.textContent = `Registration ID: ${regId || "-"} | Open: ${openCount} | Total: ${totalCount}`;

    if (!defects.length) {
      defectsTbody.innerHTML = '<tr><td colspan="6" class="muted">No defects returned for this aircraft.</td></tr>';
      return;
    }

    defectsTbody.innerHTML = defects.map((d) => `
      <tr>
        <td>${escapeHtml(d.defectItemNo || d.defectSequenceNo || d.id || "-")}</td>
        <td>${escapeHtml(d.defectStatus || "-")}</td>
        <td>${escapeHtml(d.defect || "-")}</td>
        <td>${fmtDefectDate(d.openDate)}</td>
        <td>${fmtDefectDate(d.closeDate)}</td>
        <td>${escapeHtml(d.melReference || d.melCategory || d.melPriority || "-")}</td>
      </tr>
    `).join("");
  }

  async function openRegistrationDefects(reg, regId) {
    if (!defectsDialog || !envisionRegDefectsUrl) return;
    if (!regId) {
      alert("No Envision registration ID is available for this aircraft.");
      return;
    }

    const cacheKey = String(regId);
    let payload = regDefectsCache.get(cacheKey);
    if (!payload) {
      const resp = await fetch(`${envisionRegDefectsUrl}?registration_id=${encodeURIComponent(regId)}`, {
        headers: { Accept: "application/json" },
      });
      const data = await resp.json();
      if (!resp.ok || !data.ok) {
        throw new Error(data.error || `Defects fetch failed (${resp.status})`);
      }
      payload = data;
      regDefectsCache.set(cacheKey, data);
    }

    renderDefects(reg, regId, payload);
    defectsDialog.showModal();
  }

  function maintenanceMinute(item) {
    if (!item || typeof item !== "object") return null;
    const d = parseEnvisionUtcDate(
      item.plannedStartDate
      || item.actualStartDate
      || item.openDate
      || null,
    );
    return d ? minutesFromMidnight(d.toISOString()) : null;
  }

  function maintenanceTitle(item) {
    const workOrder = String(item.orderNo || item.workOrderNo || "").trim();
    const title = String(item.description || item.title || "Work Order").trim();
    const status = String(item.workOrderStatus || "").trim();
    const plannedStart = fmtDateTimeInSelectedTz(item.plannedStartDate);
    const plannedFinish = fmtDateTimeInSelectedTz(item.plannedFinishDate);
    const start = fmtDateTimeInSelectedTz(item.plannedStartDate || item.actualStartDate || item.openDate || null);
    const end = fmtDateTimeInSelectedTz(item.plannedFinishDate || item.actualFinishDate || item.closeDate || null);
    return [
      workOrder ? `WO ${workOrder}` : "",
      title,
      status,
      `Planned ${plannedStart} -> ${plannedFinish}`,
      `Window ${start} -> ${end}`,
    ]
      .filter(Boolean)
      .join(" | ");
  }

  function maintenanceRange(item) {
    if (!item || typeof item !== "object") return null;
    const startDate = parseEnvisionUtcDate(item.plannedStartDate || item.actualStartDate || item.openDate || null);
    const endDate = parseEnvisionUtcDate(item.plannedFinishDate || item.actualFinishDate || item.closeDate || null);
    let start = startDate ? minutesFromMidnight(startDate.toISOString()) : null;
    let end = endDate ? minutesFromMidnight(endDate.toISOString()) : null;
    if (start === null && end === null) return null;
    if (start === null && end !== null) start = end - 120;
    if (end === null && start !== null) end = start + 120;
    if (end !== null && start !== null && end <= start) end = start + 60;
    return { start, end };
  }

  function renderMaintenanceBars(track, topPx, items) {
    if (!showMaintenance || !track || !Array.isArray(items) || !items.length) return;
    const visibleItems = items
      .map((item) => ({ item, range: maintenanceRange(item) }))
      .filter((x) => x.range && rangeToPixels(x.range.start, x.range.end));
    const maxBars = Math.min(3, visibleItems.length);
    for (let i = 0; i < maxBars; i += 1) {
      const { item, range } = visibleItems[i];
      const px = rangeToPixels(range.start, range.end);
      if (!px) continue;
      const bar = document.createElement("div");
      bar.className = "maintenance-bar";
      bar.style.left = `${px.left}px`;
      bar.style.width = `${Math.max(18, px.width)}px`;
      bar.style.top = `${topPx + i * 22}px`;
      bar.title = maintenanceTitle(item);
      const title = String(item.description || item.title || "Work Order").trim();
      const wo = String(item.orderNo || item.workOrderNo || "").trim();
      bar.textContent = wo ? `${title} (${wo})` : title;
      track.appendChild(bar);
    }
    if (visibleItems.length > maxBars) {
      const more = document.createElement("div");
      more.className = "maintenance-more";
      more.style.top = `${topPx + maxBars * 22}px`;
      more.textContent = `+${visibleItems.length - maxBars} more maintenance item(s)`;
      more.title = visibleItems.slice(maxBars).map((x) => maintenanceTitle(x.item)).join("\n");
      track.appendChild(more);
    }
  }

  async function fetchRegistrationMaintenance(regId) {
    const key = String(regId || "");
    if (!key || !envisionRegMaintenanceUrl) return [];
    if (regMaintenanceCache.has(key)) return regMaintenanceCache.get(key) || [];
    const resp = await fetch(`${envisionRegMaintenanceUrl}?registration_id=${encodeURIComponent(key)}`, {
      headers: { Accept: "application/json" },
    });
    const js = await resp.json();
    if (!resp.ok || !js.ok) throw new Error(js.error || `Maintenance request failed (${resp.status})`);
    const items = Array.isArray(js.maintenance) ? js.maintenance : [];
    regMaintenanceCache.set(key, items);
    return items;
  }

  async function preloadRegistrationMaintenance() {
    if (!envisionRegMaintenanceUrl) return;
    const catalog = await fetchRegistrationCatalog();
    const ids = Array.from(new Set([
      ...flights
        .map((f) => Number(f.registration_id || 0))
        .filter((id) => Number.isFinite(id) && id > 0),
      ...catalog
        .map((c) => Number(c.regId || 0))
        .filter((id) => Number.isFinite(id) && id > 0),
    ]));
    const missing = ids.filter((id) => !regMaintenanceCache.has(String(id)));
    if (!missing.length) return;
    await Promise.all(missing.map(async (regId) => {
      try {
        await fetchRegistrationMaintenance(regId);
      } catch (_err) {
        regMaintenanceCache.set(String(regId), []);
      }
    }));
    if (showMaintenance) renderRows();
  }

  function renderCrewInDetail(f, crew) {
    const host = detailList.querySelector("#detailCrewBody");
    if (!host) return;
    if (!Array.isArray(crew) || !crew.length) {
      host.innerHTML = '<div class="muted">No operating crew found in Envision.</div>';
      return;
    }
    host.innerHTML = crew.map((c) => {
      const role = escapeHtml(c.position || c.role || c.crewPosition || "Crew");
      const name = escapeHtml(c.name || c.fullName || c.employeeName || "-");
      return `<div class="kv"><span>${role}</span><strong>${name}</strong></div>`;
    }).join("");
  }

  async function populateDetailCrew(f) {
    const host = detailList.querySelector("#detailCrewBody");
    if (!host) return;
    if (!f || !f.envision_flight_id) {
      host.innerHTML = '<div class="muted">No Envision flight ID linked - crew not available.</div>';
      return;
    }
    if (f.crewLoaded) {
      renderCrewInDetail(f, f.crew || []);
      return;
    }
    host.innerHTML = '<div class="muted">Loading crew from Envision...</div>';
    const crew = await fetchEnvisionCrew(f.envision_flight_id);
    if (String(selectedId) !== String(f.envision_flight_id)) return;
    if (crew === null) {
      host.innerHTML = '<div class="muted">Unable to load crew right now.</div>';
      return;
    }
    f.crewLoaded = true;
    f.crew = crew;
    renderCrewInDetail(f, f.crew);
  }

  function setDetail(f) {
    selectedFlight = f || null;
    if (!f) {
      detailMuted.style.display = "";
      detailList.innerHTML = "";
      setActionsEnabled(false);
      return;
    }
    detailMuted.style.display = "none";
    setActionsEnabled(true);
    const pax = Array.isArray(f.pax_list) ? f.pax_list : [];
    const booked = pax.filter((p) => classifyPaxStatus(p) === "BOOKED").length;
    const checked = pax.filter((p) => classifyPaxStatus(p) === "CHECKED").length;
    const boarded = pax.filter((p) => classifyPaxStatus(p) === "BOARDED").length;
    const flown = pax.filter((p) => classifyPaxStatus(p) === "FLOWN").length;
    const total = pax.length || Number(f.pax_count || 0);
    const pt = paxTypeBreakdown(pax);
    const delays = normalizeDelays(f.delays);
    const delayRowsHtml = delays.length
      ? delays.map((d, idx) => `
          <div class="delay-row">
            <span class="delay-row-code">${escapeHtml(d.code || `#${idx + 1}`)}</span>
            <span class="delay-row-mins">${Number.isFinite(d.mins) ? `${d.mins}m` : "-"}</span>
            <span class="delay-row-remark">${escapeHtml(d.remarks || "No remark")}</span>
          </div>
        `).join("")
      : '<div class="muted">No delay remarks available.</div>';
    const bagKg = Number(f.bags_kg || 0).toFixed(1);
    const defectCount = Number.isFinite(Number(f.defect_count)) ? Number(f.defect_count) : 0;
    const defectTotal = Number.isFinite(Number(f.defect_total)) ? Number(f.defect_total) : defectCount;

    const apgLinkedHtml = f.apg_plan_id
      ? `<a class="apg-route-link" href="https://fly.rocketroute.com/route/${f.apg_plan_id}" target="_blank" rel="noopener noreferrer" title="Open APG route ${f.apg_plan_id}">APG Linked</a>`
      : `<span title="No APG plan">No APG</span>`;
    const dcsLinked = Boolean(f.dcs_linked);

    detailList.innerHTML = `
      <div class="detail-grid">
        <div class="detail-card">
          <div class="card-title">Flight</div>
          <div class="card-main">${flightCode(f)}</div>
          <div class="card-sub">${f.dep || ""} -> ${f.ades || ""} | ${f.reg || "-"}</div>
          <div class="kv"><span>Status</span><strong>${f.flight_status || "-"}</strong></div>
          <div class="kv"><span>Defects</span><strong title="Open defects ${defectCount}, total records ${defectTotal}">${defectCount} open</strong></div>
          <div class="kv">
            <span>Links</span>
            <strong>
              <span title="DCS ${dcsLinked ? "linked" : "not linked"}">${dcsLinked ? "DCS Linked" : "No DCS"}</span>
              |
              ${apgLinkedHtml}
            </strong>
          </div>
        </div>

        <div class="detail-card">
          <div class="card-title">Times</div>
          <div class="kv"><span>STD/STA</span><strong>${fmtTime(f.std_sched_nz)} / ${fmtTime(f.sta_sched_nz)}</strong></div>
          <div class="kv"><span>ETD/ETA</span><strong>${fmtTime(f.std_nz)} / ${fmtTime(f.sta_nz)}</strong></div>
          <div class="card-sub" style="margin-top:8px;">Delays</div>
          <div class="delay-detail-list">${delayRowsHtml}</div>
        </div>

        <div class="detail-card">
          <div class="card-title">Passengers</div>
          <div class="kv"><span>Booked</span><strong>${booked}</strong></div>
          <div class="kv"><span>Checked-in</span><strong>${checked}</strong></div>
          <div class="kv"><span>Boarded</span><strong>${boarded}</strong></div>
          <div class="kv"><span>Flown</span><strong>${flown}</strong></div>
          <div class="kv"><span>Total</span><strong>${total}</strong></div>
          <div class="card-sub">AD ${pt.ad} / CHD ${pt.chd} / INF ${pt.inf}</div>
          <div class="kv"><span>Baggage</span><strong>${bagKg} kg</strong></div>
        </div>

        <div class="detail-card">
          <div class="card-title">Crew</div>
          <div id="detailCrewBody" class="detail-crew-body"></div>
        </div>
      </div>
    `;
    populateDetailCrew(f);
  }

  function renderFlightGroup(track, cfg) {
    const {
      f,
      groupLeft,
      groupWidth,
      barLeft,
      barWidth,
      schedLeft,
      schedWidth,
      laneTop,
      showDep,
      showArr,
      isSelected,
      isCancelled,
    } = cfg;

    const group = document.createElement("div");
    group.className = "flight-group";
    group.style.left = `${groupLeft}px`;
    group.style.width = `${groupWidth}px`;
    group.style.top = `${laneTop}px`;

    const routeLayer = document.createElement("div");
    routeLayer.className = "flight-route-layer";
    if (showDep) {
      const depEl = document.createElement("span");
      depEl.className = "flight-route-tag dep";
      depEl.textContent = (f.dep || "").toUpperCase();
      routeLayer.appendChild(depEl);
    }
    if (showArr) {
      const arrEl = document.createElement("span");
      arrEl.className = "flight-route-tag arr";
      arrEl.textContent = (f.ades || "").toUpperCase();
      routeLayer.appendChild(arrEl);
    }
    group.appendChild(routeLayer);

    if (schedLeft !== null) {
      const schedBar = document.createElement("div");
      schedBar.className = "flight-stdsta-bar";
      schedBar.style.left = `${schedLeft - groupLeft}px`;
      schedBar.style.width = `${schedWidth}px`;
      group.appendChild(schedBar);
    }

    const bar = document.createElement("div");
    bar.className = "bar flight-main-bar";
    if (!f.apg_plan_id && !isCancelled) bar.classList.add("apg-missing");
    if (isSelected) bar.classList.add("selected");
    bar.style.left = `${barLeft - groupLeft}px`;
    bar.style.width = `${barWidth}px`;
    bar.style.background = isCancelled ? "#d73a49" : barColor(f.flight_status);
    bar.textContent = flightCode(f);
    const mainStartIso = flightMainStartIso(f);
    const mainEndIso = flightMainEndIso(f);
    bar.title = isCancelled
      ? `Cancelled | ${fmtTime(f.std_nz)} ${f.dep || ""}-${f.ades || ""}`
      : `${fmtTime(mainStartIso)}-${fmtTime(mainEndIso)} ${f.dep || ""}-${f.ades || ""} | ${f.flight_status || "Unknown"} | Pax ${f.pax_count || 0}`;
    bar.addEventListener("click", () => {
      selectedId = f.envision_flight_id;
      setDetail(f);
      renderRows();
    });
    bar.addEventListener("dblclick", (ev) => {
      ev.preventDefault();
      ev.stopPropagation();
      openModifyLegDialog(f);
    });
    group.appendChild(bar);

    track.appendChild(group);
  }
  function renderRows() {
    rowsEl.innerHTML = "";
    const sorted = [...flights].sort((a, b) => {
      const am = flightStartMinutes(a) ?? 9999;
      const bm = flightStartMinutes(b) ?? 9999;
      return am - bm;
    });

    const grouped = new Map();
    const cancelledFlights = [];
    for (const f of sorted) {
      if (isCancelledFlight(f)) {
        cancelledFlights.push(f);
        continue;
      }
      const regKey = (f.reg || "Unknown").trim() || "Unknown";
      if (!grouped.has(regKey)) grouped.set(regKey, []);
      grouped.get(regKey).push(f);
    }

    const rowsByReg = Array.from(grouped.entries()).map(([reg, regFlights]) => {
      const regId = Number(regFlights.find((rf) => rf.registration_id)?.registration_id || 0) || null;
      return { reg, regId, regFlights, maintenanceOnly: false };
    });

    if (showMaintenance && Array.isArray(registrationCatalogCache)) {
      const existing = new Set(rowsByReg.map((r) => String(r.reg || "").toUpperCase()));
      for (const c of registrationCatalogCache) {
        const key = String(c.reg || "").toUpperCase();
        if (!key || existing.has(key)) continue;
        const maint = regMaintenanceCache.get(String(c.regId)) || [];
        if (!Array.isArray(maint) || !maint.length) continue;
        rowsByReg.push({
          reg: c.reg,
          regId: c.regId,
          regFlights: [],
          maintenanceOnly: true,
        });
      }
    }

    rowsByReg.sort((a, b) => {
      const af = a.regFlights[0];
      const bf = b.regFlights[0];
      const am = af ? (flightStartMinutes(af) ?? 9999) : 9999;
      const bm = bf ? (flightStartMinutes(bf) ?? 9999) : 9999;
      if (am !== bm) return am - bm;
      return String(a.reg || "").localeCompare(String(b.reg || ""));
    });

    if (!rowsByReg.length) {
      rowsEl.innerHTML = '<div class="row"><div class="row-label"><div class="meta">No flights or maintenance found for selected date.</div></div><div class="track"></div></div>';
      return;
    }

    const timelinePx = Math.max(1, (windowEndMin - windowStartMin) * pxPerMinute);
    for (const rowData of rowsByReg) {
      const reg = rowData.reg;
      const regFlights = Array.isArray(rowData.regFlights) ? rowData.regFlights : [];
      const row = document.createElement("div");
      row.className = "row";

      const label = document.createElement("div");
      label.className = "row-label";
      const regDefectsOpen = regFlights.reduce((maxCount, rf) => {
        const val = Number(rf.defect_count);
        return Number.isFinite(val) ? Math.max(maxCount, val) : maxCount;
      }, 0);
      const regDefectsTotal = regFlights.reduce((maxCount, rf) => {
        const val = Number(rf.defect_total);
        return Number.isFinite(val) ? Math.max(maxCount, val) : maxCount;
      }, regDefectsOpen);
      const regId = rowData.regId || regFlights.find((rf) => rf.registration_id)?.registration_id || "";
      const regMaint = showMaintenance && regId
        ? (regMaintenanceCache.get(String(regId)) || []).filter((m) => {
          const rng = maintenanceRange(m);
          return !!(rng && rangeToPixels(rng.start, rng.end));
        })
        : [];
      label.innerHTML = `
        <button class="reg-link" type="button" title="View defects for ${reg}">${reg}</button>
        <div class="meta">${regFlights.length} flight${regFlights.length === 1 ? "" : "s"}${rowData.maintenanceOnly ? " | Maintenance only" : ""}</div>
        <div class="meta" title="Open defects ${regDefectsOpen}, total records ${regDefectsTotal}">Defects: ${regDefectsOpen}</div>
        <div class="meta" title="Scheduled maintenance due today">Mx: ${regMaint.length}</div>
      `;
      const regLink = label.querySelector(".reg-link");
      if (regLink) {
        regLink.addEventListener("click", async () => {
          try {
            await openRegistrationDefects(reg, regId);
          } catch (err) {
            alert(err.message || "Unable to load defects.");
          }
        });
      }

      const track = document.createElement("div");
      track.className = "track";
      const packed = assignLanes(regFlights);
      const laneHeight = 58;
      const maintenanceRows = showMaintenance && regMaint.length ? Math.min(3, regMaint.length) + (regMaint.length > 3 ? 1 : 0) : 0;
      const maintenanceHeight = maintenanceRows ? (maintenanceRows * 22 + 8) : 0;
      const flightLanes = regFlights.length ? packed.laneCount : 0;
      const rowHeight = Math.max(52, 18 + flightLanes * laneHeight + maintenanceHeight);
      row.style.minHeight = `${rowHeight}px`;
      track.style.minHeight = `${rowHeight}px`;
      if (maintenanceRows) {
        const maintenanceTop = 12 + flightLanes * laneHeight;
        renderMaintenanceBars(track, maintenanceTop, regMaint);
      }

      for (let i = 0; i < packed.items.length; i += 1) {
        const laneItem = packed.items[i];
        const f = laneItem.f;
        const start = laneItem.start;
        const end = laneItem.end;
        if (start === null) continue;
        const lane = laneItem.lane;

        const schedStart = minutesFromMidnight(f.std_sched_nz);
        const schedEnd = minutesFromMidnight(f.sta_sched_nz);

        const rightMin = end !== null ? end : start + 45;
        const barRange = rangeToPixels(start, rightMin);
        if (!barRange) continue;
        const left = barRange.left;
        const width = Math.max(28, barRange.width);
        const laneTop = 12 + lane * laneHeight;
        let schedLeft = null;
        let schedWidth = null;
        if (schedStart !== null) {
          const schedRightMin = schedEnd !== null ? schedEnd : schedStart + 45;
          const schedRange = rangeToPixels(schedStart, schedRightMin);
          if (schedRange) {
            // Keep scheduled strip readable at low zoom using the same
            // minimum-visible logic as the main flight bar.
            const minVisibleWidth = Math.max(28, width);
            if (schedRange.width >= minVisibleWidth) {
              schedLeft = schedRange.left;
              schedWidth = schedRange.width;
            } else {
              // Preserve scheduled center while expanding to readable width.
              const schedMid = schedRange.left + (schedRange.width / 2);
              schedWidth = minVisibleWidth;
              schedLeft = schedMid - (schedWidth / 2);
              const maxLeft = Math.max(0, timelinePx - schedWidth);
              schedLeft = Math.max(0, Math.min(maxLeft, schedLeft));
            }
          }
        }

        const prev = i > 0 ? packed.items[i - 1].f : null;
        const dep = (f.dep || "").toUpperCase();
        const prevArr = (prev?.ades || "").toUpperCase();
        const prevEnd = prev ? flightEndMinutes(prev) : null;
        const prevGap = (prevEnd !== null && start !== null) ? (start - prevEnd) : null;
        // Always show destination for each sector.
        // Suppress origin only when it's the same station as previous destination
        // and turnaround gap is short (<= 3h), to avoid duplicated middle tags.
        const showDep = !prev || dep !== prevArr || (prevGap !== null && prevGap > 180);
        const showArr = true;

        const contentLeft = Math.min(left, schedLeft !== null ? schedLeft : left);
        const contentRight = Math.max(left + width, schedLeft !== null ? (schedLeft + schedWidth) : (left + width));
        let groupLeft = contentLeft - airportLabelPadPx;
        let groupRight = contentRight + airportLabelPadPx;
        if (groupRight - groupLeft < minAirportLabelSpanPx) {
          const mid = (groupLeft + groupRight) / 2;
          groupLeft = mid - minAirportLabelSpanPx / 2;
          groupRight = mid + minAirportLabelSpanPx / 2;
        }
        if (groupLeft < 0) {
          groupRight += -groupLeft;
          groupLeft = 0;
        }
        if (groupRight > timelinePx) {
          const over = groupRight - timelinePx;
          groupLeft = Math.max(0, groupLeft - over);
          groupRight = timelinePx;
        }
        renderFlightGroup(track, {
          f,
          groupLeft,
          groupWidth: Math.max(28, groupRight - groupLeft),
          barLeft: left,
          barWidth: width,
          schedLeft,
          schedWidth,
          laneTop,
          showDep,
          showArr,
          isSelected: String(f.envision_flight_id) === String(selectedId),
          isCancelled: false,
        });
      }

      row.appendChild(label);
      row.appendChild(track);
      rowsEl.appendChild(row);
    }

    if (cancelledFlights.length) {
      const row = document.createElement("div");
      row.className = "row";
      const label = document.createElement("div");
      label.className = "row-label";
      label.innerHTML = `
        <div class="fno">CANCELLED</div>
        <div class="meta">${cancelledFlights.length} flight${cancelledFlights.length === 1 ? "" : "s"}</div>
      `;
      const track = document.createElement("div");
      track.className = "track";
      const packedCancelled = assignLanes(cancelledFlights);
      const laneHeight = 58;
      const rowHeight = 18 + packedCancelled.laneCount * laneHeight;
      row.style.minHeight = `${rowHeight}px`;
      track.style.minHeight = `${rowHeight}px`;

      for (let i = 0; i < packedCancelled.items.length; i += 1) {
        const laneItem = packedCancelled.items[i];
        const f = laneItem.f;
        const start = laneItem.start;
        const end = laneItem.end;
        if (start === null) continue;
        const lane = laneItem.lane;
        const rightMin = end !== null ? end : start + 45;
        const barRange = rangeToPixels(start, rightMin);
        if (!barRange) continue;
        const left = barRange.left;
        const width = Math.max(28, barRange.width);
        const laneTop = 12 + lane * laneHeight;
        let groupLeft = left - airportLabelPadPx;
        let groupRight = left + width + airportLabelPadPx;
        if (groupRight - groupLeft < minAirportLabelSpanPx) {
          const mid = (groupLeft + groupRight) / 2;
          groupLeft = mid - minAirportLabelSpanPx / 2;
          groupRight = mid + minAirportLabelSpanPx / 2;
        }
        if (groupLeft < 0) {
          groupRight += -groupLeft;
          groupLeft = 0;
        }
        if (groupRight > timelinePx) {
          const over = groupRight - timelinePx;
          groupLeft = Math.max(0, groupLeft - over);
          groupRight = timelinePx;
        }
        renderFlightGroup(track, {
          f,
          groupLeft,
          groupWidth: Math.max(28, groupRight - groupLeft),
          barLeft: left,
          barWidth: width,
          schedLeft: null,
          schedWidth: null,
          laneTop,
          showDep: true,
          showArr: true,
          isSelected: String(f.envision_flight_id) === String(selectedId),
          isCancelled: true,
        });
      }

      row.appendChild(label);
      row.appendChild(track);
      rowsEl.appendChild(row);
    }
  }

  function updateStats() {
    statFlights.textContent = String(flights.length);
    statPax.textContent = String(flights.reduce((n, f) => n + Number(f.pax_count || 0), 0));
    statNoApg.textContent = String(flights.reduce((n, f) => n + (f.apg_plan_id ? 0 : 1), 0));
    statBags.textContent = String(Math.round(flights.reduce((n, f) => n + Number(f.bags_kg || 0), 0)));
  }

  async function loadData(opts = {}) {
    const showSpinner = opts.showSpinner !== false;
    const day = dayInput.value || app.dataset.day;
    const u = `${apiUrl}?date=${encodeURIComponent(day)}&include_delays=1`;
    refreshBtn.disabled = true;
    if (showSpinner) setBoardLoading(true);
    try {
      regDefectsCache.clear();
      const resp = await fetch(u, { headers: { Accept: "application/json" } });
      const data = await resp.json();
      const maybeRows = data.results || data.rows || data.data || [];
      flights = Array.isArray(maybeRows) ? maybeRows : [];
      updateTimeWindowFromFlights();
      if (!hasUserZoom) fitPxPerMinuteToViewport();
      applyTimelineScale();
      buildAxis();
      if (!resp.ok || data.ok === false) {
        showMessage(data.error || `Request failed (${resp.status})`, true);
      } else if (!flights.length) {
        showMessage("No flights returned for this date.", false);
      } else {
        showMessage("", false);
      }
      updateStats();
      renderRows();
      preloadRegistrationMaintenance().catch(() => {});
      updateLiveNowBar();
      const refreshedSelected = findSelectedFlight();
      if (refreshedSelected) {
        setDetail(refreshedSelected);
      } else if (!selectedId) {
        setDetail(null);
      }
    } catch (err) {
      console.error("New Live Gantt load failed", err);
      showMessage(`Failed to load flights: ${err.message}`, true);
    } finally {
      refreshBtn.disabled = false;
      if (showSpinner) setBoardLoading(false);
    }
  }

  function buildManifestPayload(f) {
    const rawNo = String(f.flight_number || "").toUpperCase().replace(/\s+/g, "");
    const numberOnly = (rawNo.match(/(\d+)$/) || [null, rawNo])[1] || rawNo;
    return {
      dep: (f.dep || "").toUpperCase(),
      ades: (f.ades || "").toUpperCase(),
      date: dayInput.value || app.dataset.day,
      designator: (f.designator || "").toUpperCase(),
      number: numberOnly,
      reg: (f.reg || "").toUpperCase(),
      envision_flight_id: f.envision_flight_id || null,
      pax_list: f.pax_list || [],
      status_mode: "exclude_booked",
    };
  }

  async function previewManifest() {
    const f = selectedFlight;
    if (!f) return;
    const resp = await fetch(manifestPreviewUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(buildManifestPayload(f)),
    });
    if (!resp.ok) throw new Error(await resp.text());
    const data = await resp.json();
    if (!data.ok || !data.html) throw new Error(data.error || "Manifest preview failed");
    const w = window.open("", "_blank");
    w.document.open();
    w.document.write(data.html);
    w.document.close();
  }

  async function submitToApg() {
    const f = selectedFlight;
    if (!f) return;
    if (!f.apg_plan_id) {
      alert("No APG plan linked for this flight.");
      return;
    }
    const rawNo = String(f.flight_number || "").toUpperCase().replace(/\s+/g, "");
    const numberOnly = (rawNo.match(/(\d+)$/) || [null, rawNo])[1] || rawNo;
    const payload = {
      apg_plan_id: Number(f.apg_plan_id),
      dep: (f.dep || "").toUpperCase(),
      ades: (f.ades || "").toUpperCase(),
      reg: (f.reg || "").toUpperCase(),
      date: dayInput.value || app.dataset.day,
      designator: (f.designator || "").toUpperCase(),
      flight_number: numberOnly,
      envision_flight_id: f.envision_flight_id || null,
      preview_only: false,
      pax_list: f.pax_list || [],
    };
    const resp = await fetch(apgPushUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await resp.json();
    if (!resp.ok || !data.ok) throw new Error(data.error || `Submit failed (${resp.status})`);
    const modalLines = [
      `Flight ${payload.designator}${payload.flight_number} submitted to APG route ${payload.apg_plan_id}.`,
    ];
    if (data.plan_version !== undefined && data.plan_version !== null) {
      modalLines.push(`APG plan version: ${data.plan_version}`);
    }
    if (data.manifest_uploaded) {
      const manifestVersion = data.manifest_version ? ` v${data.manifest_version}` : "";
      modalLines.push(`Manifest uploaded${manifestVersion} (doc_id: ${data.manifest_doc_id || "n/a"}).`);
    } else if (data.manifest_error) {
      modalLines.push(`Manifest upload issue: ${data.manifest_error}`);
    }
    showApgStatusModal("Submitted to APG", modalLines);
  }

  async function resetApgPassengers() {
    const f = selectedFlight;
    if (!f) return;
    if (!f.apg_plan_id) {
      alert("No APG plan linked for this flight.");
      return;
    }
    const resp = await fetch(apgResetUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ apg_plan_id: Number(f.apg_plan_id), preview_only: false }),
    });
    const data = await resp.json();
    if (!resp.ok || !data.ok) throw new Error(data.error || `Reset failed (${resp.status})`);
    alert("APG passenger lines reset.");
    await loadData();
  }

  async function runPassengerSyncTest() {
    if (!envisionPaxSyncUrl) throw new Error("Passenger sync endpoint not configured.");
    const resp = await fetch(envisionPaxSyncUrl, {
      method: "POST",
      headers: { Accept: "application/json" },
    });
    const js = await resp.json();
    if (!resp.ok || !js.ok) throw new Error(js.error || `Passenger sync failed (${resp.status})`);
    const updated = Number(js.updated || 0);
    const failed = Number(js.failed || 0);
    alert(`Passenger sync complete. Updated: ${updated}, Failed: ${failed}`);
    await loadData({ showSpinner: false });
  }

  const SSR_SPECIAL = new Set(["CKIN", "DEAF", "PETC", "VVIP", "UNZZ", "UMNR", "WEAP", "WEP"]);

  function hasUmnrSSR(ssrs) {
    return Array.isArray(ssrs) && ssrs.some((s) => {
      const code = String(s.Code || s.code || "").toUpperCase();
      return code === "UMNR" || code === "UM";
    });
  }

  function hasWheelchairSSR(ssrs) {
    return Array.isArray(ssrs) && ssrs.some((s) => String(s.Code || s.code || "").toUpperCase().startsWith("WC"));
  }

  function hasSpecialSSR(ssrs) {
    return Array.isArray(ssrs) && ssrs.some((s) => SSR_SPECIAL.has(String(s.Code || s.code || "").toUpperCase()));
  }

  function hasCkinInfoSSR(ssrs) {
    return Array.isArray(ssrs) && ssrs.some((s) => {
      const code = String(s.Code || s.code || "").toUpperCase();
      const txt = String(s.FreeText || s.freeText || "").toUpperCase();
      return code === "CKIN" || txt.includes("CKIN");
    });
  }

  function hasInfantSSR(ssrs) {
    return Array.isArray(ssrs) && ssrs.some((s) => {
      const code = String(s.Code || s.code || "").toUpperCase();
      return code === "INFT" || code === "INF";
    });
  }

  function hasWeaponSSR(ssrs) {
    return Array.isArray(ssrs) && ssrs.some((s) => {
      const code = String(s.Code || s.code || "").toUpperCase();
      return code === "WEAP" || code === "WEP";
    });
  }

  function seatmapConfigForFlight(f) {
    const reg = String(f.reg || "").toUpperCase();
    const type = String(f.aircraft_type || "").toUpperCase();
    if (type.includes("ATR") || reg.startsWith("ZK-MC")) {
      return { rows: Array.from({ length: 17 }, (_, i) => i + 1), left: ["A", "B"], right: ["C", "D"], name: `ATR 72 (${reg || "Unknown"})` };
    }
    if (type.includes("SAAB") || type.includes("SF3") || type.includes("SF340") || reg.startsWith("ZK-CI") || reg.startsWith("ZK-KR")) {
      return { rows: Array.from({ length: 11 }, (_, i) => i + 1), left: ["A"], right: ["B", "C"], name: `Saab 340 (${reg || "Unknown"})` };
    }
    return null;
  }

  function openSeatmap() {
    const f = selectedFlight;
    if (!f || !seatmapDialog) return;
    const cfg = seatmapConfigForFlight(f);
    seatmapGrid.innerHTML = "";
    if (!cfg) {
      seatmapTitle.textContent = `Seatmap ${f.reg || ""}`;
      seatmapInfo.textContent = "Seatmap not available for this aircraft type.";
      seatmapDialog.showModal();
      return;
    }
    seatmapTitle.textContent = cfg.name;

    const paxBySeat = {};
    (f.pax_list || []).forEach((p) => {
      const seat = String(p.Seat || p.SeatNumber || p.SeatNo || "").toUpperCase().trim();
      if (seat) paxBySeat[seat] = p;
    });

    function renderSeatCell(rowNum, col) {
      const code = `${rowNum}${col}`;
      const el = document.createElement("div");
      el.className = "seat";
      const pax = paxBySeat[code];
      el.textContent = col;
      if (!pax) {
        el.classList.add("seat-empty");
      } else {
        const ssrs = Array.isArray(pax.Ssrs) ? pax.Ssrs : [];
        const paxType = String(pax.PassengerType || pax.passengerType || "").toUpperCase();
        const isUmnr = hasUmnrSSR(ssrs);
        if (isUmnr || paxType === "CH" || paxType === "CHD") el.classList.add("seat-child");
        else if (hasInfantSSR(ssrs) || paxType === "INF") el.classList.add("seat-adult-infant");
        else el.classList.add("seat-adult");

        if (isUmnr) el.classList.add("seat-umnr");
        if (hasCkinInfoSSR(ssrs)) el.classList.add("seat-ckin-info");

        const icons = document.createElement("div");
        icons.className = "seat-icons";
        if (isUmnr) {
          const um = document.createElement("span");
          um.className = "seat-icon seat-icon-umnr";
          um.textContent = "U";
          icons.appendChild(um);
        }
        if (hasSpecialSSR(ssrs)) {
          const sp = document.createElement("span");
          sp.className = "seat-icon seat-icon-special";
          sp.textContent = "!";
          icons.appendChild(sp);
        }
        if (hasCkinInfoSSR(ssrs)) {
          const info = document.createElement("span");
          info.className = "seat-icon seat-icon-info";
          info.textContent = "ℹ";
          icons.appendChild(info);
        }
        if (hasWeaponSSR(ssrs)) {
          const wp = document.createElement("span");
          wp.className = "seat-icon seat-icon-weapon";
          wp.textContent = "🔫";
          icons.appendChild(wp);
        }
        if (hasWheelchairSSR(ssrs)) {
          const wc = document.createElement("span");
          wc.className = "seat-icon seat-icon-wheelchair";
          wc.textContent = "♿";
          icons.appendChild(wc);
        }
        if (icons.children.length) el.appendChild(icons);
      }

      el.addEventListener("click", () => {
        seatmapGrid.querySelectorAll(".seat.selected").forEach((n) => n.classList.remove("selected"));
        el.classList.add("selected");
        if (!pax) {
          seatmapInfo.innerHTML = `<div><strong>${code}</strong></div><div>Empty seat</div>`;
          return;
        }
        const name = `${pax.NamePrefix || ""} ${pax.GivenName || ""} ${pax.Surname || ""}`.replace(/\s+/g, " ").trim();
        const pnr = pax.BookingReferenceID || "-";
        const status = pax.Status || "-";
        const ssr = ssrText(pax) || "—";
        seatmapInfo.innerHTML = `
          <div><strong>${code}</strong></div>
          <div>${name || "-"}</div>
          <div>PNR: ${pnr}</div>
          <div>Status: ${status}</div>
          <div>SSR: ${ssr}</div>
        `;
      });
      return el;
    }

    cfg.rows.forEach((rowNum) => {
      const row = document.createElement("div");
      row.className = "seat-row";
      row.innerHTML = `<div class="seat-label">${rowNum}</div>`;
      const left = document.createElement("div");
      left.className = "seat-block";
      cfg.left.forEach((c) => left.appendChild(renderSeatCell(rowNum, c)));
      row.appendChild(left);
      const aisle = document.createElement("div");
      aisle.className = "seat-aisle";
      row.appendChild(aisle);
      const right = document.createElement("div");
      right.className = "seat-block";
      cfg.right.forEach((c) => right.appendChild(renderSeatCell(rowNum, c)));
      row.appendChild(right);
      seatmapGrid.appendChild(row);
    });

    seatmapInfo.textContent = "Select a seat to view passenger details.";
    seatmapDialog.showModal();
  }

  function ssrText(p) {
    const list = Array.isArray(p.Ssrs) ? p.Ssrs : [];
    if (!list.length) return "";
    return list.map((s) => {
      const code = s.Code || "";
      const free = s.FreeText || "";
      return free ? `${code} (${free})` : code;
    }).join(", ");
  }

  function paxName(p) {
    return `${p.NamePrefix || ""} ${p.GivenName || ""} ${p.Surname || ""}`.replace(/\s+/g, " ").trim();
  }

  function seatSortValue(seat) {
    const s = String(seat || "").toUpperCase().trim();
    const m = s.match(/^(\d+)([A-Z])?$/);
    if (!m) return 99999;
    const row = Number(m[1]);
    const col = (m[2] || "Z").charCodeAt(0) - 65;
    return row * 100 + col;
  }

  function openPassengerList() {
    const f = selectedFlight;
    if (!f || !paxDialog || !paxTbody) return;
    const code = flightCode(f);
    paxTitle.textContent = `Passenger List - ${code} ${f.dep || ""}-${f.ades || ""}`;
    const pax = Array.isArray(f.pax_list) ? [...f.pax_list] : [];
    pax.sort((a, b) => seatSortValue(a.Seat || a.SeatNumber) - seatSortValue(b.Seat || b.SeatNumber));
    paxTbody.innerHTML = "";
    if (!pax.length) {
      paxTbody.innerHTML = '<tr><td colspan="8">No passengers found for this flight.</td></tr>';
      paxDialog.showModal();
      return;
    }
    for (const p of pax) {
      const porg = String(p.__manifest_origin || f.dep || "").toUpperCase();
      const pdst = String(p.__manifest_dest || f.ades || "").toUpperCase();
      const proute = `${porg || "-"}-${pdst || "-"}`;
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${p.Seat || p.SeatNumber || ""}</td>
        <td>${paxName(p)}</td>
        <td>${proute}</td>
        <td>${p.PassengerType || ""}</td>
        <td>${p.Status || ""}</td>
        <td>${p.BaggageWeight || 0}</td>
        <td>${p.BookingReferenceID || ""}</td>
        <td>${ssrText(p)}</td>
      `;
      paxTbody.appendChild(tr);
    }
    paxDialog.showModal();
  }

  function applyTheme(theme) {
    document.body.setAttribute("data-theme", theme);
    themeBtn.textContent = theme === "dark" ? "Light Mode" : "Dark Mode";
    localStorage.setItem("new_gantt_theme", theme);
  }

  function setupAutoRefresh() {
    if (timer) clearInterval(timer);
    if (autoRefresh.checked) timer = setInterval(() => loadData({ showSpinner: false }), 60000);
  }

  function setupLiveNowTimer() {
    if (liveNowTimer) clearInterval(liveNowTimer);
    liveNowTimer = setInterval(updateLiveNowBar, 30000);
  }

  rowsEl.addEventListener("scroll", () => {
    if (axisScrollEl) axisScrollEl.scrollLeft = rowsEl.scrollLeft;
    updateLiveNowBar();
  });

  rowsEl.addEventListener("mousemove", (ev) => {
    if (!crosshairToggle || !crosshairToggle.checked || !boardEl || !crosshairV || !crosshairH) return;
    const boardRect = boardEl.getBoundingClientRect();
    const labelWidth = getRowLabelWidth();
    const x = ev.clientX - boardRect.left;
    const y = ev.clientY - boardRect.top;
    if (x < labelWidth || y < 0 || y > boardRect.height) {
      hideCrosshairs();
      return;
    }
    crosshairV.style.left = `${x}px`;
    crosshairH.style.top = `${y}px`;
    crosshairH.style.left = `${labelWidth}px`;
    crosshairV.hidden = false;
    crosshairH.hidden = false;
  });

  rowsEl.addEventListener("mouseleave", hideCrosshairs);
  if (boardEl) boardEl.addEventListener("mouseleave", hideCrosshairs);

  refreshBtn.addEventListener("click", () => loadData({ showSpinner: true }));
  if (zoomInBtn) {
    zoomInBtn.addEventListener("click", () => {
      hasUserZoom = true;
      pxPerMinute = Math.min(4, +(pxPerMinute + 0.2).toFixed(2));
      applyTimelineScale();
      buildAxis();
      renderRows();
      updateLiveNowBar();
    });
  }
  if (zoomOutBtn) {
    zoomOutBtn.addEventListener("click", () => {
      hasUserZoom = true;
      pxPerMinute = Math.max(0.7, +(pxPerMinute - 0.2).toFixed(2));
      applyTimelineScale();
      buildAxis();
      renderRows();
      updateLiveNowBar();
    });
  }
  dayInput.addEventListener("change", () => {
    hasUserZoom = false;
    loadData({ showSpinner: true });
  });
  if (tzSelect) {
    tzSelect.addEventListener("change", () => {
      currentTimeZone = tzSelect.value || "Pacific/Auckland";
      updateTimeWindowFromFlights();
      if (!hasUserZoom) fitPxPerMinuteToViewport();
      applyTimelineScale();
      buildAxis();
      renderRows();
      setDetail(findSelectedFlight());
      updateLiveNowBar();
      localStorage.setItem("new_gantt_tz", currentTimeZone);
    });
  }
  autoRefresh.addEventListener("change", setupAutoRefresh);
  if (crosshairToggle) {
    crosshairToggle.addEventListener("change", () => {
      localStorage.setItem("new_gantt_crosshair", crosshairToggle.checked ? "1" : "0");
      if (!crosshairToggle.checked) hideCrosshairs();
    });
  }
  if (maintenanceToggle) {
    maintenanceToggle.addEventListener("change", () => {
      showMaintenance = !!maintenanceToggle.checked;
      localStorage.setItem("new_gantt_maintenance", showMaintenance ? "1" : "0");
      renderRows();
      if (showMaintenance) preloadRegistrationMaintenance().catch(() => {});
    });
  }
  themeBtn.addEventListener("click", () => applyTheme(document.body.getAttribute("data-theme") === "dark" ? "light" : "dark"));
  window.addEventListener("resize", () => {
    if (hasUserZoom) return;
    fitPxPerMinuteToViewport();
    applyTimelineScale();
    buildAxis();
    renderRows();
    updateLiveNowBar();
  });

  if (btnPreviewManifest) btnPreviewManifest.addEventListener("click", withBusy(btnPreviewManifest, "Loading...", previewManifest));
  if (btnPaxList) btnPaxList.addEventListener("click", openPassengerList);
  if (syncPaxBtn) syncPaxBtn.addEventListener("click", withBusy(syncPaxBtn, "Syncing...", runPassengerSyncTest));
  if (btnSubmitApg) btnSubmitApg.addEventListener("click", withBusy(btnSubmitApg, "Submitting...", submitToApg));
  if (btnResetApg) btnResetApg.addEventListener("click", withBusy(btnResetApg, "Resetting...", resetApgPassengers));
  if (btnSeatmap) btnSeatmap.addEventListener("click", openSeatmap);
  if (btnMovementMsg) btnMovementMsg.addEventListener("click", openMovementDialog);
  if (mvModeDep) mvModeDep.addEventListener("click", () => setMovementMode("dep"));
  if (mvModeArr) mvModeArr.addEventListener("click", () => setMovementMode("arr"));
  if (mvAddDelay) mvAddDelay.addEventListener("click", () => addMovementDelayRow());
  if (mlOk) mlOk.addEventListener("click", () => saveModifyLegRemark({ closeOnDone: true }));
  if (mlPrev) {
    mlPrev.addEventListener("click", async () => {
      const prev = adjacentFlightForModifyLeg(-1);
      if (!prev) return;
      selectedId = prev.envision_flight_id;
      setDetail(prev);
      renderRows();
      await openModifyLegDialog(prev);
    });
  }
  if (mlNext) {
    mlNext.addEventListener("click", async () => {
      const next = adjacentFlightForModifyLeg(1);
      if (!next) return;
      selectedId = next.envision_flight_id;
      setDetail(next);
      renderRows();
      await openModifyLegDialog(next);
    });
  }
  if (mvOffblocks) mvOffblocks.addEventListener("input", recalcMovementDelay);
  if (flightEditAction) flightEditAction.addEventListener("change", refreshEditFormFromAction);
  if (flightEditSubmit) flightEditSubmit.addEventListener("click", submitFlightEdit);
  if (movementSubmit) movementSubmit.addEventListener("click", submitMovementMessage);

  async function initializeGanttPage() {
    applyTimelineScale();
    buildAxis();
    applyTheme(localStorage.getItem("new_gantt_theme") || "light");
    currentTimeZone = localStorage.getItem("new_gantt_tz") || "Pacific/Auckland";
    if (tzSelect) tzSelect.value = currentTimeZone;
    if (crosshairToggle) crosshairToggle.checked = localStorage.getItem("new_gantt_crosshair") === "1";
    showMaintenance = localStorage.getItem("new_gantt_maintenance") === "1";
    if (maintenanceToggle) maintenanceToggle.checked = showMaintenance;
    setupAutoRefresh();
    setupLiveNowTimer();
    setActionsEnabled(false);
    updateLiveNowBar();
    await ensureEnvisionEnvironment();
    await loadData({ showSpinner: true });
  }

  initializeGanttPage().catch((err) => {
    console.error(err);
    alert(err.message || "Unable to initialize Live Gantt.");
  });
})();
