document.addEventListener("DOMContentLoaded", function () {
  const root = document.getElementById("dcs-gantt-root");
  if (!root) return;

  const GANTT_URL      = root.dataset.ganttUrl;
  const APG_PUSH_URL   = root.dataset.apgPushUrl;
  const APG_RESET_URL  = root.dataset.apgResetUrl;
  const SAVE_TIMES_URL = root.dataset.saveTimesUrl;
  const ENV_TIMES_URL  = root.dataset.envTimesUrl;
  const ENV_CREW_URL  = root.dataset.envCrewUrl;

  const APG_PLAN_URL_TMPL = root.dataset.apgPlanUrlTemplate.replace(/0$/, "__PLAN__");


  const dataNodes     = Array.from(document.querySelectorAll(".gantt-flight-data"));
  const rowsContainer = document.getElementById("dcs-gantt-rows");
  const axisEl        = document.getElementById("dcs-gantt-axis");
  const spinner       = document.getElementById("gantt-loading");

  if (!rowsContainer || !axisEl) return;
  
  const loadingEl = document.getElementById("gantt-loading");
  
  // Header meta: "Loaded" date + "Last refresh"
  const loadedDateEl  = document.querySelector(".date-pill .date-text");
  const lastRefreshEl = document.getElementById("gantt-last-refresh");

  // Format "Loaded" date nicely (Mon 08 Dec 2025)
  function formatLoadedDateStr(isoDate) {
    if (!isoDate) return isoDate;
    const d = new Date(isoDate + "T00:00:00");
    if (isNaN(d)) return isoDate;
    return d.toLocaleDateString("en-NZ", {
      weekday: "short",
      day: "2-digit",
      month: "short",
      year: "numeric",
    });
  }

  function fetchEnvisionCrew(envisionFlightId) {
  if (!envisionFlightId || !ENV_CREW_URL) {
    return Promise.resolve(null);
  }

  return fetch(`${ENV_CREW_URL}?flight_id=${encodeURIComponent(envisionFlightId)}`)
    .then(r => r.json())
    .then(data => {
      if (!data.ok) {
        console.warn("Envision /flight_crew error:", data.error || data);
        return null;
      }
      return data.crew || [];
    })
    .catch(err => {
      console.error("Envision /flight_crew fetch failed:", err);
      return null;
    });
}

  function updateLoadedDateLabel(isoDate) {
    if (!loadedDateEl || !isoDate) return;
    loadedDateEl.textContent = formatLoadedDateStr(isoDate);
  }

  function updateLastRefreshLabel() {
    if (!lastRefreshEl) return;
    const now = new Date();
    lastRefreshEl.textContent = now.toLocaleTimeString("en-NZ", {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    });
  }



  function setGanttLoading(isLoading) {
    if (!loadingEl) return;
    loadingEl.style.display = isLoading ? "flex" : "none";
  }


  const DEFAULT_BLOCK_MIN = 60;

  // Base → ICAO mapping
  const BASE_ICAO = {
    "AKL": "NZAA",  // Auckland
    "WLG": "NZWN",  // Wellington
    "CHC": "NZCH",  // Christchurch
    "PPQ": "NZPP",  // Paraparaumu
    "WHK": "NZWK",  // Whakatane
    "WAG": "NZWU",  // Whanganui
    "CHT": "NZCI",  // Chatham Islands
    "ZQN": "NZQN",  // Queenstown
    "BHE": "NZWB",  // Blenheim
  };

    // ---------- Delay code metadata (IATA code -> Envision ID + description) ----------
  const DELAY_CODE_META = {
    "81": { id: 7,   description: "ATFM due to ATC en-route demand capacity" },
    "11": { id: 9,   description: "Late check-in, acceptance after deadline" },
    "12": { id: 10,  description: "Late check-in, congestion in check-in" },
    "15": { id: 14,  description: "Boarding, discrepancies and paging, missing check-in passenger" },
    "16": { id: 16,  description: "Commercial publicity/passenger convenience, illness/death, VIP, Press, TV" },
    "17": { id: 17,  description: "Catering order, late or incorrect order given to supplier" },
    "18": { id: 18,  description: "Baggage Processing, sorting etc" },
    "21": { id: 19,  description: "Cargo documentation, errors" },
    "22": { id: 20,  description: "Late positioning of Cargo" },
    "23": { id: 21,  description: "Late acceptance of Cargo" },
    "24": { id: 22,  description: "Inadequate cargo packing and/or quantity" },
    "25": { id: 23,  description: "Oversells, cargo booking error" },
    "26": { id: 24,  description: "Late cargo preparation in warehouse" },
    "27": { id: 25,  description: "Mail documentation, errors" },
    "28": { id: 26,  description: "Late mail positioning" },
    "31": { id: 27,  description: "Aircraft documentation late/inaccurate (W&B, gen dec, pax manifest, etc.)" },
    "32": { id: 28,  description: "Loading / Unloading, bulky/special load, lack of loading staff" },
    "33": { id: 29,  description: "Loading equipment, lack of/or breakdown" },
    "34": { id: 30,  description: "Servicing equipment, lack of/or breakdown, lack of staff (e.g. steps)" },
    "35": { id: 31,  description: "Aircraft cleaning" },
    "36": { id: 32,  description: "Fuelling / defueling" },
    "37": { id: 33,  description: "Catering, late delivery or loading" },
    "38": { id: 34,  description: "ULD, lack of or serviceability" },
    "39": { id: 35,  description: "Technical equipment lack or breakdown" },
    "41": { id: 36,  description: "Aircraft defects" },
    "42": { id: 37,  description: "Scheduled maintenance, late release" },
    "43": { id: 38,  description: "Non scheduled maintenance, extra checks / additional work" },
    "44": { id: 39,  description: "Spares and maintenance equipment, lack of/or breakdown" },
    "45": { id: 40,  description: "AOG spares, to be carried to another station" },
    "46": { id: 41,  description: "Aircraft change for technical reasons within same fleet" },
    "47": { id: 44,  description: "Stand-by aircraft, lack of planned stand-by aircraft for technical reasons" },
    "51": { id: 45,  description: "Damage in flight ops (bird/lighting/turbulence/overweight landing/taxi collision)" },
    "52": { id: 46,  description: "Damage during ground operations, collision (other than during taxi)" },
    "55": { id: 48,  description: "Departure Control" },
    "56": { id: 49,  description: "Cargo preparation / documentation" },
    "57": { id: 50,  description: "Flight Plans" },
    "58": { id: 51,  description: "Other automated systems" },
    "61": { id: 52,  description: "Flight Plan, late completion / change, flight documentation" },
    "62": { id: 53,  description: "Operational requirements, fuel / load alteration" },
    "63": { id: 56,  description: "Late crew boarding / departure procedures (other than connection/stand-by)" },
    "71": { id: 62,  description: "Departure station below aircraft operating limits" },
    "72": { id: 63,  description: "Destination station below aircraft operating limits" },
    "73": { id: 64,  description: "Alternate station below aircraft operating limits" },
    "74": { id: 65,  description: "Enroute – strong headwind, rerouting, weather avoidance" },
    "75": { id: 66,  description: "De-icing and de-snowing of aircraft" },
    "76": { id: 67,  description: "Removal of snow, ice, water and sand from airport" },
    "77": { id: 68,  description: "Ground handling impacted by adverse weather conditions" },
    "85": { id: 88,  description: "Restrictions at departure airport (closure, unrest, noise abatement, etc.)" },
    "86": { id: 89,  description: "Immigration, customs, health" },
    "87": { id: 90,  description: "Airport facilities, stands, ramp congestion, lighting, buildings, gate limits" },
    "88": { id: 91,  description: "Restrictions at destination airport, with or without ATFM" },
    "93": { id: 92,  description: "Aircraft rotation, late arrival from previous sector" },
    "99": { id: 93,  description: "Other reason, not matching any codes above" },
    "01": { id: 94,  description: "Planned schedule deviation for regular flights" },
    "02": { id: 95,  description: "Planned schedule deviation for charter flights" },
    "03": { id: 96,  description: "Late bus" },
    "13": { id: 98,  description: "Check-in error passenger and/or baggage" },
    "14": { id: 99,  description: "Oversales booking error" },
    "29": { id: 101, description: "Late mail acceptance" },
    "48": { id: 102, description: "Scheduled cabin configuration/version adjustments" },
    "59": { id: 103, description: "Operational requirements; fuel or load alteration" },
    "60": { id: 104, description: "Late crew boarding or departure procedures" },
    "64": { id: 105, description: "Flight deck crew shortage/sickness/stand-by/FTL" },
    "65": { id: 106, description: "Flight deck crew special request (non-operational)" },
    "66": { id: 107, description: "Late cabin crew boarding/departure (not connections/stand-by)" },
    "82": { id: 108, description: "ATFM due to ATC staff/equipment en-route, industrial action, staff shortage" },
    "83": { id: 109, description: "ATFM due to restriction at destination airport" },
    "84": { id: 110, description: "ATFM due to weather at destination" },
    "89": { id: 111, description: "Restrictions at departure airport inc. ATS/start-up/pushback" },
    "91": { id: 112, description: "Load connection, awaiting from another flight" },
    "92": { id: 113, description: "Through check-in error passenger and baggage" },
    "94": { id: 114, description: "Cabin crew rotation awaiting cabin crew from another flight" },
    "95": { id: 115, description: "Crew rotation awaiting crew from another flight" },
    "96": { id: 116, description: "Ops control re-routing, diversion, a/c change (non-technical)" },
    "97": { id: 117, description: "Industrial action within own airline" },
    "98": { id: 118, description: "Industrial action outside own airline, excluding ATC" },
    "67": { id: 119, description: "Cabin crew shortage/sickness/stand-by/FTL" }
  };

  // ---------- Helpers ----------
  function parseDate(val) {
    if (!val) return null;
    const d = new Date(val);
    return isNaN(d.getTime()) ? null : d;
  }

function hasUmnrSSR(ssrs) {
  if (!Array.isArray(ssrs)) return false;
  return ssrs.some(s => {
    const code = (s.Code || s.code || "").toUpperCase();
    return code === "UMNR" || code === "UM"; // cover both if they ever use UM
  });
}


  function formatDateYMD(d) {
    if (!d) return "";
    const yyyy = d.getFullYear();
    const mm   = String(d.getMonth() + 1).padStart(2, "0");
    const dd   = String(d.getDate()).padStart(2, "0");
    return `${yyyy}-${mm}-${dd}`;
  }

  function fmtTime(d) {
    if (!d) return "";
    return d.toLocaleTimeString("en-NZ", {
      hour: "2-digit",
      minute: "2-digit",
      hour12: false,
    });
  }

  function normaliseStatus(status) {
    if (!status) return "unknown";
    const s = status.toLowerCase();
    if (s.includes("planning"))        return "planning";
    if (s.includes("on blocks"))       return "onblocks";
    if (s.includes("off blocks"))      return "offblocks";
    if (s.includes("take off"))        return "takeoff";
    if (s.includes("landed"))          return "landed";
    if (s.includes("return to stand")) return "returntostand";
    if (s.includes("divert"))          return "diverted";
    return "unknown";
  }

    // ---------- Build rows from DOM (hidden .gantt-flight-data divs) ----------
  function buildRowsFromDom(nodes) {
    return nodes.map(node => ({
      reg: node.dataset.reg || "Unknown",
      dep: node.dataset.dep || "",
      ades: node.dataset.ades || "",
      std_nz: node.dataset.std || null,
      sta_nz: node.dataset.sta || null,
      std_sched_nz: node.dataset.stdSched || null,
      sta_sched_nz: node.dataset.staSched || null,
      dep_actual_nz: node.dataset.depActual || null,
      arr_actual_nz: node.dataset.arrActual || null,
      flight_number: node.dataset.flight || "",
      designator: node.dataset.designator || "",
      apg_plan_id: node.dataset.apgPlanId || "",
      block_mins: node.dataset.block || "0",
      aircraft_type: node.dataset.aircraftType || "",
      flight_status: node.dataset.flightStatus || "",
      adt: node.dataset.adt || "0",
      chd: node.dataset.chd || "0",
      inf: node.dataset.inf || "0",
      pax_count: node.dataset.paxCount || "0",
      bags_kg: node.dataset.bagsKg || "0",
      pax_list: node.dataset.paxList ? JSON.parse(node.dataset.paxList) : [],
      envision_flight_id: node.dataset.envisionFlightId || null,      // NEW
      delays: node.dataset.delays ? JSON.parse(node.dataset.delays) : [],

    }));
  }
  // ---------- Convert rows → flights model ----------
function buildFlightsFromRows(rows) {
  return rows
    .map((row, idx) => {
      const stdEst    = parseDate(row.std_nz);
      const staEst    = parseDate(row.sta_nz);
      const stdSched  = parseDate(row.std_sched_nz);
      const staSched  = parseDate(row.sta_sched_nz);

      // NEW: actuals
      const depActual = parseDate(row.dep_actual_nz);
      const arrActual = parseDate(row.arr_actual_nz);

      const block = parseInt(row.block_mins || "0", 10) || DEFAULT_BLOCK_MIN;

      // Primary times used for the bar:
      // 1) Actual if present
      // 2) Otherwise estimated
      // 3) Otherwise scheduled
      let barStart = depActual || stdEst || stdSched || null;
      let barEnd   = arrActual || staEst || staSched || null;

      if (!barEnd && barStart) {
        barEnd = new Date(barStart.getTime() + block * 60000);
      }

      const flightFull = (row.flight_number || "").trim();
      const designator = (row.designator || "").trim().toUpperCase();

      let numericPart = flightFull;
      if (designator && flightFull.toUpperCase().startsWith(designator)) {
        numericPart = flightFull.slice(designator.length);
      }

      let paxList = [];
      try {
        paxList = row.pax_list || [];
      } catch (e) {
        console.warn("Bad pax_list JSON", e);
      }

      return {
        id: idx,
        reg: (row.reg || "UNKNOWN").toUpperCase().trim(),
        dep: (row.dep || "").toUpperCase(),
        ades: (row.ades || "").toUpperCase(),

        // These now represent the times used to draw the bar:
        stdEst: barStart,
        staEst: barEnd,

        // Keep scheduled too for the thin bar / labels
        stdSched,
        staSched,

        // Optional: keep raw actuals if you want them later in tooltips
        depActual,
        arrActual,

        designator,
        flightFull,
        flightNumeric: numericPart,
        apgPlanId: row.apg_plan_id || "",
        blockMins: block,
        statusRaw: row.flight_status || "",
        statusNorm: normaliseStatus(row.flight_status),
        paxTotal: parseInt(row.pax_count || "0", 10) || 0,
        paxAdt: parseInt(row.adt || "0", 10) || 0,
        paxChd: parseInt(row.chd || "0", 10) || 0,
        paxInf: parseInt(row.inf || "0", 10) || 0,
        bagsKg: parseFloat(row.bags_kg || "0") || 0,
        paxList,
        aircraftType: row.aircraft_type || "",
        envisionFlightId: row.envision_flight_id || null,
        delays: row.delays || [],
      };
    })
    .filter(f => f.stdEst && f.staEst);  // now these are barStart/barEnd
}


  // ---------- Global Gantt state ----------
  let flights    = [];
  let chartStart = null;
  let chartEnd   = null;
  let durationMs = 0;
  let byReg      = {};
  let regs       = [];
  let firstRefresh = true;  // controls the loading overlay (only on first fetch)

  // ---------- Modals / context menu shared state ----------
  let currentFlight = null;
  let currentBarEl  = null;
  let currentTimeMode = null;  // "de8p" or "arr"

  const actionModalEl  = document.getElementById("flightActionModal");
  const actionModal    = actionModalEl ? new bootstrap.Modal(actionModalEl) : null;
  const timeModalEl    = document.getElementById("timeModal");
  const timeModal      = timeModalEl ? new bootstrap.Modal(timeModalEl) : null;
  const paxListModalEl = document.getElementById("paxListModal");
  const paxListModal   = paxListModalEl ? new bootstrap.Modal(paxListModalEl) : null;
  const seatmapModalEl = document.getElementById("seatmapModal");
  const seatmapModal   = seatmapModalEl ? new bootstrap.Modal(seatmapModalEl) : null;

  const ctxMenu         = document.getElementById("flightContextMenu");
  const ctxDepartureBtn = document.getElementById("ctx-departure");
  const ctxArrivalBtn   = document.getElementById("ctx-arrival");

  function hideContextMenu() {
    if (ctxMenu) ctxMenu.style.display = "none";
  }
  document.addEventListener("click", hideContextMenu);

  // ---------- Gantt rendering ----------
  function renderGanttFromFlights(newFlights) {
    flights = newFlights || [];

    if (!flights.length) {
      rowsContainer.innerHTML = "";
      axisEl.innerHTML = "";
      byReg = {};
      regs  = [];
      return;
    }

    // Time window
    let minStd = flights[0].stdEst;
    let maxEta = flights[0].staEst;
    flights.forEach(f => {
      if (f.stdEst && f.stdEst < minStd) minStd = f.stdEst;
      if (f.staEst && f.staEst > maxEta) maxEta = f.staEst;
    });

    chartStart = new Date(minStd.getTime() - 60 * 60000);
    chartEnd   = new Date(maxEta.getTime() + 60 * 60000);
    durationMs = chartEnd - chartStart;

    // Axis
    axisEl.innerHTML = "";
    const startHour = new Date(chartStart);
    startHour.setMinutes(0, 0, 0);
    for (let t = new Date(startHour); t <= chartEnd; t.setHours(t.getHours() + 1)) {
      const offset = (t - chartStart) / durationMs * 100;
      const tick   = document.createElement("div");
      tick.className = "tick";
      tick.style.left = offset + "%";

      const label = document.createElement("div");
      label.className = "tick-label";
      label.textContent = fmtTime(t);

      tick.appendChild(label);
      axisEl.appendChild(tick);
    }

    // Group by registration
    byReg = {};
    flights.forEach(f => {
      const regKey = f.reg || "UNKNOWN";
      if (!byReg[regKey]) byReg[regKey] = [];
      byReg[regKey].push(f);
    });
    regs = Object.keys(byReg).sort();

    // Re-render rows with current base filter
    const baseFilterSelect = document.getElementById("base-filter");
    const baseCode = baseFilterSelect ? (baseFilterSelect.value || "") : "";
    renderRowsForBase(baseCode);
  }

  // ---------- Rows / bars (with base filter) ----------
  function renderRowsForBase(baseCode) {
    rowsContainer.innerHTML = "";

    const base     = (baseCode || "").toUpperCase();
    const baseIcao = base ? (BASE_ICAO[base] || null) : null;

    regs.forEach(reg => {
      const flightsForReg = (byReg[reg] || []).slice().sort((a, b) => a.stdEst - b.stdEst);

      // Filter aircraft lines by base (either IATA or mapped ICAO)
      if (base) {
        const hasBaseFlight = flightsForReg.some(f => {
          const dep  = (f.dep || "").toUpperCase();
          const ades = (f.ades || "").toUpperCase();
          return (
            dep === base || ades === base ||
            (baseIcao && (dep === baseIcao || ades === baseIcao))
          );
        });
        if (!hasBaseFlight) return; // skip this aircraft
      }

      const row = document.createElement("div");
      row.className = "gantt-row";

      const labelCol = document.createElement("div");
      labelCol.className = "gantt-row-label";
      labelCol.textContent = reg;

      const track = document.createElement("div");
      track.className = "gantt-row-track";

      let lastArrAirport = null;

      flightsForReg.forEach(f => {
        const leftPct  = (f.stdEst - chartStart) / durationMs * 100;
        const widthPct = Math.max((f.staEst - f.stdEst) / durationMs * 100, 2);

        // scheduled bar (thin)
        if (f.stdSched && f.staSched) {
          const schedLeft  = (f.stdSched - chartStart) / durationMs * 100;
          const schedWidth = Math.max((f.staSched - f.stdSched) / durationMs * 100, 2);
          const schedBar = document.createElement("div");
          schedBar.className = "gantt-flight-sched";
          schedBar.style.left  = schedLeft + "%";
          schedBar.style.width = schedWidth + "%";
          track.appendChild(schedBar);
        }

        // actual bar (thick)
        const bar = document.createElement("div");
        bar.className = "gantt-flight status-" + (f.statusNorm || "unknown");
        bar.style.left  = leftPct + "%";
        bar.style.width = widthPct + "%";
        bar.dataset.flightId = f.id;

        const labelCode = `${f.designator || ""}${f.flightNumeric || f.flightFull || ""}`.trim();
        bar.innerHTML = `<span class="flight-main">${labelCode}</span>`;

        const hasActual = !!(f.depActual || f.arrActual);
        const startLabel = fmtTime(f.stdEst);
        const endLabel   = fmtTime(f.staEst);
        bar.title = `${labelCode}  ${f.dep} → ${f.ades}  (${startLabel}–${endLabel})` +
                    (hasActual ? " [ACT]" : "");


        bar.addEventListener("click", function (ev) {
        ev.preventDefault();
        currentFlight = f;
        currentBarEl  = bar;

        populateFlightDetailsModal(f);
        updateCrewSidebar(f);        // 🔹 update right-hand crew panel

        if (actionModal) actionModal.show();
      });

        bar.addEventListener("contextmenu", function (ev) {
          ev.preventDefault();
          currentFlight = f;
          currentBarEl  = bar;
          if (!ctxMenu) return;
          ctxMenu.style.left = ev.pageX + "px";
          ctxMenu.style.top  = ev.pageY + "px";
          ctxMenu.style.display = "block";
        });

        // airport labels
        if (!lastArrAirport || f.dep !== lastArrAirport) {
          const depLbl = document.createElement("div");
          depLbl.className = "gantt-airport-label dep";
          depLbl.textContent = f.dep;
          depLbl.style.left = leftPct + "%";
          track.appendChild(depLbl);
        }

        const arrLbl = document.createElement("div");
        arrLbl.className = "gantt-airport-label arr";
        arrLbl.textContent = f.ades;
        arrLbl.style.left = (leftPct + widthPct) + "%";
        track.appendChild(arrLbl);
        lastArrAirport = f.ades;

        track.appendChild(bar);
      });

      row.appendChild(labelCol);
      row.appendChild(track);
      rowsContainer.appendChild(row);
    });
  }

  const baseFilterSelect = document.getElementById("base-filter");
  if (baseFilterSelect) {
    baseFilterSelect.addEventListener("change", function () {
      renderRowsForBase(this.value || "");
    });
  }

// ---------- Passenger status classifier ----------
function classifyPaxStatus(p) {
  const raw =
    (p.DCSStatus       || p.DcsStatus      ||
     p.Status          || p.status         ||
     p.CheckInStatus   || p.checkInStatus  ||
     p.BoardingStatus  || p.boardingStatus ||
     ""
    ).toString().toUpperCase();

  const explicitFlown =
    p.Flown === true || p.flown === true;

  // Highest state first: FLOWN → BOARDED → CHECKED → BOOKED
  if (explicitFlown || raw.includes("FLOWN") || raw === "FLWN") {
    return "FLOWN";
  }

  if (p.Boarded === true || p.boarded === true) {
    return "BOARDED";
  }
  if (p.CheckedIn === true || p.checkedIn === true) {
    return "CHECKED";
  }

  if (!raw) return "BOOKED";

  if (raw.includes("FLOWN") || raw === "FLWN") return "FLOWN";
  if (raw.includes("BOARD")) return "BOARDED";
  if (raw.includes("CHECK")) return "CHECKED";

  if (raw === "CI"  || raw === "CKIN" || raw === "CKI") return "CHECKED";
  if (raw === "BD"  || raw === "BRD")                 return "BOARDED";

  return "BOOKED";
}


// ---------- Flight details modal ----------
function populateFlightDetailsModal(f) {
  const codeEl      = document.getElementById("modal-flight-code");
  const routeEl     = document.getElementById("modal-route");
  const acEl        = document.getElementById("modal-aircraft");
  const stdStaEl    = document.getElementById("modal-std-sta");
  const etdEtaEl    = document.getElementById("modal-etd-eta");
  const atdAtaEl    = document.getElementById("modal-atd-ata");
  const statusEl    = document.getElementById("modal-status");
  const paxBrkEl    = document.getElementById("modal-pax-breakdown");
  const paxTypesEl  = document.getElementById("modal-pax-types");
  const bagsEl      = document.getElementById("modal-bags");
  const warningsEl  = document.getElementById("flight-warnings");

  const labelCode = `${f.designator || ""}${f.flightNumeric || f.flightFull || ""}`.trim();

  if (codeEl)  codeEl.textContent  = labelCode;
  if (routeEl) routeEl.textContent = `${f.dep} → ${f.ades}`;
  if (acEl)    acEl.textContent    = f.reg;

  const stdTxt = f.stdSched ? fmtTime(f.stdSched) : "—";
  const staTxt = f.staSched ? fmtTime(f.staSched) : "—";
  if (stdStaEl) stdStaEl.textContent = `${stdTxt} / ${staTxt}`;

  const etdTxt = fmtTime(f.stdEst);
  const etaTxt = fmtTime(f.staEst);
  if (etdEtaEl) etdEtaEl.textContent = `${etdTxt || "—"} / ${etaTxt || "—"}`;

  if (statusEl) statusEl.textContent = f.statusRaw || "Unknown";

  // --- Passenger breakdown (stacked, including FLOWN) ---
  const paxList = Array.isArray(f.paxList) ? f.paxList : [];
  let bookedTotal  = 0;
  let checkedTotal = 0;
  let boardedTotal = 0;
  let flownTotal   = 0;

  paxList.forEach(p => {
    const bucket = classifyPaxStatus(p);
    switch (bucket) {
      case "FLOWN":
        flownTotal++;
        break;
      case "BOARDED":
        boardedTotal++;
        break;
      case "CHECKED":
        checkedTotal++;
        break;
      default: // BOOKED or anything else
        bookedTotal++;
        break;
    }
  });

  const totalDisplay = f.paxTotal || paxList.length;
  const typeStr = `Total: ${totalDisplay} (AD ${f.paxAdt} / CHD ${f.paxChd} / INF ${f.paxInf})`;

  if (paxBrkEl) {
    paxBrkEl.innerHTML = `
      <div class="pax-line">
        <span class="pax-label">Booked</span>
        <span class="pax-value">${bookedTotal}</span>
      </div>
      <div class="pax-line">
        <span class="pax-label">Checked-in</span>
        <span class="pax-value">${checkedTotal}</span>
      </div>
      <div class="pax-line">
        <span class="pax-label">Boarded</span>
        <span class="pax-value">${boardedTotal}</span>
      </div>
      <div class="pax-line">
        <span class="pax-label">Flown</span>
        <span class="pax-value">${flownTotal}</span>
      </div>
      <div class="pax-line mt-1">
        <span class="pax-label">Total</span>
        <span class="pax-value">${totalDisplay}</span>
      </div>
    `;
  }

  if (paxTypesEl) {
    paxTypesEl.textContent = typeStr;
  }

  if (paxTypesEl) {
    paxTypesEl.textContent = typeStr;
  }

  if (bagsEl) bagsEl.textContent = (f.bagsKg || 0).toFixed(1);

  // ---- Warnings (DCS + APG) with hover details ----
  // ---- Warnings (DCS + APG) with hover details ----
  if (warningsEl) {
    const hasDcs = Array.isArray(f.paxList) && f.paxList.length > 0;
    const hasApg = !!(f.apgPlanId && f.apgPlanId.toString().trim());
    const hasEnv = !!f.envisionFlightId;   // 👈 new: do we have Envision ID?

    const flightDate = formatDateYMD(f.stdEst || f.stdSched);
    const etdText    = fmtTime(f.stdEst);
    const etaText    = fmtTime(f.staEst);

    const dcsTip = hasDcs
      ? `DCS flight key:\n  ${labelCode}  ${f.dep} → ${f.ades}\n  Date: ${flightDate}\n  ETD/ETA (local): ${etdText} / ${etaText}`
      : `No DCS passenger list linked for:\n  ${labelCode}  ${f.dep} → ${f.ades}\n  Date: ${flightDate}`;

    const envLine = hasEnv ? `\nEnvision flight ID: ${f.envisionFlightId}` : "";

    const apgTip = hasApg
      ? `APG plan ID: ${f.apgPlanId}${envLine}\nMatched using:\n  ${labelCode}  ${f.dep} → ${f.ades}\n  Date: ${flightDate}`
      : `No APG plan ID is currently linked to:\n  ${labelCode}  ${f.dep} → ${f.ades}\n  Date: ${flightDate}${envLine}`;

    let html = "";

    // DCS badge
    html += hasDcs
      ? `<span class="badge bg-success me-1" title="${dcsTip.replace(/"/g, "&quot;")}">DCS linked</span>`
      : `<span class="badge bg-secondary me-1" title="${dcsTip.replace(/"/g, "&quot;")}">No DCS pax</span>`;

    // APG + Envision badge
    if (hasApg) {
      const apgLabelText = hasEnv
        ? `APG ${f.apgPlanId} / ENV ${f.envisionFlightId}`
        : `APG ${f.apgPlanId}`;

      html += `<span class="badge bg-success" title="${apgTip.replace(/"/g, "&quot;")}">${apgLabelText}</span>`;
    } else {
      html += `<span class="badge bg-warning text-dark" title="${apgTip.replace(/"/g, "&quot;")}">No APG plan</span>`;
    }

    warningsEl.innerHTML = html;
  }


  // ---- Delays table (kept inside Q3) ----
  const delaysEl = document.getElementById("modal-delays");
  if (delaysEl) {
    const delays = Array.isArray(f.delays) ? f.delays : [];

    if (!delays.length) {
      delaysEl.innerHTML = '<span class="text-muted">No delays recorded.</span>';
    } else {
      const rowsHtml = delays.map(d => {
        const code = d.code || d.delayCode || "";
        const mins = d.delayMinutes || d.minutes || 0;
        const leg  = d.isArrival ? "ARR" : "DEP";
        const meta = DELAY_CODE_META[code] || null;
        const desc = d.description || (meta ? meta.description : "");

        return `
          <tr>
            <td>${leg}</td>
            <td>${code}</td>
            <td class="text-end">${mins}</td>
            <td>${desc}</td>
          </tr>
        `;
      }).join("");

      delaysEl.innerHTML = `
        <div class="table-responsive">
          <table class="table table-sm mb-0">
            <thead>
              <tr>
                <th>Leg</th>
                <th>Code</th>
                <th class="text-end">Min</th>
                <th>Description</th>
              </tr>
            </thead>
            <tbody>
              ${rowsHtml}
            </tbody>
          </table>
        </div>
      `;
    }
  }

  // ---- ATD / ATA from Envision ----
  if (atdAtaEl) {
    atdAtaEl.textContent = "— / —";

    if (typeof fetchEnvisionTimes === "function" && f.envisionFlightId) {
      fetchEnvisionTimes(f.envisionFlightId).then(data => {
        if (!data || !data.local_hm) return;
        const hm = data.local_hm;

        const atd = hm.departureActual || "";
        const ata = hm.arrivalActual   || "";

        const atdText = atd || "—";
        const ataText = ata || "—";
        atdAtaEl.textContent = `${atdText} / ${ataText}`;
      });
    }
  }
    // ---- Crew (from Envision) ----
  const crewEl = document.getElementById("modal-crew");
  if (crewEl) {
    // Clear previous content
    crewEl.innerHTML = "";

    // If we don't have an Envision ID, just show a note
    if (!f.envisionFlightId) {
      crewEl.innerHTML =
        '<span class="text-muted">No Envision flight ID linked – crew not available.</span>';
    } else {
      crewEl.innerHTML =
        '<span class="text-muted">Loading crew from Envision…</span>';

      // Optional: cache on the flight object so we don't refetch if the modal reopens
      if (Array.isArray(f.crew) && f.crew.length) {
        renderCrewTable(f.crew, crewEl);
      } else if (typeof fetchEnvisionCrew === "function") {
        fetchEnvisionCrew(f.envisionFlightId).then(crew => {
          if (!crew || !crew.length) {
            crewEl.innerHTML =
              '<span class="text-muted">No operating crew found in Envision.</span>';
            return;
          }

          // cache
          f.crew = crew;
          renderCrewTable(crew, crewEl);
        });
      }
    }
  }
}


// --- send to APG with pending icon + stronger payload building ---
function sendToFlightPlan(f, barEl, { previewOnly = false } = {}) {
  if (!f) return;

  // Button feedback
  const sendBtn = document.getElementById("btn-send-apg");
  let originalBtnHtml = "";
  if (sendBtn) {
    originalBtnHtml = sendBtn.innerHTML;
    sendBtn.disabled = true;
    sendBtn.innerHTML =
      '<span class="spinner-border spinner-border-sm me-1" role="status" aria-hidden="true"></span>Sending…';
  }

  // Bar feedback
  if (barEl) barEl.classList.add("sending");

  // --- Build required fields safely ---

  // Flight date: prefer ETD/STD estimate, fall back to scheduled
  const dt = f.stdEst || f.stdSched || null;
  const dateStr = dt ? dt.toISOString().slice(0, 10) : null; // yyyy-mm-dd

  // Plan ID from model
    const apgPlanId = (
    f.apgPlanId ||
    f.apg_plan_id ||
    f.apgRouteId ||
    f.apg_route_id ||
    f.routeId ||
    f.route_id ||
    ""
  ).toString().trim() || null;

  // Designator + flight number parsing
  let designator = (f.designator || "").toString().trim().toUpperCase();
  let fullNo =
    (f.flightFull ||
      f.flight ||
      (designator && f.flightNumeric ? designator + f.flightNumeric : "") ||
      "").toString().replace(/\s+/g, "").toUpperCase();

  // If we still don't have a designator, try to infer from fullNo
  if (!designator && fullNo) {
    const m = fullNo.match(/^([A-Z]{1,3})?(\d+)/);
    if (m && m[1]) {
      designator = m[1];
    }
  }

  // Numeric part: last run of digits (this is what Python expects as flight_number)
  let number = "";
  if (fullNo) {
    const m2 = fullNo.match(/(\d+)$/);
    if (m2) number = m2[1];
  }
  // If no fullNo yet but we have designator + numeric, build it
  if (!fullNo && designator && f.flightNumeric) {
    number = f.flightNumeric.toString();
    fullNo = (designator + number).toUpperCase();
  }

  // Envision flight ID (any shape we can get)
  const envisionFlightId =
    f.envisionFlightId ||
    f.envision_flight_id ||
    null;

  // Final payload – matches api_dcs_push_to_apg expectations
  const payload = {
    apg_plan_id: apgPlanId,
    dep: (f.dep || "").toString().toUpperCase(),
    ades: (f.ades || "").toString().toUpperCase(),
    date: dateStr,
    designator: designator,
    flight_number: number || null,               // <-- KEY: matches Python "flight_number"
    reg: (f.reg || "").toString().toUpperCase(),
    envision_flight_id: envisionFlightId,
    preview_only: !!previewOnly,
    // extra data if you want it later; backend will just ignore this for now
    pax_list: Array.isArray(f.paxList) ? f.paxList : [],
  };

  console.info("[APG] push payload", payload);

  // Basic guard so we don't spam 400s
  if (
    !payload.apg_plan_id ||
    !payload.dep ||
    !payload.ades ||
    !payload.date ||
    !payload.designator ||
    !payload.flight_number
  ) {
    console.error("[APG] missing required payload fields", payload);
    alert("Cannot send to APG: missing required data (plan/dep/ades/date/flight).");
    if (sendBtn) {
      sendBtn.disabled = false;
      sendBtn.innerHTML = originalBtnHtml;
    }
    if (barEl) barEl.classList.remove("sending");
    return Promise.reject(new Error("Missing required fields for APG push"));
  }

  return fetch(APG_PUSH_URL, {                     // <--- use your configured URL
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  })
    .then((r) => {
      if (!r.ok) {
        return r
          .json()
          .catch(() => ({}))
          .then((j) => {
            const msg = j.error || `HTTP ${r.status}`;
            throw new Error(msg);
          });
      }
      return r.json();
    })
    .then((data) => {
      console.info("[APG] push OK", data);
      // at this point your Flask route has:
      //  - updated the APG plan
      //  - generated the manifest PDF
      //  - uploaded it to APG (if no errors)
      return data;
    })
    .catch((err) => {
      console.error("[APG] send error:", err);
      throw err;
    })
    .finally(() => {
      if (sendBtn) {
        sendBtn.disabled = false;
        sendBtn.innerHTML = originalBtnHtml;
      }
      if (barEl) {
        barEl.classList.remove("sending");
      }
    });
}


// --- Reset APG plan for the current flight ---
async function resetFlightPlan(f, barEl) {
  if (!f) return;

  const resetBtn = document.getElementById("btn-reset-apg");
  if (!resetBtn) return;

  const dt = f.stdEst || f.stdSched || null;
  const dateStr = dt ? dt.toISOString().slice(0, 10) : null;

  const apgPlanId = (f.apgPlanId || f.apg_plan_id || "").toString().trim() || null;

  const payload = {
    apg_plan_id: apgPlanId,
    dep: (f.dep || "").toString().toUpperCase(),
    date: dateStr,
    designator: (f.designator || "").toString().toUpperCase(),
    flight_number:
      (f.flightFull ||
        (f.designator || "") + (f.flightNumeric || "")).
        toString().replace(/\s+/g, "").toUpperCase()
  };

  if (!payload.apg_plan_id || !payload.dep || !payload.date || !payload.designator || !payload.flight_number) {
    alert("Cannot reset APG plan: missing required data.");
    return;
  }

  if (!confirm(`Reset APG plan ${payload.apg_plan_id} for ${payload.designator}${payload.flight_number}?`)) {
    return;
  }

  resetBtn.disabled = true;
  const originalText = resetBtn.textContent;
  resetBtn.textContent = "Resetting…";

  if (barEl) barEl.classList.add("sending");

  try {
    const resp = await fetch(APG_RESET_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    const data = await resp.json().catch(() => ({}));

    if (!resp.ok || !data.ok) {
      throw new Error(data.error || resp.statusText || "Reset failed");
    }

    alert("APG plan has been reset.");
    if (barEl) {
      barEl.classList.remove("sending");
      barEl.classList.remove("sent");
    }
  } catch (err) {
    console.error("[APG] reset error:", err);
    alert("Error resetting APG plan: " + err.message);
    if (barEl) barEl.classList.remove("sending");
  } finally {
    resetBtn.disabled = false;
    resetBtn.textContent = originalText;
  }
}
// Wire the reset button
const resetBtn = document.getElementById("btn-reset-apg");
if (resetBtn) {
  resetBtn.addEventListener("click", function () {
    if (!currentFlight) {
      alert("Please select a flight on the Gantt first.");
      return;
    }
    resetFlightPlan(currentFlight, currentBarEl);
  });
}

  async function loadApgPlanDetails(planId) {
    const detailsEl = document.getElementById("apg-plan-details");
    const idSpan    = document.getElementById("apg-plan-id");
    const jsonPre   = document.getElementById("apg-plan-json");
    if (!detailsEl || !idSpan || !jsonPre) return;

    try {
      const res  = await fetch(APG_PLAN_URL_TMPL.replace("__PLAN__", encodeURIComponent(planId)));
      const data = await res.json();
      if (!res.ok || !data.ok) return;
      idSpan.textContent      = planId;
      jsonPre.textContent     = JSON.stringify(data.plan, null, 2);
      detailsEl.style.display = "";
      detailsEl.open          = true;
    } catch (err) {
      console.error("Error fetching APG plan:", err);
    }
  }

  const sendBtn = document.getElementById("btn-send-apg");
  if (sendBtn) {
    sendBtn.addEventListener("click", function () {
      if (!currentFlight || !currentBarEl) return;
      sendToFlightPlan(currentFlight, currentBarEl);
    });
  }

    // ---------- Envision: load actual times for a flight ----------

  function fetchEnvisionTimes(envisionFlightId) {
    if (!envisionFlightId) {
      return Promise.resolve(null);
    }

      return fetch(`${ENV_TIMES_URL}?flight_id=${encodeURIComponent(envisionFlightId)}`)
      .then(r => r.json())
      .then(data => {
        if (!data.ok) {
          console.warn("Envision /flight_times error:", data.error || data);
          return null;
        }
        return data;  // { local_hm: { departureActual, departureTakeOff, arrivalLanded, arrivalActual }, ... }
      })
      .catch(err => {
        console.error("Envision /flight_times fetch failed:", err);
        return null;
      });
  }

  function formatDob(dobStr) {
    if (!dobStr) return "";
    const d = new Date(dobStr);
    if (isNaN(d.getTime())) return dobStr;
    const y = d.getFullYear();
    const m = String(d.getMonth() + 1).padStart(2, "0");
    const day = String(d.getDate()).padStart(2, "0");
    return `${y}-${m}-${day}`;
  }

  function calcAge(dobStr) {
    if (!dobStr) return "";
    const d = new Date(dobStr);
    if (isNaN(d.getTime())) return "";
    const today = new Date();
    let age = today.getFullYear() - d.getFullYear();
    const m = today.getMonth() - d.getMonth();
    if (m < 0 || (m === 0 && today.getDate() < d.getDate())) age--;
    return age;
  }

  function getField(obj, candidates, fallback = "") {
    for (const c of candidates) {
      if (obj[c] !== undefined && obj[c] !== null && obj[c] !== "") {
        return obj[c];
      }
    }
    return fallback;
  }

  // Seat parsing helper for sorting "12C"
  function parseSeatCode(seatStr) {
    if (!seatStr) return { row: Number.MAX_SAFE_INTEGER, col: "" };
    const s = seatStr.toString().trim().toUpperCase();
    const m = s.match(/^(\d+)([A-Z]+)$/);
    if (!m) return { row: Number.MAX_SAFE_INTEGER, col: s };
    return {
      row: parseInt(m[1], 10),
      col: m[2],
    };
  }

  function sortPaxTable(colIndex, ascending) {
    const table = document.getElementById("pax-list-table");
    const tbody = document.getElementById("pax-list-body");
    if (!table || !tbody) return;

    const rows = Array.from(tbody.querySelectorAll("tr"));
    if (!rows.length) return;

    rows.sort((a, b) => {
      const aText = (a.children[colIndex]?.textContent || "").trim();
      const bText = (b.children[colIndex]?.textContent || "").trim();

      if (colIndex === 0) {
        const aSeat = parseSeatCode(aText);
        const bSeat = parseSeatCode(bText);
        if (aSeat.row !== bSeat.row)
          return ascending ? (aSeat.row - bSeat.row) : (bSeat.row - aSeat.row);
        if (aSeat.col < bSeat.col) return ascending ? -1 : 1;
        if (aSeat.col > bSeat.col) return ascending ? 1 : -1;
        return 0;
      }

      if (colIndex === 3) {
        const aNum = parseInt(aText || "0", 10);
        const bNum = parseInt(bText || "0", 10);
        if (isNaN(aNum) && isNaN(bNum)) return 0;
        if (isNaN(aNum)) return ascending ? 1 : -1;
        if (isNaN(bNum)) return ascending ? -1 : 1;
        return ascending ? (aNum - bNum) : (bNum - aNum);
      }

      if (aText < bText) return ascending ? -1 : 1;
      if (aText > bText) return ascending ? 1 : -1;
      return 0;
    });

    rows.forEach(row => tbody.appendChild(row));
  }

  (function setupPaxTableHeaderSort() {
    const table = document.getElementById("pax-list-table");
    if (!table) return;

    const headers = table.querySelectorAll("thead th");
    headers.forEach((th, idx) => {
      th.style.cursor = "pointer";
      th.dataset.sortDir = "none";

      th.addEventListener("click", () => {
        const current = th.dataset.sortDir;
        const newDir = (current === "asc") ? "desc" : "asc";

        headers.forEach(h => h.dataset.sortDir = "none");
        th.dataset.sortDir = newDir;

        sortPaxTable(idx, newDir === "asc");
      });
    });
  })();

  function populatePassengerListModal(passengers) {
    const tbody = document.getElementById("pax-list-body");
    if (!tbody) return;

    tbody.innerHTML = "";

    (passengers || []).forEach((p) => {
      const name =
        (
          getField(p, ["Name", "FullName", "PassengerName"], "") ||
          (
            (getField(p, ["NamePrefix"], "") + " " +
             getField(p, ["GivenName", "FirstName"], "") + " " +
             getField(p, ["Surname", "LastName"], ""))
          )
        ).trim();

      const dob   = getField(p, ["DateOfBirth", "BirthDate", "DOB"], "");
      const age   = calcAge(dob);
      const seat  = getField(p, ["Seat", "SeatNo", "SeatNumber"], "");
      const pnr   = getField(p, ["BookingReferenceID", "PNR", "PnrCode", "RecordLocator"], "");
      const statusBucket = classifyPaxStatus(p);
      let statusLabel = "";
      switch (statusBucket) {
        case "BOOKED":  statusLabel = "Booked";      break;
        case "CHECKED": statusLabel = "Checked-in";  break;
        case "BOARDED": statusLabel = "Boarded";     break;
        case "FLOWN":   statusLabel = "Flown";       break;
        default:        statusLabel = statusBucket || ""; break;
      }

      const ssrs = getField(p, ["Ssrs", "SSRs", "SpecialServiceRequests"], []);
      let ssrText = "";
      if (Array.isArray(ssrs)) {
        ssrText = ssrs
          .map((s) => {
            const c   = (s.Code || s.code || "").toUpperCase();
            const txt = s.FreeText || s.freeText || "";
            return txt ? `${c} (${txt})` : c;
          })
          .filter(Boolean)
          .join(", ");
      } else if (typeof ssrs === "string") {
        ssrText = ssrs;
      }

      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${seat}</td>
        <td>${name}</td>
        <td>${dob ? dob.substring(0, 10) : ""}</td>
        <td>${age ? age : ""}</td>
        <td>${pnr}</td>
        <td>${statusLabel}</td>
        <td>${ssrText}</td>
      `;
      tbody.appendChild(tr);
    });

    // default sort: seat ascending
    sortPaxTable(0, true);
  }

  const viewPaxBtn = document.getElementById("btn-view-pax");
  if (viewPaxBtn && paxListModal) {
    viewPaxBtn.addEventListener("click", function () {
      if (!currentFlight) return;
      populatePassengerListModal(currentFlight.paxList || []);
      paxListModal.show();
    });
  }

  // ---------- Seatmap ----------
  const SSR_SPECIAL = new Set(["CKIN", "DEAF", "PETC", "VVIP", "UNZZ","UMNR"]);

  function hasWheelchairSSR(ssrs) {
    if (!Array.isArray(ssrs)) return false;
    return ssrs.some(s => {
      const code = (s.Code || s.code || "").toUpperCase();
      return code.startsWith("WC");
    });
  }

  function hasSpecialSSR(ssrs) {
    if (!Array.isArray(ssrs)) return false;
    return ssrs.some(s => {
      const code = (s.Code || s.code || "").toUpperCase();
      return SSR_SPECIAL.has(code);
    });
  }

  function hasInfantSSR(ssrs) {
    if (!Array.isArray(ssrs)) return false;
    return ssrs.some(s => {
      const code = (s.Code || s.code || "").toUpperCase();
      return code === "INFT" || code === "INF";
    });
  }

  const seatmapConfigs = {
    SF3_STD: {
      name: "SF340A",
      rows: [1,2,3,4,5,6,7,8,9,10,11],
      left: ["A"],
      right: ["B","C"],
      lastRowMode: "4-inline",
      hasRow0C: false,
    },
    SF3_CIZ: {
      name: "SF340B",
      rows: [1,2,3,4,5,6,7,8,9,10,11],
      left: ["A"],
      right: ["B","C"],
      lastRowMode: "4-inline",
      hasRow0C: false,
    },
    SF3_CIT: {
      name: "SF340A",
      rows: [0,1,2,3,4,5,6,7,8,9,10,11],
      left: ["A"],
      right: ["B","C"],
      lastRowMode: null,
      hasRow0C: true,
    },
    
    ATR72: {
      name: "ATR 72",
      rows: [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17],
      left: ["A","B"],
      right:["C","D"],
      lastRowMode: null,
      hasRow0C: false,
    },
  };

  function seatmapKeyForFlight(f) {
    const t = (f.aircraftType || "").toUpperCase();
    const r = (f.reg || "").toUpperCase();

    if (t.includes("ATR") || r.includes("MCU")) return "ATR72";

    const isSaab =
      t.includes("SAAB") ||
      t.includes("SF3") ||
      t.includes("SF34") ||
      t.includes("SF340") ||
      r.startsWith("ZK-CI") ||
      r.startsWith("ZK-KR");

    if (isSaab) {
      if (r === "ZK-CIT") return "SF3_CIT";
      if (r === "ZK-CIZ") return "SF3_CIZ";
      return "SF3_STD";
    }

    return null;
  }

  function buildSeatmapForFlight(f) {
    const container = document.getElementById("seatmap-container");
    const infoEl    = document.getElementById("seatmap-info");
    const titleEl   = document.getElementById("seatmapModalLabel");
    if (!container || !infoEl || !titleEl) return;

    container.innerHTML = "";
    infoEl.textContent = "Click a seat to see passenger details.";

    const key = seatmapKeyForFlight(f);
    const cfg = key ? seatmapConfigs[key] : null;

    if (!cfg) {
      titleEl.textContent = `Seat map — ${f.reg}`;
      infoEl.textContent  = "Seatmap not available for this aircraft type.";
      return;
    }

    titleEl.textContent = `Seat map — ${cfg.name} (${f.reg})`;

    const paxBySeat = {};
    (f.paxList || []).forEach(p => {
      const seat = (p.Seat || p.SeatNumber || "").toString().toUpperCase();
      if (seat) paxBySeat[seat] = p;
    });

    function makeSeat(rowNum, col) {
      const seatCode = `${rowNum}${col}`;
      const seatEl   = document.createElement("div");
      seatEl.className = "seat";
      seatEl.textContent = col;
      seatEl.dataset.seat = seatCode;

      const pax = paxBySeat[seatCode];

      if (pax) {
        const ssrsForSeat = getField(pax, ["Ssrs","SSRs","SpecialServiceRequests"], []);
        const paxType     = (pax.PassengerType || pax.passengerType || "").toUpperCase();
        const isUmnr      = hasUmnrSSR(ssrsForSeat);

        // --- Base colour: treat UMNR as a child visually ---
        if (isUmnr || paxType === "CH" || paxType === "CHD") {
          seatEl.classList.add("seat-child");
        } else if (hasInfantSSR(ssrsForSeat)) {
          seatEl.classList.add("seat-adult-infant");
        } else {
          seatEl.classList.add("seat-adult");
        }

        const iconsDiv = document.createElement("div");
        iconsDiv.className = "seat-icons";

        // UMNR child icon + red border
        if (isUmnr) {
          seatEl.classList.add("seat-umnr");

          const um = document.createElement("span");
          um.className = "seat-icon seat-icon-umnr";
          um.textContent = "🧒";   // small child emoji
          iconsDiv.appendChild(um);
        }

        if (hasSpecialSSR(ssrsForSeat)) {
          const sp = document.createElement("span");
          sp.className = "seat-icon seat-icon-special";
          sp.textContent = "!";
          iconsDiv.appendChild(sp);
        }

        if (hasWheelchairSSR(ssrsForSeat)) {
          const wc = document.createElement("span");
          wc.className = "seat-icon seat-icon-wheelchair";
          wc.textContent = "♿";
          iconsDiv.appendChild(wc);
        }

        if (iconsDiv.children.length > 0) {
          seatEl.appendChild(iconsDiv);
        }
      } else {
        seatEl.classList.add("seat-empty");
      }

      seatEl.addEventListener("click", function () {
        container.querySelectorAll(".seat.selected").forEach(el => el.classList.remove("selected"));
        seatEl.classList.add("selected");

        const pax = paxBySeat[seatCode];
        if (pax) {
          const name = [
            pax.NamePrefix || "",
            pax.GivenName || pax.FirstName || "",
            pax.Surname   || pax.LastName  || ""
          ].join(" ").trim();

          const dobRaw = pax.DateOfBirth || pax.BirthDate || "";
          const dob    = formatDob(dobRaw);
          const age    = calcAge(dobRaw);
          const pnr    = pax.BookingReferenceID || "";
          const ssrsArray = Array.isArray(pax.Ssrs) ? pax.Ssrs : [];
          let ssrs = "";
          if (ssrsArray.length) {
            ssrs = ssrsArray.map(s => {
              const code = s.Code || "";
              const text = s.FreeText || "";
              return text ? `${code} (${text})` : code;
            }).join(", ");
          }

          const isUmnr = hasUmnrSSR(ssrsArray);

          infoEl.innerHTML = `
            <div><strong>${seatCode}</strong></div>
            <div>${name}</div>
            <div>DOB: ${dob || "—"}${age ? ` (${age} yrs)` : ""}</div>
            <div>PNR: ${pnr || "—"}</div>
            <div>SSR: ${ssrs || "—"}</div>
            ${isUmnr ? `<div class="mt-1 text-danger"><strong>Unaccompanied minor</strong> 🧒</div>` : ""}
          `;
        } else {
          infoEl.innerHTML = `<strong>${seatCode}</strong><br>Empty seat`;
        }
      });

      return seatEl;
    }

    container.innerHTML = "";
    const lastRowNumber = cfg.rows[cfg.rows.length - 1];

    cfg.rows.forEach(rowNum => {
      const rowDiv = document.createElement("div");
      rowDiv.className = "seat-row";

      const label = document.createElement("div");
      label.className = "seat-label";
      label.textContent = rowNum;
      rowDiv.appendChild(label);

      const isRow0C          = cfg.hasRow0C && rowNum === 0;
      const isLastRow4Inline = cfg.lastRowMode === "4-inline" && rowNum === lastRowNumber;

      if (isRow0C) {
        const ghostA = document.createElement("div");
        ghostA.className = "seat";
        ghostA.style.visibility = "hidden";
        const ghostB = document.createElement("div");
        ghostB.className = "seat";
        ghostB.style.visibility = "hidden";

        const leftBlock = document.createElement("div");
        leftBlock.className = "seat-block";
        leftBlock.appendChild(ghostA);
        leftBlock.appendChild(ghostB);
        rowDiv.appendChild(leftBlock);

        const aisle = document.createElement("div");
        aisle.className = "seat-aisle";
        rowDiv.appendChild(aisle);

        const rightBlock = document.createElement("div");
        rightBlock.className = "seat-block";
        rightBlock.appendChild(makeSeat(rowNum, "C"));
        rowDiv.appendChild(rightBlock);

        container.appendChild(rowDiv);
        return;
      }

      if (isLastRow4Inline) {
        const block = document.createElement("div");
        block.className = "seat-block";
        ["A", "B", "C", "D"].forEach(col => block.appendChild(makeSeat(rowNum, col)));
        rowDiv.appendChild(block);
        container.appendChild(rowDiv);
        return;
      }

      const leftCols  = cfg.left.slice();
      const rightCols = cfg.right.slice();

      const leftBlock = document.createElement("div");
      leftBlock.className = "seat-block";
      leftCols.forEach(col => leftBlock.appendChild(makeSeat(rowNum, col)));
      rowDiv.appendChild(leftBlock);

      const aisle = document.createElement("div");
      aisle.className = "seat-aisle";
      rowDiv.appendChild(aisle);

      const rightBlock = document.createElement("div");
      rightBlock.className = "seat-block";
      rightCols.forEach(col => rightBlock.appendChild(makeSeat(rowNum, col)));
      rowDiv.appendChild(rightBlock);

      container.appendChild(rowDiv);
    });
  }

  const seatmapBtn = document.getElementById("btn-seatmap");
  if (seatmapBtn && seatmapModal) {
    seatmapBtn.addEventListener("click", function () {
      if (!currentFlight) return;
      buildSeatmapForFlight(currentFlight);
      seatmapModal.show();
    });
  }

  // ---------- Times / delay modal ----------
  function minutesBetween(t1, t2) {
    if (!t1 || !t2) return 0;
    return Math.round((t2 - t1) / 60000);
  }

  function recalcDelayAllocated() {
    const delayRows    = document.getElementById("delay-rows");
    const delayAllocEl = document.getElementById("delay-allocated");
    if (!delayRows || !delayAllocEl) return;
    const minsInputs = delayRows.querySelectorAll(".delay-minutes");
    let total = 0;
    minsInputs.forEach(inp => {
      const v = parseInt(inp.value || "0", 10);
      if (!isNaN(v)) total += v;
    });
    delayAllocEl.textContent = total.toString();
  }

    // ---------- Collect delay rows from modal ----------
function collectDelaysFromModal() {
  const rows = document.querySelectorAll("#delay-rows tr");
  const delays = [];
  const unknownCodes = new Set();
  const isArrival = (currentTimeMode === "arr");

  rows.forEach((tr) => {
    const codeInput   = tr.querySelector(".delay-code");
    const minsInput   = tr.querySelector(".delay-minutes");
    const remarkInput = tr.querySelector(".delay-remark");

    if (!codeInput || !minsInput) return;

    let code = (codeInput.value || "").trim();
    const mins = parseInt(minsInput.value || "0", 10);
    const remark = remarkInput ? (remarkInput.value || "").trim() : "";

    // Skip completely empty rows
    if (!code && !mins) return;

    // Normalise: strip non-digits and pad to two digits (e.g. "1" => "01")
    code = code.replace(/\D/g, "");
    if (!code) return;
    if (code.length === 1) code = "0" + code;

    if (isNaN(mins) || mins <= 0) return;

    const meta = DELAY_CODE_META[code];

    if (!meta) {
      unknownCodes.add(code);
      return;
    }

    delays.push({
      code: code,                    // "11", "81", etc.
      delayCodeId: meta.id,          // Envision ID from your table
      delayMinutes: mins,            // allocated minutes
      description: meta.description, // optional, for UI
      remark: remark,                // NEW
      isArrival: isArrival
    });
  });

  if (unknownCodes.size > 0) {
    alert(
      "Unknown delay code(s): " +
      Array.from(unknownCodes).join(", ") +
      "\nPlease fix these before saving."
    );
    return null; // abort save
  }

  return delays;
}

// --- Envision Time Helpers ---
function parseTimeToMinutes(hhmm) {
  if (!hhmm) return null;
  const [h, m] = hhmm.split(":").map(Number);
  if (Number.isNaN(h) || Number.isNaN(m)) return null;
  return h * 60 + m;
}

function renderCrewTable(crew, containerEl) {
  if (!containerEl) return;

  if (!crew || !crew.length) {
    containerEl.innerHTML =
      '<span class="text-muted">No operating crew found in Envision.</span>';
    return;
  }

  const rowsHtml = crew.map(c => `
    <div class="d-flex justify-content-between">
      <span>${c.position || ""}</span>
      <span>${c.name || ""}</span>
    </div>
  `).join("");

  containerEl.innerHTML = rowsHtml;
}



function updateCrewSidebar(flight) {
  const panel = document.getElementById("gantt-crew-body");
  if (!panel) return;

  if (!flight || !flight.envisionFlightId) {
    panel.innerHTML =
      '<span class="text-muted">No Envision flight ID linked – crew not available.</span>';
    return;
  }

  // Use cached crew if already loaded
  if (Array.isArray(flight.crew) && flight.crew.length) {
    renderCrewTable(flight.crew, panel);
    return;
  }

  panel.innerHTML =
    '<span class="text-muted">Loading crew from Envision…</span>';

  fetchEnvisionCrew(flight.envisionFlightId).then(crew => {
    if (!crew || !crew.length) {
      panel.innerHTML =
        '<span class="text-muted">No operating crew found in Envision.</span>';
      return;
    }

    // Cache on the flight object for reuse (modal + future clicks)
    flight.crew = crew;
    renderCrewTable(crew, panel);
  });
}

// Build a local wall-clock ISO "YYYY-MM-DDTHH:MM" from date + minutes
function datePlusMinutesToLocalIso(dateStr, minutes) {
  if (!dateStr || minutes == null) return null;
  const [y, mo, d] = dateStr.split("-").map(Number);
  const h = Math.floor(minutes / 60);
  const m = minutes % 60;
  const pad = n => String(n).padStart(2, "0");
  return `${y}-${pad(mo)}-${pad(d)}T${pad(h)}:${pad(m)}`;
}

function collectTimeModalPayload() {
  if (!currentFlight || !currentTimeMode) return null;

  // First, collect delay rows (and abort if any invalid codes)
  const delays = collectDelaysFromModal();
  if (delays === null) {
    // Invalid codes → do not send anything
    return null;
  }

  const getHm = (id) => {
    const el = document.getElementById(id);
    if (!el) return null;
    const v = (el.value || "").trim();
    return v && /^\d{1,2}:\d{2}$/.test(v) ? v : null;
  };

  const getDate = (id) => {
    const el = document.getElementById(id);
    if (!el) return null;
    const v = (el.value || "").trim();
    // HTML date: YYYY-MM-DD
    return v && /^\d{4}-\d{2}-\d{2}$/.test(v) ? v : null;
  };

  const base = {
    mode: currentTimeMode,
    dep: currentFlight.dep,
    ades: currentFlight.ades,
    designator: currentFlight.designator || "",
    flight_number:
      currentFlight.flightFull ||
      ((currentFlight.designator || "") + (currentFlight.flightNumeric || "")),
    std_sched: currentFlight.stdSched ? currentFlight.stdSched.toISOString() : null,
    sta_sched: currentFlight.staSched ? currentFlight.staSched.toISOString() : null,
    envision_flight_id: currentFlight.envisionFlightId || null,
    apg_plan_id: currentFlight.apgPlanId || null,
    delays: delays                      // <<< NEW: array of delay objects
  };

  if (currentTimeMode === "dep") {
    const depDate = getDate("dep-date");

    const etdHm   = getHm("time-etd");
    const offHm   = getHm("time-offblocks");
    const airHm   = getHm("time-airborne");

    const etdMinutes = parseTimeToMinutes(etdHm);
    const offMinutes = parseTimeToMinutes(offHm);
    let   airMinutes = parseTimeToMinutes(airHm);

    const etdLocalIso  = datePlusMinutesToLocalIso(depDate, etdMinutes);
    const offLocalIso  = datePlusMinutesToLocalIso(depDate, offMinutes);

    // Midnight crossing: 23:59 offblocks → 00:02 airborne next day
    let airLocalIso = null;
    if (airMinutes != null && offMinutes != null) {
      if (airMinutes < offMinutes) {
        airMinutes += 24 * 60;
      }
      airLocalIso = datePlusMinutesToLocalIso(depDate, airMinutes);
    }

    return {
      ...base,
      etd:       etdHm,
      offblocks: offHm,
      airborne:  airHm,
      dep_date:  depDate,
      arr_date:  null,
      etd_local:       etdLocalIso,
      offblocks_local: offLocalIso,
      airborne_local:  airLocalIso
    };
  } else {
    const arrDate = getDate("arr-date");

    const etaHm  = getHm("time-eta");
    const landHm = getHm("time-landing");
    const onHm   = getHm("time-onchocks");

    const etaMinutes  = parseTimeToMinutes(etaHm);
    const onMinutes   = parseTimeToMinutes(onHm);
    let   landMinutes = parseTimeToMinutes(landHm);

    const etaLocalIso = datePlusMinutesToLocalIso(arrDate, etaMinutes);
    const onLocalIso  = datePlusMinutesToLocalIso(arrDate, onMinutes);

    // Landing normally BEFORE on-blocks.
    if (landMinutes != null && onMinutes != null && landMinutes > onMinutes) {
      landMinutes -= 24 * 60; // previous day
    }
    const landLocalIso = datePlusMinutesToLocalIso(arrDate, landMinutes);

    return {
      ...base,
      eta:      etaHm,
      landing:  landHm,
      onchocks: onHm,
      dep_date: null,
      arr_date: arrDate,
      eta_local:      etaLocalIso,
      landing_local:  landLocalIso,
      onblocks_local: onLocalIso
    };
  }
}

function buildTimeModal(mode, flight) {
  currentTimeMode = mode;
  const core         = document.getElementById("time-modal-core");
  const delaySection = document.getElementById("delay-section");
  const delayRows    = document.getElementById("delay-rows");
  const delayReqEl   = document.getElementById("delay-required");
  const delayAllocEl = document.getElementById("delay-allocated");
  if (!core || !delaySection || !delayRows || !delayReqEl || !delayAllocEl) return;

  core.innerHTML = "";
  delayRows.innerHTML = "";
  delaySection.classList.add("d-none");
  delayReqEl.textContent   = "0";
  delayAllocEl.textContent = "0";

  const labelCode = `${flight.designator || ""}${flight.flightNumeric || flight.flightFull || ""}`.trim();
  const stdTxt = flight.stdSched ? fmtTime(flight.stdSched) : "—";
  const staTxt = flight.staSched ? fmtTime(flight.staSched) : "—";

  if (mode === "dep") {
    document.getElementById("timeModalLabel").textContent =
      `Departure Times — ${labelCode}`;

    core.innerHTML = `
      <div class="mb-2"><strong>${flight.dep} → ${flight.ades}</strong></div>
      <div class="mb-2 d-flex justify-content-between">
        <span>STD</span>
        <span>${stdTxt}</span>
      </div>

      <!-- ETD -->
      <div class="mb-2 d-flex justify-content-between align-items-center">
        <label class="me-2 mb-0">ETD</label>
        <input
          type="time"
          class="form-control form-control-sm w-auto"
          id="time-etd"
          value="${flight.stdEst ? fmtTime(flight.stdEst) : ""}">
      </div>

      <!-- Off blocks: DATE + TIME -->
      <div class="mb-2 d-flex justify-content-between align-items-center">
        <label class="me-2 mb-0">Off blocks</label>
        <div class="d-flex align-items-center gap-2">
          <input
            type="date"
            id="dep-date"
            class="form-control form-control-sm">
          <input
            type="time"
            class="form-control form-control-sm w-auto"
            id="time-offblocks">
        </div>
      </div>

      <!-- Airborne (uses same date as Off blocks for +1 logic) -->
      <div class="mb-2 d-flex justify-content-between align-items-center">
        <label class="me-2 mb-0">Airborne</label>
        <input
          type="time"
          class="form-control form-control-sm w-auto"
          id="time-airborne">
      </div>
    `;

    const depDateInput = document.getElementById("dep-date");
    if (depDateInput) {
      const defDate = formatDateYMD(
        flight.stdSched || flight.stdEst || new Date()
      );
      if (!depDateInput.value) depDateInput.value = defDate;
    }

    const etdInput      = document.getElementById("time-etd");
    const offInput      = document.getElementById("time-offblocks");
    const airborneInput = document.getElementById("time-airborne");

    function timeInputToDate(inputEl) {
      if (!flight.stdSched || !inputEl) return null;
      const raw = (
        inputEl.value ||
        inputEl.getAttribute("value") ||
        ""
      ).toString();

      const m = raw.match(/(\d{1,2}):(\d{2})/);
      if (!m) return null;

      const h  = parseInt(m[1], 10);
      const mi = parseInt(m[2], 10);

      const d = new Date(flight.stdSched);
      d.setHours(h, mi, 0, 0);
      return d;
    }

    function updateDelayDep() {
      if (!flight.stdSched) {
        delaySection.classList.add("d-none");
        delayReqEl.textContent   = "0";
        delayAllocEl.textContent = "0";
        return;
      }

      const offDt = timeInputToDate(offInput); // ATD

      // If no ATD entered, no delay required
      if (!offDt) {
        delaySection.classList.add("d-none");
        delayReqEl.textContent   = "0";
        delayAllocEl.textContent = "0";
        return;
      }

      let delayMins = minutesBetween(flight.stdSched, offDt);

      // Only positive delay counts – early departures shouldn't show delay
      if (delayMins < 0) {
        delayMins = 0;
      }

      if (delayMins > 15) {
        delaySection.classList.remove("d-none");
        delayReqEl.textContent = String(delayMins);
        recalcDelayAllocated();
      } else {
        delaySection.classList.add("d-none");
        delayReqEl.textContent   = "0";
        delayAllocEl.textContent = "0";
      }
    }


    if (etdInput) {
      etdInput.addEventListener("change", updateDelayDep);
      etdInput.addEventListener("input",  updateDelayDep);
    }
    if (offInput) {
      offInput.addEventListener("change", updateDelayDep);
      offInput.addEventListener("input",  updateDelayDep);
    }

    if (typeof fetchEnvisionTimes === "function" && flight.envisionFlightId) {
      fetchEnvisionTimes(flight.envisionFlightId).then(data => {
        if (!data || !data.local_hm) return;
        const hm = data.local_hm;

        if (hm.departureActual && offInput && !offInput.value) {
          offInput.value = hm.departureActual;
        }
        if (hm.departureTakeOff && airborneInput && !airborneInput.value) {
          airborneInput.value = hm.departureTakeOff;
        }
        if (hm.departureActual && etdInput && !etdInput.value) {
          etdInput.value = hm.departureActual;
        }

        updateDelayDep();
      });
    }

  } else {
    document.getElementById("timeModalLabel").textContent =
      `Arrival Times — ${labelCode}`;

    core.innerHTML = `
      <div class="mb-2"><strong>${flight.dep} → ${flight.ades}</strong></div>
      <div class="mb-2 d-flex justify-content-between">
        <span>STA</span>
        <span>${staTxt}</span>
      </div>

      <!-- ETA -->
      <div class="mb-2 d-flex justify-content-between align-items-center">
        <label class="me-2 mb-0">ETA</label>
        <input
          type="time"
          class="form-control form-control-sm w-auto"
          id="time-eta"
          value="${flight.staEst ? fmtTime(flight.staEst) : ""}">
      </div>

      <!-- Landing -->
      <div class="mb-2 d-flex justify-content-between align-items-center">
        <label class="me-2 mb-0">Landing</label>
        <input
          type="time"
          class="form-control form-control-sm w-auto"
          id="time-landing">
      </div>

      <!-- On chocks: DATE + TIME -->
      <div class="mb-2 d-flex justify-content-between align-items-center">
        <label class="me-2 mb-0">On chocks</label>
        <div class="d-flex align-items-center gap-2">
          <input
            type="date"
            id="arr-date"
            class="form-control form-control-sm">
          <input
            type="time"
            class="form-control form-control-sm w-auto"
            id="time-onchocks">
        </div>
      </div>
    `;

    const arrDateInput = document.getElementById("arr-date");
    if (arrDateInput) {
      const defDate = formatDateYMD(
        flight.staSched || flight.staEst || new Date()
      );
      if (!arrDateInput.value) arrDateInput.value = defDate;
    }

    const etaInput      = core.querySelector("#time-eta");
    const landingInput  = core.querySelector("#time-landing");
    const onChocksInput = core.querySelector("#time-onchocks");

    function updateDelayArr() {
      const etaVal = etaInput ? etaInput.value : "";
      if (!flight.staSched || !etaVal) {
        delaySection.classList.add("d-none");
        delayReqEl.textContent   = "0";
        delayAllocEl.textContent = "0";
        return;
      }
      const [h, m] = etaVal.split(":");
      const etaDt  = new Date(flight.staSched);
      etaDt.setHours(parseInt(h,10), parseInt(m,10), 0, 0);

      const diff = minutesBetween(flight.staSched, etaDt);
      if (Math.abs(diff) > 15) {
        delaySection.classList.remove("d-none");
        delayReqEl.textContent = diff.toString();
        recalcDelayAllocated();
      } else {
        delaySection.classList.add("d-none");
        delayReqEl.textContent   = "0";
        delayAllocEl.textContent = "0";
      }
    }

    if (etaInput) etaInput.addEventListener("change", updateDelayArr);

    if (typeof fetchEnvisionTimes === "function" && flight.envisionFlightId) {
      fetchEnvisionTimes(flight.envisionFlightId).then(data => {
        if (!data || !data.local_hm) return;
        const hm = data.local_hm;

        if (hm.arrivalLanded && landingInput && !landingInput.value) {
          landingInput.value = hm.arrivalLanded;
        }
        if (hm.arrivalActual && onChocksInput && !onChocksInput.value) {
          onChocksInput.value = hm.arrivalActual;
        }
        if (hm.arrivalActual && etaInput && !etaInput.value) {
          etaInput.value = hm.arrivalActual;
        }

        updateDelayArr();
      });
    }
  }
} // <--- close buildTimeModal here

// === Delay row add/handlers (outside buildTimeModal) ===
const addDelayBtn = document.getElementById("btn-add-delay");
if (addDelayBtn) {
  addDelayBtn.addEventListener("click", function () {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td style="width: 70px;">
        <input type="text"
               class="form-control form-control-sm delay-code"
               maxlength="4">
      </td>
      <td style="width: 90px;">
        <input type="number"
               class="form-control form-control-sm text-end delay-minutes"
               min="0">
      </td>
      <td>
        <input type="text"
               class="form-control form-control-sm delay-reason"
               placeholder="Auto from code"
               readonly>
      </td>
      <td>
        <input type="text"
               class="form-control form-control-sm delay-remark"
               placeholder="Optional remark">
      </td>
      <td class="text-end" style="width: 40px;">
        <button type="button"
                class="btn btn-sm btn-link text-danger p-0 btn-remove-delay">&times;</button>
      </td>
    `;

    const delayRows = document.getElementById("delay-rows");
    if (delayRows) {
      delayRows.appendChild(tr);
      attachDelayRowHandlers(tr);
      recalcDelayAllocated();
    }
  });
}

function attachDelayRowHandlers(tr) {
  const codeInput   = tr.querySelector(".delay-code");
  const minsInput   = tr.querySelector(".delay-minutes");
  const reasonInput = tr.querySelector(".delay-reason");
  const remBtn      = tr.querySelector(".btn-remove-delay");

  if (remBtn) {
    remBtn.addEventListener("click", function () {
      tr.remove();
      recalcDelayAllocated();
    });
  }

  if (minsInput) {
    minsInput.addEventListener("input", recalcDelayAllocated);
  }

  if (codeInput && reasonInput) {
    codeInput.addEventListener("input", function () {
      let raw = (codeInput.value || "").trim();
      raw = raw.replace(/\D/g, "");
      if (!raw) {
        reasonInput.value = "";
        return;
      }
      if (raw.length === 1) raw = "0" + raw;

      const meta = DELAY_CODE_META[raw];
      reasonInput.value = meta ? (meta.description || "") : "";
    });
  }
}

  if (ctxDepartureBtn) {
    ctxDepartureBtn.addEventListener("click", function () {
      hideContextMenu();
      if (!currentFlight || !timeModal) return;
      buildTimeModal("dep", currentFlight);
      timeModal.show();
    });
  }
  if (ctxArrivalBtn) {
    ctxArrivalBtn.addEventListener("click", function () {
      hideContextMenu();
      if (!currentFlight || !timeModal) return;
      buildTimeModal("arr", currentFlight);
      timeModal.show();
    });
  }
  const saveTimesBtn = document.getElementById("btn-save-times");
if (saveTimesBtn) {
  saveTimesBtn.addEventListener("click", async function () {
    const payload = collectTimeModalPayload();
    if (!payload) {
      alert("No flight / times to save.");
      return;
    }

    console.log("[TIMES] save payload:", payload);

    let resp;
    let data = null;

    try {
      resp = await fetch(SAVE_TIMES_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      // Always log status
      console.log("[TIMES] HTTP status:", resp.status, resp.statusText);

      // Try to parse JSON, but don't crash if it's empty / HTML
      try {
        data = await resp.json();
      } catch (e) {
        console.warn("[TIMES] response was not JSON or was empty");
      }

      console.log("[TIMES] save response body:", data);

      if (!resp.ok) {
        const msg =
          (data && data.error) ||
          `HTTP ${resp.status} ${resp.statusText}` ||
          "Unknown error";
        alert("Backend reported an error saving times: " + msg);
        throw new Error(msg);
      }

      if (!data || !data.ok) {
        const msg =
          (data && data.error) ||
          "Backend returned ok=false with no details";
        alert("Backend reported an error saving times: " + msg);
        throw new Error(msg);
      }

      // Success path – you can add a toast here
      // e.g. showToast("Times saved to Envision");
    } catch (err) {
      console.error("[TIMES] save error:", err);
      alert("Error saving times: " + (err.message || err));
    } finally {
      if (timeModal) timeModal.hide();
    }
  });
}

// ---------- Auto-refresh from backend ----------
// manual=true  → show loading bar + update "Loaded" date
// manual=false → silent auto-refresh (no spinner, no loaded date change)
function refreshGantt({ manual = false } = {}) {
  const dateInput = document.querySelector('input[name="date"]');
  if (!dateInput) return;
  const dayStr = dateInput.value || "";

  // Only show the loading overlay for manual loads
  if (manual) {
    setGanttLoading(true);
  }

  fetch(`${GANTT_URL}?date=${encodeURIComponent(dayStr)}`)
    .then(r => r.json())
    .then(data => {
      if (!data.ok || !Array.isArray(data.results)) return;
      const newFlights = buildFlightsFromRows(data.results);
      renderGanttFromFlights(newFlights);

      // "Loaded" date should only change on manual loads
      if (manual && dayStr) {
        updateLoadedDateLabel(dayStr);
      }

      // Last refresh updates for *every* load (manual + auto)
      updateLastRefreshLabel();
    })
    .catch(err => {
      console.error("Gantt refresh failed:", err);
    })
    .finally(() => {
      if (manual) {
        setGanttLoading(false);
      }
    });
}

// Make this a global so you can call it from buttons
window.previewManifestForRow = async function (f) {
  if (!f) {
    alert("No flight selected for manifest preview.");
    return;
  }

  // ----- Designator -----
  let designator = (f.designator || "").toUpperCase();

  // ----- Flight number (numeric part) -----
  let number = "";

  // 1) If we have a numeric field, use that
  if (f.flightNumeric) {
    number = String(f.flightNumeric);
  }
  // 2) If we have a "flight_number" field (your logged object), use that
  else if (f.flight_number) {
    let raw = String(f.flight_number).toUpperCase().trim();
    // Strip designator if it’s included in the value
    if (designator && raw.startsWith(designator)) {
      raw = raw.slice(designator.length);
    }
    number = raw;
  }
  // 3) Fallback: parse from flight/flightFull text if they exist
  else {
    const fullNo =
      (f.flightFull ||
        f.flight ||
        ""
      ).toString().replace(/\s+/g, "").toUpperCase();

    if (fullNo) {
      const m = fullNo.match(/(\d+)$/); // trailing digits
      if (m) {
        number = m[1];
      }
    }
  }

  if (!designator || !number) {
    console.error("[Manifest preview] Missing designator/number", {
      designator,
      fullNo: f.flightFull || f.flight || f.flight_number || "",
      numeric: number,
      flight: f,
    });
    alert("Cannot determine flight designator/number for manifest preview.");
    return;
  }

  // ----- Date -----
  // Prefer explicit date on the object, else derive from STD
  const dateStr = f.date || formatDateYMD(f.stdEst || f.stdSched);

  // ----- Envision flight ID (any shape we can get) -----
  const envisionFlightId =
    f.envisionFlightId || // from Gantt object
    f.envision_flight_id || // if snake_case somewhere
    null;

  const payload = {
    dep: (f.dep || "").toUpperCase(),
    ades: (f.ades || "").toUpperCase(),
    date: dateStr,
    designator: designator,
    number: number,                           // numeric part
    reg: (f.reg || "").toUpperCase(),
    envision_flight_id: envisionFlightId,     // may be null in some views
  };

  console.log("➡️ Sending payload to API:", payload);

  try {
    const resp = await fetch("/api/dcs/manifest_preview", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    if (!resp.ok) {
      const text = await resp.text();
      console.error("Manifest preview error:", resp.status, text);
      alert("Error building manifest preview: " + text);
      return;
    }

    const data = await resp.json();
    if (!data.ok) {
      alert("Manifest preview error: " + (data.error || "Unknown error"));
      return;
    }

    // Simple: open manifest HTML in a new tab
    const w = window.open("", "_blank");
    w.document.open();
    w.document.write(data.html);
    w.document.close();
  } catch (err) {
    console.error("Manifest preview fetch failed:", err);
    alert("Error calling manifest preview API: " + err.message);
  }
};


// ---------- Manifest preview button (uses currentFlight/paxList) ----------
const previewBtn = document.getElementById("btn-preview-manifest");
if (previewBtn) {
  previewBtn.addEventListener("click", function () {
    if (!currentFlight) {
      alert("Please select a flight on the Gantt first.");
      return;
    }

    // 🔹 Don’t rebuild a thin object – just pass the real flight
    window.previewManifestForRow(currentFlight);
  });
}


  // ---------- Initial build from DOM ----------
  if (dataNodes.length) {
    const domRows = buildRowsFromDom(dataNodes);
    renderGanttFromFlights(buildFlightsFromRows(domRows));
  }

    // ---------- Hook up date form + auto-refresh ----------
  const dateForm  = document.querySelector('form[method="get"]');
  const dateInputCtrl = document.querySelector('input[name="date"]');

  // Initial load is treated as a MANUAL load:
  //  - show spinner
  //  - update "Loaded" date
  refreshGantt({ manual: true });

  // Change date → manual reload (spinner + loaded date update)
  if (dateInputCtrl) {
    dateInputCtrl.addEventListener("change", function () {
      refreshGantt({ manual: true });
    });
  }

  // Submit "Load" button → manual reload (spinner + loaded date update)
  if (dateForm) {
    dateForm.addEventListener("submit", function (ev) {
      ev.preventDefault();
      refreshGantt({ manual: true });
    });
  }

  // Background auto-refresh every 60s (silent: no spinner, no "Loaded" change)
  setInterval(() => {
    refreshGantt({ manual: false });
  }, 60_000);
});
