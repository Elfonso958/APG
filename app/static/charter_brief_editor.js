(() => {
  const app = document.getElementById("briefEditor");
  if (!app) return;
  const $ = (id) => document.getElementById(id);
  const esc = (value) => String(value ?? "").replace(/[&<>'"]/g, (char) => ({ "&":"&amp;", "<":"&lt;", ">":"&gt;", "'":"&#39;", '"':"&quot;" }[char]));
  let details = {};
  try { details = JSON.parse(app.dataset.details || "{}"); } catch (_) { details = {}; }
  const lists = ["sectors", "crew", "accommodation", "transport", "ports"];
  const fields = {
    sectors:["date", "source_flight_id", "flight", "dep", "arr", "std", "sta", "aircraft", "flight_type", "passenger_info", "crew_codes", "notes"],
    crew:["code", "name", "role", "phone", "hotel", "notes"],
    accommodation:["date", "location", "hotel", "address", "phone", "notes"],
    transport:["date", "location", "time", "service", "contact", "details"],
    ports:["airport", "source", "notes"],
  };
  lists.forEach((name) => { if (!Array.isArray(details[name])) details[name] = []; });
  const timeValue = (value) => (String(value || "").match(/T(\d\d:\d\d)/) || [])[1] || String(value || "");
  const input = (field, value) => `<input type="${["std", "sta"].includes(field) ? "time" : "text"}" data-field="${field}" value="${esc(["std", "sta"].includes(field) ? timeValue(value) : value)}">`;
  const readOnly = (field, value) => `<span class="duty-readonly">${esc(["std", "sta"].includes(field) ? timeValue(value) : value || "—")}</span>`;
  const longDate = (value) => { const date = new Date(`${value}T12:00:00`); return Number.isNaN(date.getTime()) ? value : new Intl.DateTimeFormat("en-NZ", { weekday:"long", day:"numeric", month:"long" }).format(date); };
  const passengerInfo = (flight) => {
    const types = [flight.adt && `${flight.adt} AD`, flight.chd && `${flight.chd} CHD`, flight.inf && `${flight.inf} INF`].filter(Boolean);
    if (flight.pax_count) return `${flight.pax_count} pax${types.length ? ` · ${types.join(" · ")}` : ""}`;
    return flight.charter_manifest_uploaded ? "Manifest uploaded · 0 pax" : "No manifest uploaded";
  };
  function renderSectors() {
    const displayFields = fields.sectors.filter((field) => !["date", "source_flight_id"].includes(field));
    const byDay = new Map();
    details.sectors.forEach((sector) => { const day = sector.date || "Date TBC"; if (!byDay.has(day)) byDay.set(day, []); byDay.get(day).push(sector); });
    $("sectorsRows").innerHTML = [...byDay.entries()].map(([day, sectors]) => `<tr class="duty-date-heading"><td colspan="11">${esc(longDate(day))}</td></tr>${sectors.map((sector) => `<tr data-list-row="sectors">${displayFields.map((field) => `<td>${readOnly(field, field === "notes" && sector.notes === "Imported from Envision" ? "" : sector[field])}</td>`).join("")}<td><button type="button" class="remove-row">Remove</button></td></tr>`).join("")}`).join("");
  }
  const isEnvisionCrew = (crew) => crew.source === "envision" || crew.notes === "Assigned in Envision";
  function renderTable(name) { $( `${name}Rows` ).innerHTML = details[name].map((row) => `<tr data-list-row="${name}">${fields[name].map((field) => `<td>${input(field, name === "crew" && field === "notes" && isEnvisionCrew(row) ? "" : row[field])}</td>`).join("")}<td><button type="button" class="remove-row">×</button></td></tr>`).join(""); }
  function renderCards(name) { $( `${name}Rows` ).innerHTML = details[name].map((row) => `<article class="brief-item" data-list-row="${name}"><div class="brief-item-grid">${fields[name].map((field) => `<label>${field.replace(/_/g, " ").replace(/\b\w/g, (char) => char.toUpperCase())}${input(field, row[field])}</label>`).join("")}</div><button type="button" class="remove-row">×</button></article>`).join(""); }
  function render() { renderSectors(); renderTable("crew"); renderCards("accommodation"); renderCards("transport"); renderCards("ports"); $("operationsNotes").value = details.operations_notes || ""; $("crewNotes").value = details.crew_notes || ""; }
  app.addEventListener("click", async (event) => {
    const add = event.target.closest(".add-row");
    if (add) { details[add.dataset.list].push({}); render(); return; }
    const row = event.target.closest(".remove-row")?.closest("[data-list-row]");
    if (!row) return;
    const name = row.dataset.listRow;
    const index = [...document.querySelectorAll(`[data-list-row="${name}"]`)].indexOf(row);
    if (index >= 0) details[name].splice(index, 1);
    if (name === "sectors") await reconcileAssignedCrew();
    render();
  });
  $("briefForm").addEventListener("submit", () => {
    lists.filter((name) => name !== "sectors").forEach((name) => { details[name] = [...document.querySelectorAll(`[data-list-row="${name}"]`)].map((row) => Object.fromEntries(fields[name].map((field) => [field, row.querySelector(`[data-field="${field}"]`)?.value.trim() || ""]))); });
    details.operations_notes = $("operationsNotes").value.trim(); details.crew_notes = $("crewNotes").value.trim(); $("detailsJson").value = JSON.stringify(details);
  });

  const dialog = $("envisionFlightsDialog"), flightList = $("envisionFlightsList"), flightStatus = $("envisionFlightsStatus"), from = $("envisionFrom"), to = $("envisionTo");
  let flights = [];
  const timeLabel = (value) => { if (!value) return "TBC"; const date = new Date(value); return Number.isNaN(date.getTime()) ? String(value) : new Intl.DateTimeFormat("en-NZ", { hour:"2-digit", minute:"2-digit", hour12:true }).format(date); };
  function dateRange(start, end) { const out = [], cursor = new Date(`${start}T00:00:00`), finish = new Date(`${end}T00:00:00`); while (cursor <= finish && out.length <= 14) { out.push(cursor.toISOString().slice(0, 10)); cursor.setDate(cursor.getDate() + 1); } return out; }
  function renderFlightSelector() {
    const days = new Map();
    flights.forEach((flight, index) => { const day = flight._briefDate, reg = String(flight.reg || flight.registration || "Aircraft TBC").toUpperCase(); if (!days.has(day)) days.set(day, new Map()); const regs = days.get(day); if (!regs.has(reg)) regs.set(reg, []); regs.get(reg).push({ flight, index }); });
    flightList.innerHTML = [...days.entries()].map(([day, regs]) => `<section class="envision-day-group"><h3>${esc(day)}</h3>${[...regs.entries()].map(([reg, items]) => `<section class="envision-aircraft-group"><label class="envision-aircraft-heading"><input type="checkbox" data-aircraft="${esc(day)}|${esc(reg)}"><strong>${esc(reg)}</strong><span>${items.length} scheduled sector${items.length === 1 ? "" : "s"}</span></label>${items.map(({ flight, index }) => `<label class="envision-flight-row"><input type="checkbox" data-envision-flight="${index}"><span><strong>${esc(flight.flight_number || flight.flight || "Flight TBC")}</strong> · ${esc(flight.dep || flight.adep || "---")} – ${esc(flight.ades || flight.dest || "---")}</span><span>${esc(timeLabel(flight.std_nz || flight.std))}</span></label>`).join("")}</section>`).join("")}</section>`).join("") || '<p class="brief-empty">No flights were returned for this range.</p>';
  }
  async function fetchDays(days) { const sets = await Promise.all(days.map(async (day) => { const response = await fetch(`${app.dataset.ganttUrl}?date=${encodeURIComponent(day)}`); const data = await response.json(); if (!response.ok || data.ok === false) throw Error(data.error || `Unable to load ${day}`); return (data.results || data.rows || []).map((flight) => ({ ...flight, _briefDate:day })); })); return sets.flat(); }
  async function loadRange() { const days = dateRange(from.value, to.value); if (!from.value || !to.value || !days.length) return alert("Enter a valid charter date range of up to 14 days."); flightStatus.textContent = "Loading scheduled flights from Envision…"; flightList.innerHTML = ""; try { flights = await fetchDays(days); flightStatus.textContent = `${flights.length} scheduled flights from ${from.value} to ${to.value}. Select the sectors to include.`; renderFlightSelector(); } catch (error) { flightStatus.textContent = error.message; } }
  $("loadFlights").addEventListener("click", () => { from.value = document.querySelector('[name="start_date"]').value; to.value = document.querySelector('[name="end_date"]').value || from.value; dialog.showModal(); loadRange(); });
  $("refreshEnvisionFlights").addEventListener("click", loadRange); $("closeEnvisionFlights").addEventListener("click", () => dialog.close());
  flightList.addEventListener("change", (event) => { const key = event.target.dataset.aircraft; if (!key) return; flightList.querySelectorAll("[data-envision-flight]").forEach((box) => { const flight = flights[Number(box.dataset.envisionFlight)]; const flightKey = `${flight?._briefDate}|${String(flight?.reg || flight?.registration || "Aircraft TBC").toUpperCase()}`; if (flightKey === key) box.checked = event.target.checked; }); });
  $("selectAllEnvisionFlights").addEventListener("click", () => flightList.querySelectorAll('input[type="checkbox"]').forEach((box) => box.checked = true));
  async function assignedCrew(selected, sectors) {
    const known = new Set(details.crew.map((crew) => String(crew.code || crew.name || "").toUpperCase()));
    const results = await Promise.all(selected.map(async (flight) => { const id = flight.envision_flight_id || flight.id || flight.flight_id; if (!id) return []; try { const response = await fetch(`${app.dataset.flightCrewUrl}?flight_id=${encodeURIComponent(id)}&compact=1`); const data = await response.json(); return response.ok && data.ok !== false ? (data.crew || []) : []; } catch (_) { return []; } }));
    results.forEach((crewList, index) => { const codes = []; crewList.forEach((crew) => { const code = String(crew.employee_no || crew.employeeNo || crew.code || "").toUpperCase(), name = String(crew.name || ""), key = code || name.toUpperCase(); if (code) codes.push(code); if (!key || known.has(key)) return; details.crew.push({ code, name, role:crew.position || "", phone:"", hotel:"", notes:"", source:"envision" }); known.add(key); }); sectors[index].crew_codes = codes.join(", "); });
  }
  async function reconcileAssignedCrew() {
    // Preserve manually added supplementary crew, then rebuild the Envision crew
    // from the sectors that still belong to this brief.
    details.crew = details.crew.filter((crew) => !isEnvisionCrew(crew));
    const remaining = details.sectors.filter((sector) => sector.source_flight_id);
    remaining.forEach((sector) => { sector.crew_codes = ""; });
    await assignedCrew(remaining.map((sector) => ({ envision_flight_id:sector.source_flight_id })), remaining);
  }
  $("addSelectedEnvisionFlights").addEventListener("click", async () => {
    const selected = [...flightList.querySelectorAll('[data-envision-flight]:checked')].map((box) => flights[Number(box.dataset.envisionFlight)]).filter(Boolean); if (!selected.length) return alert("Select at least one flight.");
    const button = $("addSelectedEnvisionFlights"); button.disabled = true; button.textContent = "Adding crew…";
    const added = selected.map((flight) => ({ date:flight._briefDate, source_flight_id:flight.envision_flight_id || flight.id || flight.flight_id || "", flight:flight.flight_number || flight.flight || "", dep:flight.dep || flight.adep || "", arr:flight.ades || flight.dest || "", std:flight.std_nz || flight.std || "", sta:flight.sta_nz || flight.sta || "", aircraft:flight.reg || flight.registration || "", flight_type:flight.flight_type || flight.service_type || "", passenger_info:passengerInfo(flight), crew_codes:"", notes:"" }));
    details.sectors.push(...added); await assignedCrew(selected, added); render(); dialog.close(); button.disabled = false; button.textContent = "Add selected flights";
  });
  $("refreshTourData").addEventListener("click", async () => {
    const button = $("refreshTourData"), days = [...new Set(details.sectors.map((sector) => sector.date).filter(Boolean))]; if (!days.length) return alert("Select Envision flights first.");
    button.disabled = true; button.classList.add("is-loading");
    try {
      const feed = await fetchDays(days), refreshed = [], selected = [];
      details.sectors.forEach((sector) => {
        const flight = feed.find((row) => String(row.envision_flight_id || row.id || row.flight_id || "") === String(sector.source_flight_id || "")) || feed.find((row) => row._briefDate === sector.date && String(row.flight_number || row.flight || "") === String(sector.flight || ""));
        if (!flight) return;
        Object.assign(sector, { source_flight_id:flight.envision_flight_id || flight.id || flight.flight_id || sector.source_flight_id, flight:flight.flight_number || flight.flight || sector.flight, dep:flight.dep || flight.adep || sector.dep, arr:flight.ades || flight.dest || sector.arr, std:flight.std_nz || flight.std || sector.std, sta:flight.sta_nz || flight.sta || sector.sta, aircraft:flight.reg || flight.registration || sector.aircraft, flight_type:flight.flight_type || flight.service_type || sector.flight_type, passenger_info:passengerInfo(flight), crew_codes:"", notes:"" });
        refreshed.push(sector); selected.push(flight);
      });
      details.crew = details.crew.filter((crew) => !isEnvisionCrew(crew)); await assignedCrew(selected, refreshed); render();
    } catch (error) { alert(error.message); } finally { button.disabled = false; button.classList.remove("is-loading"); }
  });
  $("addSupplementaryCrew").addEventListener("click", async () => { const code = window.prompt("Enter the supplementary crew member's crew code:"); if (!code) return; try { const response = await fetch(`${app.dataset.crewLookupUrl}?crew_code=${encodeURIComponent(code.trim().toUpperCase())}`); const data = await response.json(); if (!response.ok || data.ok === false) throw Error(data.error || "Crew member not found"); const crew = data.crew; if (details.crew.some((item) => String(item.code || "").toUpperCase() === crew.code)) return alert(`${crew.code} is already on this brief.`); details.crew.push({ code:crew.code, name:crew.name, role:"Supplementary", phone:crew.phone || "", hotel:"", notes:"Supplementary crew" }); render(); } catch (error) { alert(error.message); } });
  render();
})();
