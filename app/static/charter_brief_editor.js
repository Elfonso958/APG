(() => {
  const app = document.getElementById("briefEditor");
  if (!app) return;
  const esc = (value) => String(value ?? "").replace(/[&<>'"]/g, (char) => ({"&":"&amp;","<":"&lt;",">":"&gt;","'":"&#39;",'"':"&quot;"}[char]));
  let details = {};
  try { details = JSON.parse(app.dataset.details || "{}"); } catch (_) { details = {}; }
  const lists = ["sectors", "crew", "accommodation", "transport", "ports"];
  lists.forEach((name) => { if (!Array.isArray(details[name])) details[name] = []; });
  const fields = {
    sectors:["date","report","flight","dep","arr","std","sta","aircraft","notes"],
    crew:["name","role","phone","hotel","notes"],
    accommodation:["location","hotel","address","phone","notes"],
    transport:["location","time","service","contact","details"],
    ports:["airport","source","notes"],
  };
  const input = (list, field, value, type="text") => `<input type="${type}" data-field="${field}" value="${esc(value)}" aria-label="${field}">`;
  function removeButton() { return '<button type="button" class="remove-row" title="Remove this item">×</button>'; }
  function renderTable(list) {
    const node = document.getElementById(`${list}Rows`);
    node.innerHTML = details[list].map((item) => `<tr data-list-row="${list}">${fields[list].map((field) => `<td>${input(list, field, item[field])}</td>`).join("")}<td>${removeButton()}</td></tr>`).join("");
  }
  function renderCards(list) {
    const node = document.getElementById(`${list}Rows`);
    node.innerHTML = details[list].map((item) => `<article class="brief-item" data-list-row="${list}"><div class="brief-item-grid">${fields[list].map((field) => `<label>${field.replace(/\b\w/g, (c) => c.toUpperCase())}${input(list, field, item[field])}</label>`).join("")}</div>${removeButton()}</article>`).join("");
  }
  function render() { renderTable("sectors"); renderTable("crew"); renderCards("accommodation"); renderCards("transport"); renderCards("ports"); document.getElementById("operationsNotes").value = details.operations_notes || ""; document.getElementById("crewNotes").value = details.crew_notes || ""; }
  function addRow(list, value={}) { details[list].push(value); render(); }
  app.addEventListener("click", (event) => {
    const add = event.target.closest(".add-row");
    if (add) { addRow(add.dataset.list); return; }
    const remove = event.target.closest(".remove-row");
    if (!remove) return;
    const row = remove.closest("[data-list-row]");
    const list = row?.dataset.listRow;
    const rows = [...document.querySelectorAll(`[data-list-row="${list}"]`)];
    const index = rows.indexOf(row);
    if (index >= 0) details[list].splice(index, 1);
    render();
  });
  document.getElementById("briefForm").addEventListener("submit", () => {
    lists.forEach((list) => {
      details[list] = [...document.querySelectorAll(`[data-list-row="${list}"]`)].map((row) => Object.fromEntries(fields[list].map((field) => [field, row.querySelector(`[data-field="${field}"]`)?.value.trim() || ""])));
    });
    details.operations_notes = document.getElementById("operationsNotes").value.trim();
    details.crew_notes = document.getElementById("crewNotes").value.trim();
    document.getElementById("detailsJson").value = JSON.stringify(details);
  });
  document.getElementById("loadFlights").addEventListener("click", async () => {
    const date = document.querySelector('[name="start_date"]').value;
    if (!date) return alert("Set the charter start date first.");
    const button = document.getElementById("loadFlights"); button.disabled = true; button.textContent = "Loading…";
    try {
      const response = await fetch(`${app.dataset.ganttUrl}?date=${encodeURIComponent(date)}`);
      const data = await response.json();
      if (!response.ok || data.ok === false) throw Error(data.error || "Unable to load Envision flights");
      const flights = data.results || data.rows || [];
      const charterFlights = flights.filter((flight) => /charter/i.test(`${flight.service_type || ""} ${flight.flight_type || ""} ${flight.flight_number || flight.flight || ""}`));
      const selected = charterFlights.length ? charterFlights : flights;
      if (!selected.length) throw Error("No flights were found for this date.");
      if (!window.confirm(`Add ${selected.length} flight${selected.length === 1 ? "" : "s"} to this tour of duty?`)) return;
      details.sectors.push(...selected.map((flight) => ({ date, report:"", flight:flight.flight_number || flight.flight || "", dep:flight.dep || flight.adep || "", arr:flight.ades || flight.dest || "", std:flight.std_nz || flight.std || "", sta:flight.sta_nz || flight.sta || "", aircraft:flight.reg || "", notes:"Imported from Envision" })));
      render();
    } catch (error) { alert(error.message); } finally { button.disabled = false; button.textContent = "Load Envision flights"; }
  });
  render();
})();
