(() => {
  const body = document.getElementById('requestSectorRows');
  const form = document.getElementById('requestForm');
  if (!body || !form) return;

  // Airport co-ordinates allow a practical block-time estimate without an external API.
  const airports = {
    AKL: [-37.008, 174.792], WLG: [-41.327, 174.805], CHC: [-43.489, 172.532], DUD: [-45.928, 170.198],
    IVC: [-46.412, 168.313], ZQN: [-45.021, 168.739], ROT: [-38.109, 176.317], TRG: [-37.672, 176.197],
    NPL: [-39.008, 174.179], NPE: [-39.465, 176.870], PMR: [-40.320, 175.617], NSN: [-41.299, 173.221],
    WSZ: [-41.739, 171.580], TUO: [-38.740, 176.084], GIS: [-38.663, 177.978], KKE: [-35.262, 173.912],
    WAG: [-39.962, 175.025], TEU: [-45.533, 167.650], TBU: [-21.241, -175.149], VAV: [-18.586, -173.962],
    HPA: [-19.777, -174.341]
  };
  const fields = ['date', 'dep', 'arr', 'std', 'sta', 'aircraft_type', 'flight_type', 'passengers', 'baggage', 'catering', 'notes'];
  let rows = [{}];

  const normaliseAirport = value => (value || '').trim().toUpperCase();
  const radians = value => value * Math.PI / 180;
  const distanceNm = (from, to) => {
    const [lat1, lon1] = airports[from] || [];
    const [lat2, lon2] = airports[to] || [];
    if ([lat1, lon1, lat2, lon2].some(value => value === undefined)) return null;
    const angle = 2 * Math.asin(Math.sqrt(Math.sin((radians(lat2) - radians(lat1)) / 2) ** 2 + Math.cos(radians(lat1)) * Math.cos(radians(lat2)) * Math.sin((radians(lon2) - radians(lon1)) / 2) ** 2));
    return angle * 3440.065;
  };
  const parseTime = value => {
    const clean = (value || '').trim().replace(':', '');
    if (!/^\d{3,4}$/.test(clean)) return null;
    const hours = Number(clean.slice(0, -2));
    const minutes = Number(clean.slice(-2));
    return hours < 24 && minutes < 60 ? hours * 60 + minutes : null;
  };
  const formatTime = minutes => {
    const total = ((minutes % 1440) + 1440) % 1440;
    return String(Math.floor(total / 60)).padStart(2, '0') + String(total % 60).padStart(2, '0');
  };
  const estimateArrival = row => {
    const departure = parseTime(row.std);
    const miles = distanceNm(normaliseAirport(row.dep), normaliseAirport(row.arr));
    if (departure === null || miles === null || !row.aircraft_type) return null;
    // Average operational cruise speeds, plus a 12-minute allowance for climb, descent and taxi.
    const knots = row.aircraft_type === 'ATR72' ? 275 : 250;
    const blockMinutes = (miles / knots) * 60 + 12;
    // Always round upward: calculated arrivals must end in 0 or 5.
    return formatTime(departure + Math.ceil(blockMinutes / 5) * 5);
  };
  const input = (key, row, index) => {
    const value = row[key] || '';
    if (key === 'aircraft_type') return `<select data-row="${index}" data-key="${key}"><option value="">Select</option><option value="SF34" ${value === 'SF34' ? 'selected' : ''}>SF34</option><option value="ATR72" ${value === 'ATR72' ? 'selected' : ''}>ATR 72</option></select>`;
    const type = key === 'date' ? 'type="date"' : '';
    const placeholder = key === 'flight_type' ? 'placeholder="Charter / Position"' : key === 'sta' ? 'placeholder="Calculated"' : '';
    return `<input ${type} data-row="${index}" data-key="${key}" value="${value}" ${placeholder}>`;
  };
  const render = () => {
    body.innerHTML = rows.map((row, index) => `<tr>${fields.map(key => `<td>${input(key, row, index)}</td>`).join('')}</tr>`).join('');
  };
  const calculateRow = index => {
    const row = rows[index];
    const arrival = estimateArrival(row);
    if (!arrival) return;
    row.sta = arrival;
    row.calculated_sta = arrival;
    const target = body.querySelector(`[data-row="${index}"][data-key="sta"]`);
    if (target) target.value = arrival;
  };

  document.getElementById('addRequestSector').onclick = () => {
    const previous = rows[rows.length - 1] || {};
    rows.push({ date: previous.date || '', dep: previous.arr || '', aircraft_type: previous.aircraft_type || '' });
    render();
  };
  body.oninput = event => {
    const element = event.target;
    if (element.dataset.row === undefined) return;
    const row = rows[Number(element.dataset.row)];
    row[element.dataset.key] = element.value;
    if (element.dataset.key === 'sta' && element.value !== row.calculated_sta) row.calculated_sta = null;
  };
  body.onchange = event => {
    const element = event.target;
    if (element.dataset.row === undefined) return;
    const index = Number(element.dataset.row);
    const row = rows[index];
    row[element.dataset.key] = element.value;
    if (['dep', 'arr', 'std', 'aircraft_type'].includes(element.dataset.key)) calculateRow(index);
  };
  form.onsubmit = () => {
    document.getElementById('requestSectors').value = JSON.stringify(rows.filter(row => row.date && row.dep && row.arr && row.std && row.sta));
  };
  render();
})();
