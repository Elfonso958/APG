(() => {
  const groupsEl = document.getElementById('requestAircraftGroups');
  const form = document.getElementById('requestForm');
  if (!groupsEl || !form) return;
  const cateringServices = Array.isArray(window.charterRequestCateringServices) ? window.charterRequestCateringServices : [];
  const airports = { AKL:[-37.008,174.792],WLG:[-41.327,174.805],CHC:[-43.489,172.532],DUD:[-45.928,170.198],IVC:[-46.412,168.313],ZQN:[-45.021,168.739],ROT:[-38.109,176.317],TRG:[-37.672,176.197],NPL:[-39.008,174.179],NPE:[-39.465,176.870],PMR:[-40.320,175.617],NSN:[-41.299,173.221],WSZ:[-41.739,171.580],TUO:[-38.740,176.084],GIS:[-38.663,177.978],KKE:[-35.262,173.912],WAG:[-39.962,175.025],TEU:[-45.533,167.650],TBU:[-21.241,-175.149],VAV:[-18.586,-173.962],HPA:[-19.777,-174.341] };
  let nextGroup = 2;
  let aircraftGroups = [{ id: 'aircraft-1', aircraft_type: '', rows: [{}] }];
  const esc = value => String(value ?? '').replace(/[&<>"']/g, char => ({ '&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#39;' }[char]));
  const airport = value => String(value || '').trim().toUpperCase();
  const radians = value => value * Math.PI / 180;
  const parseTime = value => { const clean = String(value || '').trim().replace(':',''); if (!/^\d{3,4}$/.test(clean)) return null; const h=Number(clean.slice(0,-2)), m=Number(clean.slice(-2)); return h < 24 && m < 60 ? h*60+m : null; };
  const formatTime = value => { const time=((value%1440)+1440)%1440; return `${String(Math.floor(time/60)).padStart(2,'0')}${String(time%60).padStart(2,'0')}`; };
  const estimateArrival = (row, aircraftType) => {
    const start=parseTime(row.std), from=airports[airport(row.dep)], to=airports[airport(row.arr)];
    if (start === null || !from || !to || !aircraftType) return '';
    const angle=2*Math.asin(Math.sqrt(Math.sin((radians(to[0])-radians(from[0]))/2)**2+Math.cos(radians(from[0]))*Math.cos(radians(to[0]))*Math.sin((radians(to[1])-radians(from[1]))/2)**2));
    const minutes=(angle*3440.065/(aircraftType === 'ATR72' ? 275 : 250))*60+12;
    return formatTime(start + Math.ceil(minutes / 5) * 5);
  };
  const rowInput = (group, row, rowIndex, key) => {
    const value = row[key] || '', data = `data-group="${group.id}" data-row="${rowIndex}" data-key="${key}"`;
    if (key === 'flight_type') return `<select ${data}><option value="Charter" ${value === 'Charter' || !value ? 'selected' : ''}>Charter</option><option value="Charter Positioning" ${value === 'Charter Positioning' ? 'selected' : ''}>Charter Positioning</option></select>`;
    if (key === 'catering') return `<select ${data}><option value="">No catering selected</option>${cateringServices.map(item => `<option value="${esc(item)}" ${value === item ? 'selected' : ''}>${esc(item)}</option>`).join('')}</select>`;
    return `<input ${key === 'date' ? 'type="date"' : ''} ${data} value="${esc(value)}" ${key === 'sta' ? 'placeholder="Calculated"' : ''}>`;
  };
  const render = () => {
    groupsEl.innerHTML = aircraftGroups.map((group, groupIndex) => `<section class="request-aircraft-card" data-card="${group.id}">
      <div class="request-aircraft-header"><div><span class="request-step">Aircraft ${groupIndex + 1}</span><h3>${group.aircraft_type === 'ATR72' ? 'ATR 72' : group.aircraft_type === 'SF34' ? 'Saab 340' : 'Choose aircraft type'}</h3><p>All sectors in this group will be planned against one operating tail.</p></div>${aircraftGroups.length > 1 ? `<button class="request-remove-aircraft" type="button" data-remove-group="${group.id}">Remove aircraft</button>` : ''}</div>
      <label class="request-aircraft-select">Aircraft type<select data-aircraft-group="${group.id}"><option value="">Select aircraft</option><option value="SF34" ${group.aircraft_type === 'SF34' ? 'selected' : ''}>Saab 340 (SF34)</option><option value="ATR72" ${group.aircraft_type === 'ATR72' ? 'selected' : ''}>ATR 72</option></select></label>
      <div class="brief-table-wrap request-sector-table"><table><thead><tr><th>Date</th><th>Dep</th><th>Arr</th><th>STD</th><th>STA</th><th>Flight type</th><th>Passengers</th><th>Baggage</th><th>Catering</th><th>Notes</th><th></th></tr></thead><tbody>${group.rows.map((row, index) => `<tr>${['date','dep','arr','std','sta','flight_type','passengers','baggage','catering','notes'].map(key => `<td>${rowInput(group,row,index,key)}</td>`).join('')}<td><button class="remove-row" type="button" data-remove-row="${group.id}:${index}" aria-label="Remove sector">×</button></td></tr>`).join('')}</tbody></table></div>
      <button class="btn secondary request-add-sector" type="button" data-add-sector="${group.id}">+ Add sector</button>
    </section>`).join('');
  };
  const getGroup = id => aircraftGroups.find(group => group.id === id);
  const calculate = (group, row) => { const arrival=estimateArrival(row,group.aircraft_type); if (arrival) { row.sta=arrival; row.calculated_sta=arrival; } };
  groupsEl.addEventListener('input', event => {
    const field=event.target, group=getGroup(field.dataset.group); if (!group) return;
    const row=group.rows[Number(field.dataset.row)]; row[field.dataset.key]=field.value;
    if (field.dataset.key === 'sta' && field.value !== row.calculated_sta) row.calculated_sta=null;
  });
  groupsEl.addEventListener('change', event => {
    const field=event.target;
    if (field.dataset.aircraftGroup) { const group=getGroup(field.dataset.aircraftGroup); group.aircraft_type=field.value; group.rows.forEach(row => calculate(group,row)); render(); return; }
    const group=getGroup(field.dataset.group); if (!group) return;
    const row=group.rows[Number(field.dataset.row)]; row[field.dataset.key]=field.value;
    if (['dep','arr','std'].includes(field.dataset.key)) { calculate(group,row); render(); }
  });
  groupsEl.addEventListener('click', event => {
    const target=event.target.closest('button'); if (!target) return;
    if (target.dataset.addSector) { const group=getGroup(target.dataset.addSector), prior=group.rows.at(-1) || {}; group.rows.push({ date:prior.date || '', dep:prior.arr || '' }); render(); }
    if (target.dataset.removeGroup) { aircraftGroups=aircraftGroups.filter(group => group.id !== target.dataset.removeGroup); render(); }
    if (target.dataset.removeRow) { const [groupId,index]=target.dataset.removeRow.split(':'); const group=getGroup(groupId); group.rows.splice(Number(index),1); if (!group.rows.length) group.rows.push({}); render(); }
  });
  document.getElementById('addRequestAircraft').addEventListener('click', () => { aircraftGroups.push({ id:`aircraft-${nextGroup++}`, aircraft_type:'', rows:[{}] }); render(); });
  form.addEventListener('submit', () => {
    const sectors=aircraftGroups.flatMap(group => group.rows.filter(row => row.date && row.dep && row.arr && row.std && row.sta).map(row => ({ ...row, dep:airport(row.dep), arr:airport(row.arr), aircraft_type:group.aircraft_type, aircraft_group:group.id })));
    document.getElementById('requestSectors').value=JSON.stringify(sectors);
  });
  render();
})();
