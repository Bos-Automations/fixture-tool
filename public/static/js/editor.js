// --- Auto-save ---
const saveStatus = document.getElementById('saveStatus');

function showSaveStatus(text, type) {
    saveStatus.textContent = text;
    saveStatus.className = 'save-status save-status-' + type;
}

function debounce(fn, ms) {
    let timer;
    return function(...args) {
        clearTimeout(timer);
        timer = setTimeout(() => fn.apply(this, args), ms);
    };
}

async function saveField(url, data) {
    showSaveStatus('Saving...', 'saving');
    try {
        const res = await fetch(url, {
            method: 'PATCH',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data)
        });
        if (!res.ok) throw new Error('Save failed');
        showSaveStatus('Saved', 'saved');
    } catch (e) {
        showSaveStatus('Error saving', 'error');
        console.error(e);
    }
}

const debouncedSave = debounce(saveField, 400);

// --- Status-based field locking ---
const LOCKED_STATUSES = ['Approved', 'Ordered', 'Shipped', 'Received', 'Delivered'];

function getComponentPrefix(statusField) {
    const field = statusField.dataset.field;
    if (field === 'status') return 'fixture';
    return field.replace('_status', '');
}

function applyFieldLocking(row) {
    // Find all status selects in this row
    const statusSelects = row.querySelectorAll('.status-select');
    statusSelects.forEach(sel => {
        const prefix = getComponentPrefix(sel);
        const isLocked = LOCKED_STATUSES.includes(sel.value);
        // Lock/unlock all lockable fields for this component
        const lockableFields = row.querySelectorAll('[data-lockable="' + prefix + '"]');
        lockableFields.forEach(input => {
            input.readOnly = isLocked;
            input.disabled = isLocked;
            if (isLocked) {
                input.classList.add('field-locked');
            } else {
                input.classList.remove('field-locked');
            }
        });
    });
}

function updateApprovedDateDisplay(row) {
    const dateDisplays = row.querySelectorAll('.approved-date-display');
    dateDisplays.forEach(span => {
        const dateField = span.dataset.dateFor;
        const input = row.querySelector('[data-field="' + dateField + '"]');
        // The date is stored in a hidden field or in the fixture data
        const dateVal = input ? input.value : (span.dataset.dateValue || '');
        if (dateVal) {
            span.textContent = '(Approved: ' + dateVal + ')';
            span.style.display = '';
        } else {
            span.textContent = '';
        }
    });
}

// --- Event delegation for auto-save ---
document.addEventListener('change', function(e) {
    const el = e.target;

    // Project-level fields
    if (el.classList.contains('project-field')) {
        const field = el.dataset.projectField;
        let value = el.value;
        if (el.type === 'number') value = parseFloat(value) || 0;
        saveField('/api/projects/' + PROJECT_ID, { [field]: value });
        return;
    }

    // Fixture fields
    const row = el.closest('.fixture-row');
    if (!row) return;
    const table = row.dataset.table;
    const id = row.dataset.id;
    const field = el.dataset.field;
    if (!field) return;

    let value = el.value;
    if (el.type === 'number') value = parseFloat(value) || 0;

    saveField('/api/fixtures/' + table + '/' + id, { [field]: value });

    // Update summary display
    updateSummary(row);
    // Update status dots
    updateStatusDots(row);

    // If a status field changed, apply locking and update approved date
    if (el.classList.contains('status-select')) {
        applyFieldLocking(row);
        // After save completes, reload to get the approved_date from server
        setTimeout(() => refreshApprovedDates(row, table, id), 500);
    }
});

async function refreshApprovedDates(row, table, id) {
    try {
        const res = await fetch('/api/fixtures/' + table + '/' + id);
        if (!res.ok) return;
        // We don't have a GET endpoint, so we'll just update dates from the response
        // Actually, let's store the date client-side based on status change
    } catch(e) {}
    // The server auto-sets dates; we update display after a brief delay
    // For now, show the date inline from JS
    const dateDisplays = row.querySelectorAll('.approved-date-display');
    dateDisplays.forEach(span => {
        const dateField = span.dataset.dateFor;
        // Find the corresponding status field
        const statusFieldName = dateField.replace('_approved_date', '_status').replace('approved_date', 'status');
        const statusSel = row.querySelector('[data-field="' + statusFieldName + '"]');
        if (statusSel && LOCKED_STATUSES.includes(statusSel.value)) {
            if (!span.textContent) {
                const now = new Date();
                const dateStr = now.toLocaleDateString() + ' ' + now.toLocaleTimeString([], {hour: '2-digit', minute:'2-digit'});
                span.textContent = '(Approved: ' + dateStr + ')';
            }
        } else if (statusSel && !LOCKED_STATUSES.includes(statusSel.value)) {
            span.textContent = '';
        }
    });
}

// --- Color picker click handling ---
document.addEventListener('click', function(e) {
    const swatch = e.target.closest('.color-swatch');
    if (!swatch) return;

    const picker = swatch.closest('.color-picker');
    if (!picker) return;

    const row = swatch.closest('.fixture-row');
    if (!row) return;

    const field = picker.dataset.field;
    const color = swatch.dataset.color;
    const table = row.dataset.table;
    const id = row.dataset.id;

    // Update active state
    picker.querySelectorAll('.color-swatch').forEach(s => s.classList.remove('active'));
    swatch.classList.add('active');

    // Save
    saveField('/api/fixtures/' + table + '/' + id, { [field]: color });
});

// Also handle input event for text fields (debounced)
document.addEventListener('input', function(e) {
    const el = e.target;
    if (el.tagName !== 'INPUT' || el.type === 'number') return;

    if (el.classList.contains('project-field')) {
        const field = el.dataset.projectField;
        debouncedSave('/api/projects/' + PROJECT_ID, { [field]: el.value });
        return;
    }

    const row = el.closest('.fixture-row');
    if (!row) return;
    const table = row.dataset.table;
    const id = row.dataset.id;
    const field = el.dataset.field;
    if (!field) return;

    debouncedSave('/api/fixtures/' + table + '/' + id, { [field]: el.value });
    updateSummary(row);
});

// --- Tab switching ---
document.querySelectorAll('.tab').forEach(tab => {
    tab.addEventListener('click', function() {
        document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
        document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
        this.classList.add('active');
        document.getElementById('tab-' + this.dataset.tab).classList.add('active');
    });
});

// --- Expand / Collapse ---
function toggleExpand(summaryEl) {
    const row = summaryEl.closest('.fixture-row');
    row.classList.toggle('expanded');
}

// --- Update summary display from detail fields ---
function updateSummary(row) {
    const getId = row.querySelector('[data-field="fixture_id"]');
    const getDesc = row.querySelector('[data-field="description"]');
    const getQty = row.querySelector('[data-field="quantity"]');

    const idDisplay = row.querySelector('.fixture-id-display');
    const descDisplay = row.querySelector('.fixture-desc-display');
    const qtyDisplay = row.querySelector('.fixture-qty-display');

    if (getId && idDisplay) idDisplay.textContent = getId.value || '—';
    if (getDesc && descDisplay) descDisplay.textContent = getDesc.value || 'No description';
    if (getQty && qtyDisplay) qtyDisplay.textContent = 'Qty: ' + (getQty.value || 1);
}

// --- Status dot colors ---
function getStatusColor(status) {
    switch (status) {
        case 'Delivered': return '#22c55e';
        case 'Received': return '#86efac';
        case 'Shipped': return '#3b82f6';
        case 'Ordered': return '#f59e0b';
        case 'Approved': return '#a855f7';
        case 'Quoting': return '#ef4444';
        default: return '#d1d5db';
    }
}

function updateStatusDots(row) {
    const dots = row.querySelectorAll('.status-dot');
    dots.forEach(dot => {
        const comp = dot.dataset.component;
        let statusField;
        if (comp === 'fixture') {
            statusField = row.querySelector('[data-field="status"]') || row.querySelector('[data-field="fixture_status"]');
        } else {
            statusField = row.querySelector('[data-field="' + comp + '_status"]');
        }
        if (statusField) {
            dot.style.backgroundColor = getStatusColor(statusField.value);
            dot.title = (comp.charAt(0).toUpperCase() + comp.slice(1)) + ': ' + (statusField.value || 'None');
        }
    });
}

// --- Add fixture ---
async function addFixture(table, listType) {
    const res = await fetch('/api/projects/' + PROJECT_ID + '/fixtures/' + table, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
    });
    const fixture = await res.json();
    const newRow = buildFixtureRow(listType, fixture);
    const list = document.getElementById('list-' + listType);
    list.appendChild(newRow);
    newRow.classList.add('expanded');
    const idField = newRow.querySelector('[data-field="fixture_id"]');
    if (idField) idField.focus();
}

// --- Duplicate fixture ---
async function duplicateFixture(btn) {
    const row = btn.closest('.fixture-row');
    const table = row.dataset.table;
    const id = row.dataset.id;

    const res = await fetch('/api/fixtures/' + table + '/' + id + '/duplicate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
    });
    const fixture = await res.json();

    // Determine list type from table name
    const tableToListType = {
        'recessed_fixtures': 'recessed',
        'recessed_accessories': 'recessed-accessory',
        'linear_fixtures': 'linear',
        'linear_accessories': 'linear-accessory',
        'decorative_fixtures': 'decorative',
        'decorative_accessories': 'decorative-accessory',
        'landscape_fixtures': 'landscape',
        'landscape_transformers': 'transformer',
        'landscape_accessories': 'accessory'
    };
    const listType = tableToListType[table] || table.replace('_fixtures', '');
    const newRow = buildFixtureRow(listType, fixture);
    // Insert after current row
    row.parentNode.insertBefore(newRow, row.nextSibling);
    newRow.classList.add('expanded');
    showSaveStatus('Saved', 'saved');
}

// --- Delete fixture ---
async function deleteFixture(btn) {
    const row = btn.closest('.fixture-row');
    const table = row.dataset.table;
    const id = row.dataset.id;
    const fixtureId = row.querySelector('[data-field="fixture_id"]')?.value || '';

    if (!confirm('Delete fixture' + (fixtureId ? ' "' + fixtureId + '"' : '') + '?')) return;

    await fetch('/api/fixtures/' + table + '/' + id, { method: 'DELETE' });
    row.remove();
    showSaveStatus('Saved', 'saved');
}

// --- Build a fixture row from template and data ---
function buildFixtureRow(type, fixture) {
    const tpl = document.getElementById('tpl-' + type);
    const html = tpl.innerHTML
        .replace(/__ID__/g, fixture.id)
        .replace(/__FIXTURE_ID__/g, fixture.fixture_id || '—')
        .replace(/__DESCRIPTION__/g, fixture.description || 'No description')
        .replace(/__QTY__/g, fixture.quantity || 1);

    const container = document.createElement('div');
    container.innerHTML = html;
    const row = container.firstElementChild;

    // Populate all fields
    Object.keys(fixture).forEach(key => {
        const input = row.querySelector('[data-field="' + key + '"]');
        if (input) {
            if (input.tagName === 'SELECT') {
                input.value = fixture[key] || '';
            } else {
                input.value = fixture[key] !== null ? fixture[key] : '';
            }
        }
        // Set approved date display values
        if (key.endsWith('_approved_date') || key === 'approved_date') {
            const dateDisplay = row.querySelector('[data-date-for="' + key + '"]');
            if (dateDisplay && fixture[key]) {
                dateDisplay.textContent = '(Approved: ' + fixture[key] + ')';
                dateDisplay.dataset.dateValue = fixture[key];
            }
        }
    });

    // Set active color swatch
    if (fixture.color) {
        const picker = row.querySelector('.color-picker[data-field="color"]');
        if (picker) {
            const swatch = picker.querySelector('[data-color="' + fixture.color + '"]');
            if (swatch) swatch.classList.add('active');
        }
    }

    updateStatusDots(row);
    applyFieldLocking(row);

    return row;
}

// --- Initialize existing fixtures on page load ---
function initExistingFixtures() {
    const types = {
        recessed: { data: PROJECT_DATA.recessed },
        'recessed-accessory': { data: PROJECT_DATA.recessed_accessories },
        linear: { data: PROJECT_DATA.linear },
        'linear-accessory': { data: PROJECT_DATA.linear_accessories },
        decorative: { data: PROJECT_DATA.decorative },
        'decorative-accessory': { data: PROJECT_DATA.decorative_accessories },
        landscape: { data: PROJECT_DATA.landscape },
        transformer: { data: PROJECT_DATA.transformers },
        accessory: { data: PROJECT_DATA.accessories }
    };

    Object.keys(types).forEach(type => {
        const list = document.getElementById('list-' + type);
        const fixtures = types[type].data;
        if (!fixtures || fixtures.length === 0) return;

        fixtures.forEach(fixture => {
            const row = buildFixtureRow(type, fixture);
            list.appendChild(row);
        });
    });
}

initExistingFixtures();

// --- Drag and drop reordering ---
let dragRow = null;

function initDragAndDrop(row) {
    const handle = row.querySelector('.drag-handle');
    if (!handle) return;

    // Prevent the handle click from toggling expand
    handle.addEventListener('click', function(e) {
        e.stopPropagation();
    });

    // Make the row draggable when grabbed by the handle
    handle.addEventListener('mousedown', function() {
        row.setAttribute('draggable', 'true');
    });

    document.addEventListener('mouseup', function() {
        row.removeAttribute('draggable');
    });

    row.addEventListener('dragstart', function(e) {
        // Only allow drag if initiated from handle
        if (!row.getAttribute('draggable')) {
            e.preventDefault();
            return;
        }
        dragRow = row;
        row.classList.add('dragging');
        e.dataTransfer.effectAllowed = 'move';
        e.dataTransfer.setData('text/plain', '');
    });

    row.addEventListener('dragend', function() {
        row.classList.remove('dragging');
        row.removeAttribute('draggable');
        clearDragOvers();
        dragRow = null;
    });

    row.addEventListener('dragover', function(e) {
        if (!dragRow || dragRow === row) return;
        // Only allow reorder within the same list
        if (dragRow.parentNode !== row.parentNode) return;
        e.preventDefault();
        e.dataTransfer.dropEffect = 'move';
        clearDragOvers();
        row.classList.add('drag-over');
    });

    row.addEventListener('dragleave', function() {
        row.classList.remove('drag-over');
    });

    row.addEventListener('drop', function(e) {
        e.preventDefault();
        if (!dragRow || dragRow === row) return;
        if (dragRow.parentNode !== row.parentNode) return;

        const list = row.parentNode;
        const rows = Array.from(list.children);
        const dragIndex = rows.indexOf(dragRow);
        const dropIndex = rows.indexOf(row);

        if (dragIndex < dropIndex) {
            list.insertBefore(dragRow, row.nextSibling);
        } else {
            list.insertBefore(dragRow, row);
        }

        clearDragOvers();
        saveNewOrder(list);
    });

    // --- Touch support ---
    let touchClone = null;
    let touchStartY = 0;
    let touchCurrentTarget = null;

    handle.addEventListener('touchstart', function(e) {
        e.preventDefault();
        e.stopPropagation();
        dragRow = row;
        touchStartY = e.touches[0].clientY;

        // Create a visual clone
        touchClone = row.cloneNode(true);
        touchClone.style.cssText = 'position:fixed;z-index:1000;pointer-events:none;opacity:0.8;width:' +
            row.offsetWidth + 'px;left:' + row.getBoundingClientRect().left + 'px;top:' +
            row.getBoundingClientRect().top + 'px;box-shadow:0 4px 12px rgba(0,0,0,0.2);';
        document.body.appendChild(touchClone);

        row.classList.add('dragging');
    }, { passive: false });

    handle.addEventListener('touchmove', function(e) {
        if (!dragRow || !touchClone) return;
        e.preventDefault();

        const touch = e.touches[0];
        const deltaY = touch.clientY - touchStartY;
        touchClone.style.top = (row.getBoundingClientRect().top + deltaY) + 'px';

        // Find the element under the touch point
        touchClone.style.display = 'none';
        const elemBelow = document.elementFromPoint(touch.clientX, touch.clientY);
        touchClone.style.display = '';

        clearDragOvers();
        if (elemBelow) {
            const targetRow = elemBelow.closest('.fixture-row');
            if (targetRow && targetRow !== dragRow && targetRow.parentNode === dragRow.parentNode) {
                targetRow.classList.add('drag-over');
                touchCurrentTarget = targetRow;
            } else {
                touchCurrentTarget = null;
            }
        }
    }, { passive: false });

    handle.addEventListener('touchend', function(e) {
        if (!dragRow) return;
        e.preventDefault();

        if (touchCurrentTarget && touchCurrentTarget !== dragRow) {
            const list = touchCurrentTarget.parentNode;
            const rows = Array.from(list.children);
            const dragIndex = rows.indexOf(dragRow);
            const dropIndex = rows.indexOf(touchCurrentTarget);

            if (dragIndex < dropIndex) {
                list.insertBefore(dragRow, touchCurrentTarget.nextSibling);
            } else {
                list.insertBefore(dragRow, touchCurrentTarget);
            }
            saveNewOrder(list);
        }

        row.classList.remove('dragging');
        clearDragOvers();
        if (touchClone) {
            touchClone.remove();
            touchClone = null;
        }
        dragRow = null;
        touchCurrentTarget = null;
    }, { passive: false });
}

function clearDragOvers() {
    document.querySelectorAll('.drag-over').forEach(el => el.classList.remove('drag-over'));
}

async function saveNewOrder(list) {
    const rows = Array.from(list.querySelectorAll('.fixture-row'));
    if (rows.length === 0) return;

    const table = rows[0].dataset.table;
    const order = rows.map(r => parseInt(r.dataset.id));

    showSaveStatus('Saving...', 'saving');
    try {
        const res = await fetch('/api/fixtures/' + table + '/reorder', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ order: order })
        });
        if (!res.ok) throw new Error('Reorder failed');
        showSaveStatus('Saved', 'saved');
    } catch (e) {
        showSaveStatus('Error saving', 'error');
        console.error(e);
    }
}

// Initialize drag on all existing rows
document.querySelectorAll('.fixture-row').forEach(initDragAndDrop);

// Patch buildFixtureRow to init drag on new rows
const _origBuildFixtureRow = buildFixtureRow;
buildFixtureRow = function(type, fixture) {
    const row = _origBuildFixtureRow(type, fixture);
    initDragAndDrop(row);
    return row;
};
