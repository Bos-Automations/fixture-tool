import os
import json
import urllib.request
import urllib.error
from dotenv import load_dotenv

load_dotenv()

CF_ACCOUNT_ID = os.environ.get('CF_ACCOUNT_ID', '')
CF_API_TOKEN = os.environ.get('CF_API_TOKEN', '')
CF_D1_DATABASE_ID = os.environ.get('CF_D1_DATABASE_ID', '')

VALID_TABLES = {'recessed_fixtures', 'linear_fixtures', 'decorative_fixtures',
                 'landscape_fixtures', 'landscape_transformers', 'landscape_accessories',
                 'recessed_accessories', 'linear_accessories', 'decorative_accessories'}

TABLE_COLUMNS = {
    'recessed_fixtures': {
        'fixture_id', 'description', 'quantity',
        'housing_part_no', 'housing_dealer_price', 'housing_list_price', 'housing_status',
        'housing_approved_date',
        'module_part_no', 'module_dealer_price', 'module_list_price', 'module_status',
        'module_approved_date',
        'trim_part_no', 'trim_dealer_price', 'trim_list_price', 'trim_status',
        'trim_approved_date',
        'driver_part_no', 'driver_dealer_price', 'driver_list_price', 'driver_status',
        'driver_approved_date',
        'cct', 'beam_spread', 'fixture_type', 'control_type', 'color',
        'trim_style', 'trim_color', 'sort_order',
    },
    'linear_fixtures': {
        'fixture_id', 'description', 'quantity',
        'tape_part_no', 'tape_dealer_price', 'tape_list_price', 'tape_status',
        'tape_approved_date',
        'driver_part_no', 'driver_dealer_price', 'driver_list_price', 'driver_status',
        'driver_approved_date',
        'channel_part_no', 'channel_dealer_price', 'channel_list_price', 'channel_status',
        'channel_approved_date',
        'cct', 'channel_type', 'control_type', 'color', 'sort_order',
    },
    'decorative_fixtures': {
        'fixture_id', 'description', 'quantity',
        'fixture_type', 'lamp_type', 'lamp_quantity', 'cct',
        'part_no', 'dealer_price', 'list_price', 'status', 'approved_date', 'sort_order',
    },
    'landscape_fixtures': {
        'fixture_id', 'description', 'quantity',
        'cct', 'beam_spread', 'fixture_type', 'control_type', 'color',
        'trim_style', 'trim_color', 'finish', 'wattage',
        'mount_part_no', 'mount_dealer_price', 'mount_list_price', 'mount_status',
        'mount_approved_date',
        'fixture_part_no', 'fixture_dealer_price', 'fixture_list_price', 'fixture_status',
        'fixture_approved_date',
        'lamp_part_no', 'lamp_dealer_price', 'lamp_list_price', 'lamp_status',
        'lamp_approved_date',
        'accessory_part_no', 'accessory_dealer_price', 'accessory_list_price', 'accessory_status',
        'accessory_approved_date',
        'sort_order',
    },
    'landscape_transformers': {
        'fixture_id', 'description', 'quantity', 'wattage', 'control_type',
        'part_no', 'dealer_price', 'list_price', 'status', 'approved_date',
        'sort_order',
    },
    'landscape_accessories': {
        'fixture_id', 'description', 'quantity',
        'part_no', 'dealer_price', 'list_price', 'status', 'approved_date',
        'sort_order',
    },
    'recessed_accessories': {
        'fixture_id', 'description', 'quantity',
        'part_no', 'dealer_price', 'list_price', 'status', 'approved_date',
        'sort_order',
    },
    'linear_accessories': {
        'fixture_id', 'description', 'quantity',
        'part_no', 'dealer_price', 'list_price', 'status', 'approved_date',
        'sort_order',
    },
    'decorative_accessories': {
        'fixture_id', 'description', 'quantity',
        'part_no', 'dealer_price', 'list_price', 'status', 'approved_date',
        'sort_order',
    },
}

PROJECT_COLUMNS = {'name', 'address', 'parts_extras_pct', 'tax_pct', 'shipping_pct'}


# --- Cloudflare D1 REST API client ---

def _d1_query(sql, params=None):
    url = f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}/d1/database/{CF_D1_DATABASE_ID}/query"
    payload = {"sql": sql}
    if params:
        payload["params"] = params
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        url,
        data=body,
        headers={
            "Authorization": f"Bearer {CF_API_TOKEN}",
            "Content-Type": "application/json",
        }
    )
    with urllib.request.urlopen(req) as resp:
        data = json.loads(resp.read())
    if not data.get("success"):
        errors = data.get("errors", [])
        msg = errors[0].get("message", "Unknown D1 error") if errors else "Unknown D1 error"
        raise Exception(msg)
    result = data["result"][0]
    rows = result.get("results", [])
    meta = result.get("meta", {})
    return {
        "rows": rows,
        "last_insert_rowid": meta.get("last_row_id"),
        "affected_row_count": meta.get("changes", 0),
    }


def _execute(sql, args=None):
    return _d1_query(sql, args)


def _execute_batch(statements):
    results = []
    for stmt in statements:
        if isinstance(stmt, str):
            results.append(_d1_query(stmt))
        else:
            sql, args = stmt
            results.append(_d1_query(sql, args))
    return results


def init_db():
    pass


# --- Project CRUD ---

def get_all_projects():
    result = _execute('SELECT * FROM projects ORDER BY updated_date DESC')
    return result["rows"]


def create_project(name, address=''):
    result = _execute('INSERT INTO projects (name, address) VALUES (?, ?)', [name, address])
    return result["last_insert_rowid"]


def get_project(project_id):
    result = _execute('SELECT * FROM projects WHERE id = ?', [project_id])
    if not result["rows"]:
        return None
    project = result["rows"][0]
    project['recessed'] = _execute(
        'SELECT * FROM recessed_fixtures WHERE project_id = ? ORDER BY sort_order, id', [project_id]
    )["rows"]
    project['linear'] = _execute(
        'SELECT * FROM linear_fixtures WHERE project_id = ? ORDER BY sort_order, id', [project_id]
    )["rows"]
    project['decorative'] = _execute(
        'SELECT * FROM decorative_fixtures WHERE project_id = ? ORDER BY sort_order, id', [project_id]
    )["rows"]
    project['landscape'] = _execute(
        'SELECT * FROM landscape_fixtures WHERE project_id = ? ORDER BY sort_order, id', [project_id]
    )["rows"]
    project['transformers'] = _execute(
        'SELECT * FROM landscape_transformers WHERE project_id = ? ORDER BY sort_order, id', [project_id]
    )["rows"]
    project['accessories'] = _execute(
        'SELECT * FROM landscape_accessories WHERE project_id = ? ORDER BY sort_order, id', [project_id]
    )["rows"]
    project['recessed_accessories'] = _execute(
        'SELECT * FROM recessed_accessories WHERE project_id = ? ORDER BY sort_order, id', [project_id]
    )["rows"]
    project['linear_accessories'] = _execute(
        'SELECT * FROM linear_accessories WHERE project_id = ? ORDER BY sort_order, id', [project_id]
    )["rows"]
    project['decorative_accessories'] = _execute(
        'SELECT * FROM decorative_accessories WHERE project_id = ? ORDER BY sort_order, id', [project_id]
    )["rows"]
    return project


def update_project(project_id, data):
    safe_data = {k: v for k, v in data.items() if k in PROJECT_COLUMNS}
    if not safe_data:
        return
    sets = ', '.join(f'{k} = ?' for k in safe_data)
    values = list(safe_data.values()) + [project_id]
    _execute(
        f"UPDATE projects SET {sets}, updated_date = datetime('now','localtime') WHERE id = ?",
        values
    )


def delete_project(project_id):
    for table in VALID_TABLES:
        _execute(f'DELETE FROM {table} WHERE project_id = ?', [project_id])
    _execute('DELETE FROM projects WHERE id = ?', [project_id])


# --- Fixture CRUD ---

def create_fixture(table, project_id):
    if table not in VALID_TABLES:
        raise ValueError(f'Invalid table: {table}')
    result = _execute(
        f'SELECT COALESCE(MAX(sort_order), -1) + 1 as next_order FROM {table} WHERE project_id = ?',
        [project_id]
    )
    next_order = result["rows"][0]["next_order"]
    insert_result = _execute(
        f'INSERT INTO {table} (project_id, sort_order) VALUES (?, ?)',
        [project_id, next_order]
    )
    fixture_id = insert_result["last_insert_rowid"]
    fixture = _execute(f'SELECT * FROM {table} WHERE id = ?', [fixture_id])
    return fixture["rows"][0]


def duplicate_fixture(table, fixture_id):
    if table not in VALID_TABLES:
        raise ValueError(f'Invalid table: {table}')
    result = _execute(f'SELECT * FROM {table} WHERE id = ?', [fixture_id])
    if not result["rows"]:
        return None
    orig = result["rows"][0]
    project_id = orig['project_id']
    order_result = _execute(
        f'SELECT COALESCE(MAX(sort_order), -1) + 1 as next_order FROM {table} WHERE project_id = ?',
        [project_id]
    )
    next_order = order_result["rows"][0]["next_order"]
    valid_cols = TABLE_COLUMNS[table]
    cols = [c for c in orig.keys() if c != 'id' and c != 'project_id' and c in valid_cols]
    col_names = ', '.join(['project_id', 'sort_order'] + cols)
    placeholders = ', '.join(['?'] * (2 + len(cols)))
    values = [project_id, next_order] + [orig[c] for c in cols]
    insert_result = _execute(f'INSERT INTO {table} ({col_names}) VALUES ({placeholders})', values)
    new_id = insert_result["last_insert_rowid"]
    new_fixture = _execute(f'SELECT * FROM {table} WHERE id = ?', [new_id])
    return new_fixture["rows"][0]


def update_fixture(table, fixture_id, data):
    if table not in VALID_TABLES:
        raise ValueError(f'Invalid table: {table}')
    valid_cols = TABLE_COLUMNS[table]
    safe_data = {k: v for k, v in data.items() if k in valid_cols}
    if not safe_data:
        return
    sets = ', '.join(f'{k} = ?' for k in safe_data)
    values = list(safe_data.values()) + [fixture_id]
    _execute(f'UPDATE {table} SET {sets} WHERE id = ?', values)
    _execute(
        "UPDATE projects SET updated_date = datetime('now','localtime') "
        "WHERE id = (SELECT project_id FROM " + table + " WHERE id = ?)",
        [fixture_id]
    )


def reorder_fixtures(table, order):
    if table not in VALID_TABLES:
        raise ValueError(f'Invalid table: {table}')
    for i, fixture_id in enumerate(order):
        _execute(f'UPDATE {table} SET sort_order = ? WHERE id = ?', [i, fixture_id])


def delete_fixture(table, fixture_id):
    if table not in VALID_TABLES:
        raise ValueError(f'Invalid table: {table}')
    _execute(f'DELETE FROM {table} WHERE id = ?', [fixture_id])
