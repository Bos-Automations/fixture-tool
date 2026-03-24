import os
import json
import urllib.request
import urllib.error
from dotenv import load_dotenv

load_dotenv()

TURSO_URL = os.environ.get('TURSO_DATABASE_URL', '')
TURSO_AUTH_TOKEN = os.environ.get('TURSO_AUTH_TOKEN', '')

VALID_TABLES = {'recessed_fixtures', 'linear_fixtures', 'decorative_fixtures',
                 'landscape_fixtures', 'landscape_transformers', 'landscape_accessories'}

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
}

PROJECT_COLUMNS = {'name', 'address', 'parts_extras_pct', 'tax_pct', 'shipping_pct'}


# --- Turso HTTP API client ---

def _encode_arg(value):
    if value is None:
        return {"type": "null"}
    elif isinstance(value, int):
        return {"type": "integer", "value": str(value)}
    elif isinstance(value, float):
        return {"type": "float", "value": value}
    elif isinstance(value, str):
        return {"type": "text", "value": value}
    else:
        return {"type": "text", "value": str(value)}


def _decode_value(val):
    if val["type"] == "null":
        return None
    elif val["type"] == "integer":
        return int(val["value"])
    elif val["type"] == "float":
        return float(val["value"])
    elif val["type"] == "text":
        return val["value"]
    return val.get("value")


def _execute(sql, args=None):
    stmt = {"sql": sql}
    if args:
        stmt["args"] = [_encode_arg(a) for a in args]
    body = json.dumps({
        "requests": [
            {"type": "execute", "stmt": stmt},
            {"type": "close"}
        ]
    }).encode()
    req = urllib.request.Request(
        f"{TURSO_URL}/v3/pipeline",
        data=body,
        headers={
            "Authorization": f"Bearer {TURSO_AUTH_TOKEN}",
            "Content-Type": "application/json",
        }
    )
    with urllib.request.urlopen(req) as resp:
        data = json.loads(resp.read())
    result = data["results"][0]
    if result["type"] == "error":
        raise Exception(result["error"]["message"])
    r = result["response"]["result"]
    cols = [c["name"] for c in r["cols"]]
    rows = []
    for row in r["rows"]:
        rows.append({cols[i]: _decode_value(v) for i, v in enumerate(row)})
    return {
        "columns": cols,
        "rows": rows,
        "last_insert_rowid": int(r["last_insert_rowid"]) if r.get("last_insert_rowid") else None,
        "affected_row_count": r.get("affected_row_count", 0),
    }


def _execute_batch(statements):
    requests = []
    for stmt in statements:
        if isinstance(stmt, str):
            requests.append({"type": "execute", "stmt": {"sql": stmt}})
        else:
            sql, args = stmt
            requests.append({"type": "execute", "stmt": {"sql": sql, "args": [_encode_arg(a) for a in args]}})
    requests.append({"type": "close"})
    body = json.dumps({"requests": requests}).encode()
    req = urllib.request.Request(
        f"{TURSO_URL}/v3/pipeline",
        data=body,
        headers={
            "Authorization": f"Bearer {TURSO_AUTH_TOKEN}",
            "Content-Type": "application/json",
        }
    )
    with urllib.request.urlopen(req) as resp:
        data = json.loads(resp.read())
    results = []
    for r in data["results"][:-1]:  # skip the close response
        if r["type"] == "error":
            results.append({"error": r["error"]["message"]})
        else:
            res = r["response"]["result"]
            cols = [c["name"] for c in res["cols"]]
            rows = [{cols[i]: _decode_value(v) for i, v in enumerate(row)} for row in res["rows"]]
            results.append({"columns": cols, "rows": rows,
                            "last_insert_rowid": int(res["last_insert_rowid"]) if res.get("last_insert_rowid") else None})
    return results


# --- Schema ---

_CREATE_TABLES = [
    """CREATE TABLE IF NOT EXISTS projects (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL DEFAULT '',
        address TEXT DEFAULT '',
        parts_extras_pct REAL DEFAULT 5.0,
        tax_pct REAL DEFAULT 10.5,
        shipping_pct REAL DEFAULT 4.0,
        created_date TEXT DEFAULT (datetime('now','localtime')),
        updated_date TEXT DEFAULT (datetime('now','localtime'))
    )""",
    """CREATE TABLE IF NOT EXISTS recessed_fixtures (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_id INTEGER NOT NULL,
        fixture_id TEXT DEFAULT '',
        description TEXT DEFAULT '',
        housing_part_no TEXT DEFAULT '',
        housing_dealer_price REAL DEFAULT 0,
        housing_list_price REAL DEFAULT 0,
        housing_status TEXT DEFAULT '',
        housing_approved_date TEXT DEFAULT '',
        module_part_no TEXT DEFAULT '',
        module_dealer_price REAL DEFAULT 0,
        module_list_price REAL DEFAULT 0,
        module_status TEXT DEFAULT '',
        module_approved_date TEXT DEFAULT '',
        trim_part_no TEXT DEFAULT '',
        trim_dealer_price REAL DEFAULT 0,
        trim_list_price REAL DEFAULT 0,
        trim_status TEXT DEFAULT '',
        trim_approved_date TEXT DEFAULT '',
        driver_part_no TEXT DEFAULT '',
        driver_dealer_price REAL DEFAULT 0,
        driver_list_price REAL DEFAULT 0,
        driver_status TEXT DEFAULT '',
        driver_approved_date TEXT DEFAULT '',
        cct TEXT DEFAULT '',
        beam_spread TEXT DEFAULT '',
        fixture_type TEXT DEFAULT 'Fixed',
        control_type TEXT DEFAULT '',
        color TEXT DEFAULT '',
        trim_style TEXT DEFAULT '',
        trim_color TEXT DEFAULT '',
        quantity INTEGER DEFAULT 1,
        sort_order INTEGER DEFAULT 0
    )""",
    """CREATE TABLE IF NOT EXISTS linear_fixtures (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_id INTEGER NOT NULL,
        fixture_id TEXT DEFAULT '',
        description TEXT DEFAULT '',
        tape_part_no TEXT DEFAULT '',
        tape_dealer_price REAL DEFAULT 0,
        tape_list_price REAL DEFAULT 0,
        tape_status TEXT DEFAULT '',
        tape_approved_date TEXT DEFAULT '',
        driver_part_no TEXT DEFAULT '',
        driver_dealer_price REAL DEFAULT 0,
        driver_list_price REAL DEFAULT 0,
        driver_status TEXT DEFAULT '',
        driver_approved_date TEXT DEFAULT '',
        channel_part_no TEXT DEFAULT '',
        channel_dealer_price REAL DEFAULT 0,
        channel_list_price REAL DEFAULT 0,
        channel_status TEXT DEFAULT '',
        channel_approved_date TEXT DEFAULT '',
        cct TEXT DEFAULT '',
        channel_type TEXT DEFAULT '',
        control_type TEXT DEFAULT '',
        color TEXT DEFAULT '',
        quantity INTEGER DEFAULT 1,
        sort_order INTEGER DEFAULT 0
    )""",
    """CREATE TABLE IF NOT EXISTS decorative_fixtures (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_id INTEGER NOT NULL,
        fixture_id TEXT DEFAULT '',
        description TEXT DEFAULT '',
        fixture_type TEXT DEFAULT '',
        lamp_type TEXT DEFAULT '',
        lamp_quantity INTEGER DEFAULT 1,
        cct TEXT DEFAULT '',
        part_no TEXT DEFAULT '',
        dealer_price REAL DEFAULT 0,
        list_price REAL DEFAULT 0,
        status TEXT DEFAULT '',
        approved_date TEXT DEFAULT '',
        quantity INTEGER DEFAULT 1,
        sort_order INTEGER DEFAULT 0
    )""",
    """CREATE TABLE IF NOT EXISTS landscape_fixtures (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_id INTEGER NOT NULL,
        fixture_id TEXT DEFAULT '',
        description TEXT DEFAULT '',
        cct TEXT DEFAULT '',
        beam_spread TEXT DEFAULT '',
        fixture_type TEXT DEFAULT 'Fixed',
        control_type TEXT DEFAULT '',
        color TEXT DEFAULT '',
        trim_style TEXT DEFAULT '',
        trim_color TEXT DEFAULT '',
        finish TEXT DEFAULT '',
        wattage TEXT DEFAULT '',
        mount_part_no TEXT DEFAULT '',
        mount_dealer_price REAL DEFAULT 0,
        mount_list_price REAL DEFAULT 0,
        mount_status TEXT DEFAULT '',
        mount_approved_date TEXT DEFAULT '',
        fixture_part_no TEXT DEFAULT '',
        fixture_dealer_price REAL DEFAULT 0,
        fixture_list_price REAL DEFAULT 0,
        fixture_status TEXT DEFAULT '',
        fixture_approved_date TEXT DEFAULT '',
        lamp_part_no TEXT DEFAULT '',
        lamp_dealer_price REAL DEFAULT 0,
        lamp_list_price REAL DEFAULT 0,
        lamp_status TEXT DEFAULT '',
        lamp_approved_date TEXT DEFAULT '',
        accessory_part_no TEXT DEFAULT '',
        accessory_dealer_price REAL DEFAULT 0,
        accessory_list_price REAL DEFAULT 0,
        accessory_status TEXT DEFAULT '',
        accessory_approved_date TEXT DEFAULT '',
        quantity INTEGER DEFAULT 1,
        sort_order INTEGER DEFAULT 0
    )""",
    """CREATE TABLE IF NOT EXISTS landscape_accessories (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_id INTEGER NOT NULL,
        fixture_id TEXT DEFAULT '',
        description TEXT DEFAULT '',
        part_no TEXT DEFAULT '',
        dealer_price REAL DEFAULT 0,
        list_price REAL DEFAULT 0,
        status TEXT DEFAULT '',
        approved_date TEXT DEFAULT '',
        quantity INTEGER DEFAULT 1,
        sort_order INTEGER DEFAULT 0
    )""",
    """CREATE TABLE IF NOT EXISTS landscape_transformers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_id INTEGER NOT NULL,
        fixture_id TEXT DEFAULT '',
        description TEXT DEFAULT '',
        wattage TEXT DEFAULT '',
        control_type TEXT DEFAULT '',
        part_no TEXT DEFAULT '',
        dealer_price REAL DEFAULT 0,
        list_price REAL DEFAULT 0,
        status TEXT DEFAULT '',
        approved_date TEXT DEFAULT '',
        quantity INTEGER DEFAULT 1,
        sort_order INTEGER DEFAULT 0
    )""",
]

_MIGRATIONS = [
    ('recessed_fixtures', 'housing_approved_date', "TEXT DEFAULT ''"),
    ('recessed_fixtures', 'module_approved_date', "TEXT DEFAULT ''"),
    ('recessed_fixtures', 'trim_approved_date', "TEXT DEFAULT ''"),
    ('recessed_fixtures', 'driver_approved_date', "TEXT DEFAULT ''"),
    ('linear_fixtures', 'tape_approved_date', "TEXT DEFAULT ''"),
    ('linear_fixtures', 'driver_approved_date', "TEXT DEFAULT ''"),
    ('linear_fixtures', 'channel_approved_date', "TEXT DEFAULT ''"),
    ('linear_fixtures', 'channel_type', "TEXT DEFAULT ''"),
    ('recessed_fixtures', 'color', "TEXT DEFAULT ''"),
    ('linear_fixtures', 'color', "TEXT DEFAULT ''"),
    ('decorative_fixtures', 'cct', "TEXT DEFAULT ''"),
    ('decorative_fixtures', 'approved_date', "TEXT DEFAULT ''"),
    ('recessed_fixtures', 'trim_style', "TEXT DEFAULT ''"),
    ('recessed_fixtures', 'trim_color', "TEXT DEFAULT ''"),
    ('decorative_fixtures', 'part_no', "TEXT DEFAULT ''"),
]


def init_db():
    _execute_batch(_CREATE_TABLES)
    for table, column, col_type in _MIGRATIONS:
        try:
            _execute(f'ALTER TABLE {table} ADD COLUMN {column} {col_type}')
        except Exception:
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
    stmts = [(f'DELETE FROM {table} WHERE project_id = ?', [project_id]) for table in VALID_TABLES]
    stmts.append(('DELETE FROM projects WHERE id = ?', [project_id]))
    _execute_batch(stmts)


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
    """Duplicate an existing fixture row, returning the new row."""
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
    """Update sort_order for a list of fixture IDs. order is a list of fixture IDs in desired order."""
    if table not in VALID_TABLES:
        raise ValueError(f'Invalid table: {table}')
    statements = []
    for i, fixture_id in enumerate(order):
        statements.append((f'UPDATE {table} SET sort_order = ? WHERE id = ?', [i, fixture_id]))
    if statements:
        _execute_batch(statements)


def delete_fixture(table, fixture_id):
    if table not in VALID_TABLES:
        raise ValueError(f'Invalid table: {table}')
    _execute(f'DELETE FROM {table} WHERE id = ?', [fixture_id])
