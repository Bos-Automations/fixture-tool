import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fixture_tool.db')

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


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA foreign_keys = ON')
    return conn


def init_db():
    conn = get_db()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS projects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL DEFAULT '',
            address TEXT DEFAULT '',
            parts_extras_pct REAL DEFAULT 5.0,
            tax_pct REAL DEFAULT 10.5,
            shipping_pct REAL DEFAULT 4.0,
            created_date TEXT DEFAULT (datetime('now','localtime')),
            updated_date TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS recessed_fixtures (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
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
            quantity INTEGER DEFAULT 1,
            sort_order INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS linear_fixtures (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
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
        );

        CREATE TABLE IF NOT EXISTS decorative_fixtures (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
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
        );

        CREATE TABLE IF NOT EXISTS landscape_fixtures (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
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
        );

        CREATE TABLE IF NOT EXISTS landscape_accessories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
            fixture_id TEXT DEFAULT '',
            description TEXT DEFAULT '',
            part_no TEXT DEFAULT '',
            dealer_price REAL DEFAULT 0,
            list_price REAL DEFAULT 0,
            status TEXT DEFAULT '',
            approved_date TEXT DEFAULT '',
            quantity INTEGER DEFAULT 1,
            sort_order INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS landscape_transformers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
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
        );
    ''')
    # Migrate existing databases: add new columns if they don't exist
    _migrate(conn)
    conn.close()


def _migrate(conn):
    """Add columns that may not exist in older databases."""
    migrations = [
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
    for table, column, col_type in migrations:
        try:
            conn.execute(f'ALTER TABLE {table} ADD COLUMN {column} {col_type}')
            conn.commit()
        except sqlite3.OperationalError:
            pass  # Column already exists


# --- Project CRUD ---

def get_all_projects():
    conn = get_db()
    rows = conn.execute('SELECT * FROM projects ORDER BY updated_date DESC').fetchall()
    conn.close()
    return [dict(r) for r in rows]


def create_project(name, address=''):
    conn = get_db()
    cur = conn.execute('INSERT INTO projects (name, address) VALUES (?, ?)', (name, address))
    project_id = cur.lastrowid
    conn.commit()
    conn.close()
    return project_id


def get_project(project_id):
    conn = get_db()
    project = conn.execute('SELECT * FROM projects WHERE id = ?', (project_id,)).fetchone()
    if not project:
        conn.close()
        return None
    result = dict(project)
    result['recessed'] = [dict(r) for r in conn.execute(
        'SELECT * FROM recessed_fixtures WHERE project_id = ? ORDER BY sort_order, id', (project_id,)
    ).fetchall()]
    result['linear'] = [dict(r) for r in conn.execute(
        'SELECT * FROM linear_fixtures WHERE project_id = ? ORDER BY sort_order, id', (project_id,)
    ).fetchall()]
    result['decorative'] = [dict(r) for r in conn.execute(
        'SELECT * FROM decorative_fixtures WHERE project_id = ? ORDER BY sort_order, id', (project_id,)
    ).fetchall()]
    result['landscape'] = [dict(r) for r in conn.execute(
        'SELECT * FROM landscape_fixtures WHERE project_id = ? ORDER BY sort_order, id', (project_id,)
    ).fetchall()]
    result['transformers'] = [dict(r) for r in conn.execute(
        'SELECT * FROM landscape_transformers WHERE project_id = ? ORDER BY sort_order, id', (project_id,)
    ).fetchall()]
    result['accessories'] = [dict(r) for r in conn.execute(
        'SELECT * FROM landscape_accessories WHERE project_id = ? ORDER BY sort_order, id', (project_id,)
    ).fetchall()]
    conn.close()
    return result


def update_project(project_id, data):
    conn = get_db()
    safe_data = {k: v for k, v in data.items() if k in PROJECT_COLUMNS}
    if not safe_data:
        conn.close()
        return
    sets = ', '.join(f'{k} = ?' for k in safe_data)
    values = list(safe_data.values()) + [project_id]
    conn.execute(
        f"UPDATE projects SET {sets}, updated_date = datetime('now','localtime') WHERE id = ?",
        values
    )
    conn.commit()
    conn.close()


def delete_project(project_id):
    conn = get_db()
    conn.execute('DELETE FROM projects WHERE id = ?', (project_id,))
    conn.commit()
    conn.close()


# --- Fixture CRUD ---

def create_fixture(table, project_id):
    if table not in VALID_TABLES:
        raise ValueError(f'Invalid table: {table}')
    conn = get_db()
    row = conn.execute(
        f'SELECT COALESCE(MAX(sort_order), -1) + 1 as next_order FROM {table} WHERE project_id = ?',
        (project_id,)
    ).fetchone()
    next_order = row['next_order']
    cur = conn.execute(
        f'INSERT INTO {table} (project_id, sort_order) VALUES (?, ?)',
        (project_id, next_order)
    )
    fixture_id = cur.lastrowid
    conn.commit()
    fixture = conn.execute(f'SELECT * FROM {table} WHERE id = ?', (fixture_id,)).fetchone()
    conn.close()
    return dict(fixture)


def duplicate_fixture(table, fixture_id):
    """Duplicate an existing fixture row, returning the new row."""
    if table not in VALID_TABLES:
        raise ValueError(f'Invalid table: {table}')
    conn = get_db()
    original = conn.execute(f'SELECT * FROM {table} WHERE id = ?', (fixture_id,)).fetchone()
    if not original:
        conn.close()
        return None
    orig = dict(original)
    project_id = orig['project_id']
    # Get next sort_order
    row = conn.execute(
        f'SELECT COALESCE(MAX(sort_order), -1) + 1 as next_order FROM {table} WHERE project_id = ?',
        (project_id,)
    ).fetchone()
    next_order = row['next_order']
    # Build column list excluding id
    valid_cols = TABLE_COLUMNS[table]
    cols = [c for c in orig.keys() if c != 'id' and c != 'project_id' and c in valid_cols]
    col_names = ', '.join(['project_id', 'sort_order'] + cols)
    placeholders = ', '.join(['?'] * (2 + len(cols)))
    values = [project_id, next_order] + [orig[c] for c in cols]
    cur = conn.execute(f'INSERT INTO {table} ({col_names}) VALUES ({placeholders})', values)
    new_id = cur.lastrowid
    conn.commit()
    new_fixture = conn.execute(f'SELECT * FROM {table} WHERE id = ?', (new_id,)).fetchone()
    conn.close()
    return dict(new_fixture)


def update_fixture(table, fixture_id, data):
    if table not in VALID_TABLES:
        raise ValueError(f'Invalid table: {table}')
    valid_cols = TABLE_COLUMNS[table]
    safe_data = {k: v for k, v in data.items() if k in valid_cols}
    if not safe_data:
        return
    conn = get_db()
    sets = ', '.join(f'{k} = ?' for k in safe_data)
    values = list(safe_data.values()) + [fixture_id]
    conn.execute(f'UPDATE {table} SET {sets} WHERE id = ?', values)
    conn.execute('''
        UPDATE projects SET updated_date = datetime('now','localtime')
        WHERE id = (SELECT project_id FROM ''' + table + ' WHERE id = ?)', (fixture_id,)
    )
    conn.commit()
    conn.close()


def delete_fixture(table, fixture_id):
    if table not in VALID_TABLES:
        raise ValueError(f'Invalid table: {table}')
    conn = get_db()
    conn.execute(f'DELETE FROM {table} WHERE id = ?', (fixture_id,))
    conn.commit()
    conn.close()
