from flask import Flask, render_template, request, jsonify, redirect, url_for, abort, make_response, g
from datetime import date, datetime
import os
import jwt
import database as db

app = Flask(__name__, static_folder='public/static')

PTECH_SESSION_SECRET = os.environ.get('PTECH_SESSION_SECRET', '')
DASHBOARD_LOGIN_URL = 'https://dashboard.ptech.la/login'


def validate_token(token):
    """Validate a JWT token. Returns True if valid."""
    if not token or not PTECH_SESSION_SECRET:
        return False
    try:
        jwt.decode(token, PTECH_SESSION_SECRET, algorithms=['HS256'])
        return True
    except (jwt.ExpiredSignatureError, jwt.InvalidTokenError):
        return False


@app.before_request
def require_dashboard_session():
    # Skip auth check for static assets
    if request.path.startswith('/static/'):
        return None

    # Check for token in URL parameter (passed by dashboard iframe)
    url_token = request.args.get('ptech_token')
    if validate_token(url_token):
        # Store token — cookie will be set in after_request
        g.ptech_token = url_token
        return None

    # Check for local session cookie
    cookie_token = request.cookies.get('ptech_session')
    if validate_token(cookie_token):
        return None

    # Not authenticated
    return redirect(DASHBOARD_LOGIN_URL)


@app.after_request
def set_session_cookie(response):
    token = getattr(g, 'ptech_token', None)
    if token:
        response.set_cookie('ptech_session', token, httponly=True,
                            secure=True, samesite='Lax', max_age=86400)
    return response

DECORATIVE_TYPES = [
    'Sconce', 'Chandelier', 'Pendant', 'Flush Mount', 'Semi-Flush',
    'Step Light', 'Table Lamp', 'Floor Lamp', 'Monopoint', 'Other'
]

STATUS_OPTIONS = ['', 'Quoting', 'Approved', 'Ordered', 'Shipped', 'Received', 'Delivered']

FIXTURE_TYPE_OPTIONS = ['Fixed', 'Adjustable', 'Wall Wash']

TRIM_STYLE_OPTIONS = ['', 'Trimmed', 'Trimless']

CONTROL_TYPE_OPTIONS = [
    '', '0-10V', 'DALI', 'DALI2', 'DMX', 'Forward Phase',
    'Reverse Phase', 'CCX', 'Non-Dim', 'Other'
]

CHANNEL_TYPE_OPTIONS = [
    '', 'Surface', 'Recessed', 'Pendant', 'Corner', 'Flexible', 'Other'
]

FIXTURE_COLORS = [
    {'value': '', 'label': '\u2014', 'hex': 'transparent'},
    {'value': 'red', 'label': 'Red', 'hex': '#ef4444'},
    {'value': 'orange', 'label': 'Orange', 'hex': '#f97316'},
    {'value': 'yellow', 'label': 'Yellow', 'hex': '#eab308'},
    {'value': 'green', 'label': 'Green', 'hex': '#22c55e'},
    {'value': 'light-blue', 'label': 'Light Blue', 'hex': '#38bdf8'},
    {'value': 'dark-blue', 'label': 'Dark Blue', 'hex': '#1e40af'},
    {'value': 'purple', 'label': 'Purple', 'hex': '#a855f7'},
    {'value': 'pink', 'label': 'Pink', 'hex': '#ec4899'},
    {'value': 'brown', 'label': 'Brown', 'hex': '#92400e'},
    {'value': 'black', 'label': 'Black', 'hex': '#1e293b'},
    {'value': 'gray', 'label': 'Gray', 'hex': '#6b7280'},
    {'value': 'white', 'label': 'White', 'hex': '#ffffff'},
]


@app.template_filter('currency')
def currency_filter(value):
    try:
        val = float(value)
        return f'${val:,.2f}'
    except (ValueError, TypeError):
        return '$0.00'


# --- Page Routes ---

@app.route('/')
def dashboard():
    projects = db.get_all_projects()
    return render_template('dashboard.html', projects=projects)


@app.route('/project/<int:project_id>')
def project_editor(project_id):
    project = db.get_project(project_id)
    if not project:
        return redirect(url_for('dashboard'))
    return render_template('editor.html',
                           project=project,
                           decorative_types=DECORATIVE_TYPES,
                           status_options=STATUS_OPTIONS,
                           fixture_type_options=FIXTURE_TYPE_OPTIONS,
                           control_type_options=CONTROL_TYPE_OPTIONS,
                           channel_type_options=CHANNEL_TYPE_OPTIONS,
                           fixture_colors=FIXTURE_COLORS,
                           trim_style_options=TRIM_STYLE_OPTIONS)


@app.route('/project/<int:project_id>/report/<report_type>')
def report(project_id, report_type):
    project = db.get_project(project_id)
    if not project:
        return redirect(url_for('dashboard'))
    if report_type not in ('fixture', 'dealer', 'list'):
        return redirect(url_for('project_editor', project_id=project_id))

    totals = compute_totals(project, report_type)

    return render_template(f'report_{report_type}.html',
                           project=project,
                           totals=totals,
                           status_options=STATUS_OPTIONS,
                           fixture_colors=FIXTURE_COLORS,
                           now=date.today().strftime('%B %d, %Y'))


@app.route('/project/<int:project_id>/purchase-order')
def purchase_order_page(project_id):
    project = db.get_project(project_id)
    if not project:
        return redirect(url_for('dashboard'))
    return render_template('purchase_order.html',
                           project=project,
                           now=date.today().strftime('%B %d, %Y'))


def compute_totals(project, report_type):
    price_key = 'dealer_price' if report_type == 'dealer' else 'list_price'
    subtotal = 0.0

    for f in project['recessed']:
        qty = f.get('quantity', 1) or 1
        line = 0
        for comp in ['housing', 'module', 'trim', 'driver']:
            line += float(f.get(f'{comp}_{price_key}', 0) or 0)
        subtotal += line * qty

    for f in project['recessed_accessories']:
        qty = f.get('quantity', 1) or 1
        price = float(f.get(price_key, 0) or 0)
        subtotal += price * qty

    for f in project['linear']:
        qty = f.get('quantity', 1) or 1
        line = 0
        for comp in ['tape', 'driver', 'channel']:
            line += float(f.get(f'{comp}_{price_key}', 0) or 0)
        subtotal += line * qty

    for f in project['linear_accessories']:
        qty = f.get('quantity', 1) or 1
        price = float(f.get(price_key, 0) or 0)
        subtotal += price * qty

    for f in project['decorative']:
        qty = f.get('quantity', 1) or 1
        price = float(f.get(price_key, 0) or 0)
        subtotal += price * qty

    for f in project['decorative_accessories']:
        qty = f.get('quantity', 1) or 1
        price = float(f.get(price_key, 0) or 0)
        subtotal += price * qty

    for f in project['landscape']:
        qty = f.get('quantity', 1) or 1
        line = 0
        for comp in ['mount', 'fixture', 'lamp', 'accessory']:
            line += float(f.get(f'{comp}_{price_key}', 0) or 0)
        subtotal += line * qty

    for f in project['transformers']:
        qty = f.get('quantity', 1) or 1
        price = float(f.get(price_key, 0) or 0)
        subtotal += price * qty

    for f in project['accessories']:
        qty = f.get('quantity', 1) or 1
        price = float(f.get(price_key, 0) or 0)
        subtotal += price * qty

    parts_pct = float(project.get('parts_extras_pct', 5.0) or 5.0)
    tax_pct = float(project.get('tax_pct', 10.5) or 10.5)
    shipping_pct = float(project.get('shipping_pct', 4.0) or 4.0)

    parts_allowance = subtotal * parts_pct / 100
    tax = (subtotal + parts_allowance) * tax_pct / 100
    shipping = subtotal * shipping_pct / 100
    grand_total = subtotal + parts_allowance + tax + shipping

    return {
        'subtotal': subtotal,
        'parts_allowance': parts_allowance,
        'parts_pct': parts_pct,
        'tax': tax,
        'tax_pct': tax_pct,
        'shipping': shipping,
        'shipping_pct': shipping_pct,
        'grand_total': grand_total,
    }


# --- API Routes ---

@app.route('/api/projects', methods=['POST'])
def api_create_project():
    data = request.get_json() or {}
    name = data.get('name', 'New Project').strip()
    address = data.get('address', '').strip()
    if not name:
        name = 'New Project'
    project_id = db.create_project(name, address)
    return jsonify({'id': project_id, 'name': name}), 201


@app.route('/api/projects/<int:project_id>', methods=['PATCH'])
def api_update_project(project_id):
    data = request.get_json() or {}
    db.update_project(project_id, data)
    return jsonify({'ok': True})


@app.route('/api/projects/<int:project_id>', methods=['DELETE'])
def api_delete_project(project_id):
    db.delete_project(project_id)
    return jsonify({'ok': True})


@app.route('/api/projects/<int:project_id>/fixtures/<table>', methods=['POST'])
def api_create_fixture(project_id, table):
    if table not in db.VALID_TABLES:
        return jsonify({'error': 'Invalid fixture type'}), 400
    fixture = db.create_fixture(table, project_id)
    return jsonify(fixture), 201


@app.route('/api/fixtures/<table>/<int:fixture_id>/duplicate', methods=['POST'])
def api_duplicate_fixture(table, fixture_id):
    if table not in db.VALID_TABLES:
        return jsonify({'error': 'Invalid fixture type'}), 400
    fixture = db.duplicate_fixture(table, fixture_id)
    if not fixture:
        return jsonify({'error': 'Fixture not found'}), 404
    return jsonify(fixture), 201


@app.route('/api/fixtures/<table>/<int:fixture_id>', methods=['PATCH'])
def api_update_fixture(table, fixture_id):
    if table not in db.VALID_TABLES:
        return jsonify({'error': 'Invalid fixture type'}), 400
    data = request.get_json() or {}

    # Auto-set approved_date when status changes to Approved
    for key, value in list(data.items()):
        if key.endswith('_status') or key == 'status':
            prefix = key.replace('_status', '_') if key != 'status' else ''
            date_field = f'{prefix}approved_date' if prefix else 'approved_date'
            if value == 'Approved':
                data[date_field] = datetime.now().strftime('%Y-%m-%d %H:%M')
            elif value in ('', 'Quoting'):
                data[date_field] = ''

    db.update_fixture(table, fixture_id, data)
    return jsonify({'ok': True})


@app.route('/api/fixtures/<table>/<int:fixture_id>', methods=['DELETE'])
def api_delete_fixture(table, fixture_id):
    if table not in db.VALID_TABLES:
        return jsonify({'error': 'Invalid fixture type'}), 400
    db.delete_fixture(table, fixture_id)
    return jsonify({'ok': True})


@app.route('/api/fixtures/<table>/reorder', methods=['POST'])
def api_reorder_fixtures(table):
    if table not in db.VALID_TABLES:
        return jsonify({'error': 'Invalid fixture type'}), 400
    data = request.get_json() or {}
    order = data.get('order', [])
    if not order:
        return jsonify({'error': 'No order provided'}), 400
    db.reorder_fixtures(table, order)
    return jsonify({'ok': True})


db.init_db()

if __name__ == '__main__':
    app.run(debug=True, port=5001)
