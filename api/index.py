import sys
import os

# Add project root to path so imports work
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)

try:
    from app import app
except Exception as e:
    # Fallback: create a minimal app that shows the error
    from flask import Flask
    app = Flask(__name__)

    error_msg = str(e)

    @app.route('/', defaults={'path': ''})
    @app.route('/<path:path>')
    def catch_all(path):
        return f"""
        <h1>Import Error</h1>
        <p>{error_msg}</p>
        <pre>ROOT_DIR: {ROOT_DIR}</pre>
        <pre>sys.path: {sys.path}</pre>
        <pre>os.listdir ROOT: {os.listdir(ROOT_DIR) if os.path.exists(ROOT_DIR) else 'NOT FOUND'}</pre>
        <pre>env TURSO_DATABASE_URL: {'SET' if os.environ.get('TURSO_DATABASE_URL') else 'NOT SET'}</pre>
        <pre>env TURSO_AUTH_TOKEN: {'SET' if os.environ.get('TURSO_AUTH_TOKEN') else 'NOT SET'}</pre>
        """, 500
