#!/usr/bin/env python3
"""geofill-address-server: lightweight self-hosted address API for GeoFill.

Implements exactly one endpoint the GeoFill extension talks to:

    GET /api/v1/generate?country=US&city=Seattle

Response shape (matches the extension's fetchAddressFromSelfHosted):

    {"data": {"result": {"address": {
        "components": {
            "houseNumber": "350",
            "street": "5th Ave",
            "locality": "New York",
            "admin1": "New York",
            "postcode": "10118"
        },
        "formattedAddress": "350 5th Ave, New York, NY 10118",
        "matchLevel": "premise"
    }}}}

Data: addresses.db (SQLite, built from Overture Maps open data).
Stdlib only (http.server + sqlite3). Zero third-party dependencies.

Env:
    ADDRESS_DB     path to SQLite file (default: ./data/addresses.db)
    API_HOST       bind host (default: 0.0.0.0)
    API_PORT       bind port (default: 8788)
    API_TOKEN      if set, require "Authorization: Bearer <token>"
"""
import json
import os
import random
import sqlite3
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

DB_PATH = os.environ.get('ADDRESS_DB', os.path.join(
    os.path.dirname(os.path.abspath(__file__)), 'data', 'addresses.db'))
API_HOST = os.environ.get('API_HOST', '0.0.0.0')
API_PORT = int(os.environ.get('API_PORT', '8788'))
API_TOKEN = os.environ.get('API_TOKEN', '')

RANDOM = random.SystemRandom()

# House-number placement per country.
# 'prefix': "350 5th Ave" | 'suffix': "Via Roma 10" | 'jp': "1-1 Chiyoda" |
# 'tw': "No. 12, Zhongxiao Rd"
NUMBER_STYLE = {
    'US': 'prefix', 'GB': 'prefix', 'CA': 'prefix', 'AU': 'prefix',
    'FR': 'prefix', 'IN': 'prefix', 'SG': 'prefix', 'RU': 'prefix',
    'CN': 'prefix', 'KR': 'prefix', 'HK': 'prefix',
    'DE': 'suffix', 'IT': 'suffix', 'ES': 'suffix', 'NL': 'suffix',
    'BR': 'suffix', 'MX': 'suffix',
    'JP': 'jp', 'TW': 'tw',
}


def connect():
    db = sqlite3.connect(f'file:{DB_PATH}?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    return db


def make_house_number(country, stored_number):
    """Use the real sampled number when available, else generate one."""
    if stored_number:
        return stored_number
    style = NUMBER_STYLE.get(country, 'prefix')
    if style == 'jp':
        return f'{RANDOM.randint(1, 30)}-{RANDOM.randint(1, 20)}'
    return str(RANDOM.randint(1, 999))


def format_street_line(country, number, street):
    style = NUMBER_STYLE.get(country, 'prefix')
    if style == 'suffix':
        return f'{street} {number}'
    if style == 'tw':
        return f'No. {number}, {street}'
    return f'{number} {street}'


def pick_address(country, city):
    db = connect()
    try:
        row = None
        if city:
            row = db.execute(
                'SELECT * FROM streets WHERE country_code=? AND city=? COLLATE NOCASE '
                'ORDER BY RANDOM() LIMIT 1', (country, city)).fetchone()
        if row is None:
            row = db.execute(
                'SELECT * FROM streets WHERE country_code=? ORDER BY RANDOM() LIMIT 1',
                (country,)).fetchone()
        return dict(row) if row else None
    finally:
        db.close()


def generate(country, city):
    row = pick_address(country, city)
    if row is None:
        return None
    number = make_house_number(country, row['number'])
    street_line = format_street_line(country, number, row['street'])
    formatted = f"{street_line}, {row['city']}, {row['admin1']} {row['postcode']}".strip()
    return {
        'data': {
            'result': {
                'address': {
                    'components': {
                        'houseNumber': number,
                        'street': row['street'],
                        'locality': row['city'],
                        'admin1': row['admin1'],
                        'postcode': row['postcode'],
                    },
                    'formattedAddress': formatted,
                    'matchLevel': 'premise',
                }
            }
        }
    }


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):  # keep stdout quiet-ish
        sys.stderr.write('%s - %s\n' % (self.address_string(), fmt % args))

    def _send(self, code, payload):
        body = json.dumps(payload, ensure_ascii=False).encode('utf-8')
        self.send_response(code)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _authorized(self):
        if not API_TOKEN:
            return True
        return self.headers.get('Authorization', '') == f'Bearer {API_TOKEN}'

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == '/health':
            return self._send(200, {'status': 'ok'})
        if parsed.path != '/api/v1/generate':
            return self._send(404, {'error': 'not found'})
        if not self._authorized():
            return self._send(401, {'error': 'unauthorized'})
        qs = parse_qs(parsed.query)
        country = (qs.get('country') or [''])[0].upper()
        city = (qs.get('city') or [''])[0].strip()
        if not country:
            return self._send(400, {'error': 'country is required'})
        result = generate(country, city)
        if result is None:
            return self._send(404, {'error': 'NO_POOL_COVERAGE',
                                    'message': f'no addresses for {country}'})
        return self._send(200, result)


def main():
    if not os.path.exists(DB_PATH):
        print(f'addresses.db not found at {DB_PATH}; build data first '
              f'(etl/extract.py --finalize)', file=sys.stderr)
        sys.exit(1)
    server = ThreadingHTTPServer((API_HOST, API_PORT), Handler)
    print(f'geofill-address-server listening on {API_HOST}:{API_PORT}',
          flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
