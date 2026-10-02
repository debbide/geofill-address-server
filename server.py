#!/usr/bin/env python3
"""geofill-address-server: lightweight self-hosted address API for GeoFill.

Implements exactly one endpoint the GeoFill extension talks to:

    GET /api/v1/generate?country=US&city=Seattle

Plus a tiny built-in index page at GET / for interactive use/testing
(country/city pickers populated from the DB, optional token field).

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
import re
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


# Plausible house numbers: 1-4 digits, optional single letter or range.
# Overture's raw `number` sometimes holds parcel ids ('10229') or unit
# designators ('3m'); those fail this check and we generate instead.
_NUMBER_OK = re.compile(r'^\d{1,4}[A-Za-z]?(-\d{1,4}[A-Za-z]?)?$')


def make_house_number(country, stored_number):
    """Use the real sampled number when plausible, else generate one."""
    if stored_number and _NUMBER_OK.match(stored_number.strip()):
        return stored_number.strip()
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
    tail = f"{row['admin1']} {row['postcode']}".strip()
    formatted = f"{street_line}, {row['city']}"
    if tail:
        formatted = f"{formatted}, {tail}"
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


def coverage():
    """{country_code: [city, ...]} for the index page dropdowns."""
    db = connect()
    try:
        out = {}
        for cc, city in db.execute(
                'SELECT DISTINCT country_code, city FROM streets '
                'ORDER BY country_code, city'):
            out.setdefault(cc, []).append(city)
        return out
    finally:
        db.close()


INDEX_TEMPLATE = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>GeoFill 地址生成器</title>
<style>
  :root { color-scheme: light dark; }
  body { font-family: system-ui, -apple-system, "Segoe UI", sans-serif;
         max-width: 640px; margin: 2rem auto; padding: 0 1rem; line-height: 1.5; }
  h1 { font-size: 1.4rem; margin-bottom: .2rem; }
  .sub { opacity: .65; font-size: .9rem; margin-bottom: 1.5rem; }
  label { display: block; font-size: .85rem; opacity: .8; margin: .8rem 0 .25rem; }
  select, input, button { font-size: 1rem; padding: .55rem .7rem; border-radius: 8px;
    border: 1px solid #8886; width: 100%; box-sizing: border-box; }
  button { margin-top: 1.2rem; cursor: pointer; font-weight: 600; }
  .card { margin-top: 1.5rem; border: 1px solid #8886; border-radius: 12px; padding: 1rem 1.2rem; }
  .addr { font-size: 1.15rem; font-weight: 650; word-break: break-word; }
  table { width: 100%; border-collapse: collapse; margin-top: .6rem; font-size: .92rem; }
  td { padding: .3rem .4rem; border-bottom: 1px solid #8883; }
  td:first-child { opacity: .65; width: 5.5rem; }
  .err { color: #c33; margin-top: 1rem; }
  .row { display: flex; gap: .6rem; }
  .row > * { flex: 1; }
</style>
</head>
<body>
<h1>🏠 GeoFill 地址生成器</h1>
<div class="sub">真实街道数据（Overture Maps）· 覆盖 __COVERAGE__ 个国家</div>

<label for="country">国家</label>
<select id="country"></select>

<label for="city">城市（可空，空 = 全国随机）</label>
<input id="city" list="cities" placeholder="如 Berlin" autocomplete="off">
<datalist id="cities"></datalist>

<label for="token">API Token（未启用可留空）</label>
<input id="token" type="password" placeholder="Bearer token" autocomplete="off">

<button id="go">生成地址</button>

<div id="out"></div>

<script>
const COVERAGE = __CITIES_JSON__;
const $ = id => document.getElementById(id);
const countrySel = $('country'), cityInput = $('city'), citiesList = $('cities');
Object.keys(COVERAGE).forEach(cc => {
  const o = document.createElement('option');
  o.value = cc; o.textContent = cc + '（' + COVERAGE[cc].length + ' 城）';
  countrySel.appendChild(o);
});
function syncCities() {
  citiesList.innerHTML = '';
  (COVERAGE[countrySel.value] || []).forEach(c => {
    const o = document.createElement('option'); o.value = c;
    citiesList.appendChild(o);
  });
}
countrySel.addEventListener('change', syncCities);
syncCities();
$('token').value = localStorage.getItem('apiToken') || '';
$('token').addEventListener('change', () =>
  localStorage.setItem('apiToken', $('token').value));

$('go').addEventListener('click', async () => {
  const out = $('out');
  const qs = new URLSearchParams({ country: countrySel.value });
  if (cityInput.value.trim()) qs.set('city', cityInput.value.trim());
  const headers = {};
  const tk = $('token').value.trim();
  if (tk) headers['Authorization'] = 'Bearer ' + tk;
  let resp;
  try {
    resp = await fetch('/api/v1/generate?' + qs, { headers });
  } catch (e) {
    out.innerHTML = '<div class="err">请求失败：' + e.message + '</div>';
    return;
  }
  if (!resp.ok) {
    const t = await resp.text();
    out.innerHTML = '<div class="err">HTTP ' + resp.status + '：' + t + '</div>';
    return;
  }
  const a = (await resp.json()).data.result.address;
  const c = a.components;
  const esc = s => String(s).replace(/[&<>"]/g, m =>
    ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[m]));
  out.innerHTML =
    '<div class="card"><div class="addr">' + esc(a.formattedAddress) + '</div>' +
    '<table>' +
    '<tr><td>门牌号</td><td>' + esc(c.houseNumber) + '</td></tr>' +
    '<tr><td>街道</td><td>' + esc(c.street) + '</td></tr>' +
    '<tr><td>城市</td><td>' + esc(c.locality) + '</td></tr>' +
    '<tr><td>州/省</td><td>' + esc(c.admin1) + '</td></tr>' +
    '<tr><td>邮编</td><td>' + esc(c.postcode) + '</td></tr>' +
    '</table>' +
    '<button id="cp">复制完整地址</button></div>';
  $('cp').addEventListener('click', () => navigator.clipboard.writeText(a.formattedAddress));
});
</script>
</body>
</html>"""


def render_index():
    cov = coverage()
    cities_json = json.dumps(cov, ensure_ascii=False)
    return (INDEX_TEMPLATE
            .replace('__COVERAGE__', str(len(cov)))
            .replace('__CITIES_JSON__', cities_json))


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

    def _send_html(self, code, html):
        body = html.encode('utf-8')
        self.send_response(code)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == '/':
            return self._send_html(200, render_index())
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
