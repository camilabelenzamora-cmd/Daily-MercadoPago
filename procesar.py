#!/usr/bin/env python3
"""
Procesa archivos XLSX/CSV del Daily Mercado Pago y actualiza los JSON del repo.
Uso: python procesar.py <ruta_al_archivo>
"""
import sys, json, os
from pathlib import Path
from datetime import datetime, timezone

try:
    import pandas as pd
except ImportError:
    print("ERROR Falta pandas. Instalar con: pip install pandas openpyxl")
    sys.exit(1)

REPO = Path(__file__).parent
DATA = REPO / "data"
SITES = ['MLA', 'MLB', 'MLM', 'MLC', 'MLU']
MAQUILA_SKUS_BY_SITE = {
    'MLM': ['40AMA', '40AMT', '81AMA']
}

def detect_site_from_filename(name):
    n = name.upper()
    for s in SITES:
        if s in n:
            return s
    return None

def detect_site_from_value(val):
    v = str(val or '').upper()
    if 'ARGENTINA' in v or 'ARBA' in v: return 'MLA'
    if 'BRSP' in v or 'BRASIL' in v or 'BRAZIL' in v: return 'MLB'
    if 'MEXICO' in v or 'MÉXI' in v or 'MEXI' in v or 'MEX' in v or 'MLM' in v or 'MEXICO' in v: return 'MLM'
    if 'CHILE' in v: return 'MLC'
    if 'URUGUAY' in v: return 'MLU'
    return None

def is_stock(name): n = name.lower(); return 'wmscaja' in n and 'hist' not in n
def is_maquila(name): n = name.lower(); return ('maquila' in n or 'caja' in n) and not is_stock(n)
def is_asn(name): n = name.lower(); return 'asn' in n or 'detalhe asn' in n
def is_despachos(name): n = name.lower(); return 'detalleos' in n or 'detalleorden' in n or 'ordemdesalida' in n or 'histlpndestino' in n.replace(' ', '')

def parse_date(raw):
    try:
        raw = str(raw).strip().split(' ')[0]
        if '-' in raw:
            parts = raw.split('-')
            if len(parts[0]) == 4:
                return raw  # already YYYY-MM-DD
            d, m, y = parts
        else:
            d, m, y = raw.split('/')
        return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
    except:
        return None

def classify_product(nom):
    n = str(nom or '').strip().lower()
    if any(x in n for x in ['tarjeta', 'card', 'prepaid', 'cartao', 'cartão']): return 'Cards'
    if n.startswith('point') or n.startswith('mini') or 'n950' in n or ('smart' in n and not n.startswith('funda')): return 'Point'
    if any(x in n for x in ['bobina', 'kit rollo', 'rollo']): return 'Bobinas'
    return 'Others'

def load_json(path):
    if path.exists():
        with open(path, encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_json(path, data):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def read_file(filepath):
    p = Path(filepath)
    if p.suffix.lower() in ('.xlsx', '.xls'):
        try:
            df = pd.read_excel(p, dtype=str, engine='calamine')
        except Exception:
            df = pd.read_excel(p, dtype=str)
    else:
        try:
            df = pd.read_csv(p, dtype=str, sep=None, engine='python', encoding='utf-8-sig')
        except Exception:
            df = pd.read_csv(p, dtype=str, sep=None, engine='python', encoding='latin-1')
    df = df.fillna('')
    return df.to_dict('records')

def process_despachos(rows):
    first = rows[0] if rows else {}
    site_col = next((c for c in ['SITIO FUENTE', 'LOCAL DE ORIGEM', 'LOCAL', 'SITIO'] if c in first), None)
    site_val = next((r[site_col] for r in rows if r.get(site_col)), '') if site_col else ''
    if not site_val:
        site_val = next((r.get('DESCR SITIO', '') for r in rows if r.get('DESCR SITIO')), '')
    site = detect_site_from_value(site_val)

    by_date = {}
    for r in rows:
        raw = r.get('FECHA DESPACHO') or r.get('DATA ENVIO') or r.get('DATA ENVIO ', '')
        fecha = parse_date(raw)
        if not fecha: continue
        if fecha not in by_date:
            by_date[fecha] = {'rows': [], 'os_ids': set()}
        by_date[fecha]['rows'].append(r)
        os_id = r.get('ID OS') or next((r[k] for k in r if 'PEDIDO' in k.upper()), '')
        if os_id: by_date[fecha]['os_ids'].add(str(os_id))

    results = []
    for fecha, d in by_date.items():
        products = {'Cards': 0, 'Point': 0, 'Bobinas': 0, 'Others': 0}
        routes = {'Cards': {}, 'Point': {}, 'Bobinas': {}, 'Others': {}}
        for r in d['rows']:
            nom = r.get('NOM PROD') or r.get('NOME PROD', '')
            prod = classify_product(nom)
            uds = int(float(r.get('UNI DESP') or r.get('UN ENVIADAS') or 0) or 0)
            ruta = (r.get('RUTA') or r.get('ROTA') or '').strip() or 'Sin ruta'
            products[prod] += uds
            if site == 'MLM' and ruta == 'XD - CARDS' and prod == 'Cards' and nom:
                if not isinstance(routes[prod].get(ruta), dict):
                    routes[prod][ruta] = {'total': routes[prod].get(ruta, 0), 'products': {}}
                routes[prod][ruta]['total'] += uds
                routes[prod][ruta]['products'][nom] = routes[prod][ruta]['products'].get(nom, 0) + uds
            else:
                if isinstance(routes[prod].get(ruta), dict):
                    routes[prod][ruta]['total'] += uds
                else:
                    routes[prod][ruta] = routes[prod].get(ruta, 0) + uds

        def route_val(v): return v['total'] if isinstance(v, dict) else v
        for p in products:
            routes[p] = dict(sorted(routes[p].items(), key=lambda x: route_val(x[1]), reverse=True))

        results.append({
            'fecha': fecha, 'site': site,
            'products': products, 'routes': routes,
            'total_unidades': sum(products.values()),
            'total_os': len(d['os_ids']),
            'uploadedAt': datetime.now(timezone.utc).isoformat()
        })
    return results, site

def process_maquila(rows):
    first = rows[0] if rows else {}
    site_col = next((c for c in ['DESCRIPCION SITIO', 'DESCRICAO LOCAL', 'LOCAL'] if c in first), None)
    site = detect_site_from_value(next((r[site_col] for r in rows if r.get(site_col)), '') if site_col else '')
    sku_filter = MAQUILA_SKUS_BY_SITE.get(site)

    by_date = {}
    for r in rows:
        raw = r.get('FECHA MOD.') or r.get('FECHA MOD') or \
              next((r[k] for k in r if 'DATA MOD' in k.upper() or 'DATA MODIF' in k.upper()), '')
        fecha = parse_date(str(raw).strip()) if str(raw).strip() and str(raw).strip() != 'NaN' else None
        if not fecha: continue
        if fecha not in by_date: by_date[fecha] = []
        by_date[fecha].append(r)

    results = []
    for fecha, drows in by_date.items():
        products, reps = {}, {}
        for r in drows:
            if sku_filter:
                sku = (r.get('PRODUCTO') or r.get('SKU') or '').strip().upper()
                if sku not in sku_filter: continue
            prod = (r.get('NOM PRODUCTO') or r.get('NOM PRODUTO') or '').strip()
            uds = int(float(r.get('UNIDADES MOVIMIENTO') or 0) or 0)
            rep = (r.get('USUARIO ULT MOD.') or r.get('USUARIO ULT MOD') or '').strip() or 'Sin usuario'
            if not prod or 'etiquetas places' in prod.lower(): continue
            products[prod] = products.get(prod, 0) + uds
            if prod not in reps: reps[prod] = {}
            reps[prod][rep] = reps[prod].get(rep, 0) + uds

        for p in reps: reps[p] = dict(sorted(reps[p].items(), key=lambda x: x[1], reverse=True))
        products = dict(sorted(products.items(), key=lambda x: x[1], reverse=True))
        results.append({
            'fecha': fecha, 'site': site,
            'products': products, 'reps': reps,
            'total': sum(products.values()),
            'uploadedAt': datetime.now(timezone.utc).isoformat()
        })
    return results, site

def process_asn(rows):
    first = rows[0] if rows else {}
    site_col = 'NOM SITIO' if 'NOM SITIO' in first else 'LOCAL DESTINO'
    site = detect_site_from_value(next((r[site_col] for r in rows if r.get(site_col)), '') if site_col in first else '')

    by_date = {}
    for r in rows:
        raw = r.get('FECHA ASIG PUERTA') or \
              next((r[k] for k in r if 'DATA ALOCA' in k.upper() or 'DOCA' in k.upper()), '')
        fecha = parse_date(str(raw).strip()) if str(raw).strip() and str(raw).strip() != 'NaN' else None
        if not fecha: continue
        if fecha not in by_date: by_date[fecha] = []
        by_date[fecha].append(r)

    results = []
    for fecha, drows in by_date.items():
        devoluciones, inbound_suppliers = {}, {}
        for r in drows:
            tipo = (r.get('TIPO ASN') or r.get('TIPO DE ASN') or '').strip()
            prod = (r.get('NOM PROD') or r.get('NOME PROD') or '').strip()
            prov = (r.get('ASN NOM PROV') or r.get('ASN NOM FORN') or '').strip() or 'Sin proveedor'
            uds = int(float(r.get('UN RECIBIDAS') or r.get('UN RECEBIDAS') or 0) or 0)
            if not prod or not uds: continue
            if tipo in ('DEVCLI', 'INS'):
                devoluciones[prod] = devoluciones.get(prod, 0) + uds
            else:
                if prov not in inbound_suppliers:
                    inbound_suppliers[prov] = {'total': 0, 'products': {}}
                inbound_suppliers[prov]['total'] += uds
                inbound_suppliers[prov]['products'][prod] = inbound_suppliers[prov]['products'].get(prod, 0) + uds

        inbound_sorted = dict(sorted(inbound_suppliers.items(), key=lambda x: x[1]['total'], reverse=True))
        results.append({
            'fecha': fecha, 'site': site,
            'devoluciones': {'products': devoluciones, 'total': sum(devoluciones.values())},
            'inbound': {'suppliers': inbound_sorted, 'total': sum(v['total'] for v in inbound_suppliers.values())},
            'uploadedAt': datetime.now(timezone.utc).isoformat()
        })
    return results, site

def process_stock(rows):
    EXCLUDE_ZONES = {'VR', 'STG'}
    site_val = next((r.get('DESCRIPCION SITIO','') for r in rows if r.get('DESCRIPCION SITIO','')), '')
    site = detect_site_from_value(site_val)

    def get_zone(ubi): return (ubi or '').split('-')[0].upper()
    def get_calle_key(ubi):
        parts = (ubi or '').split('-')
        return parts[2] if len(parts) >= 3 else None

    # Filter: only UBICADO or RECIBIDO, exclude virtual/staging zones
    valid = [r for r in rows if r.get('ESTADO','') in ('UBICADO','RECIBIDO')
             and get_zone(r.get('UBICACION','')) not in EXCLUDE_ZONES
             and r.get('UBICACION','')]

    def safe_int(v):
        try: return int(float(v)) if v not in (None,'','NaN') else 0
        except: return 0

    # Calles RK: count unique positions per calle key
    rk_rows = [r for r in valid if get_zone(r.get('UBICACION','')) == 'RK'
               and r.get('ESTADO','') == 'UBICADO']
    calle_positions = {}
    for r in rk_rows:
        key = get_calle_key(r['UBICACION'])
        if key: calle_positions.setdefault(key, set()).add(r['UBICACION'])
    calles = {k: len(v) for k, v in calle_positions.items()}

    # Zones: count unique positions and build detail for PK/MI/PKB/BUF/DK
    DETAIL_ZONES = {'PK', 'MI', 'PKB', 'BUF', 'DK'}
    zonas = {}
    zone_rows = [r for r in valid if r.get('ESTADO','') == 'UBICADO']
    zone_positions = {}
    zone_detail = {}
    for r in zone_rows:
        z = get_zone(r['UBICACION'])
        if z == 'RK': continue
        zone_positions.setdefault(z, set()).add(r['UBICACION'])
        if z in DETAIL_ZONES:
            ubi = r['UBICACION']
            sku = r.get('PRODUCTO','').strip()
            uds = safe_int(r.get('UNID. DISP.', 0))
            key = (ubi, sku)
            zone_detail.setdefault(z, {})
            zone_detail[z][key] = zone_detail[z].get(key, 0) + uds

    for z, positions in zone_positions.items():
        detalle = []
        if z in DETAIL_ZONES:
            for (ubi, sku), uds in sorted(zone_detail.get(z,{}).items()):
                if sku: detalle.append({'ubicacion': ubi, 'sku': sku, 'unidades': uds})
        zonas[z] = {'ocupadas': len(positions), 'detalle': detalle}

    # Recibido: stock in RECIBIDO state
    recibido_agg = {}
    for r in valid:
        if r.get('ESTADO','') == 'RECIBIDO':
            ubi = r.get('UBICACION','')
            sku = r.get('PRODUCTO','').strip()
            uds = safe_int(r.get('UNID. DISP.', 0))
            key = (ubi, sku)
            recibido_agg[key] = recibido_agg.get(key, 0) + uds
    recibido = [{'ubicacion': k[0], 'sku': k[1], 'unidades': v}
                for k, v in sorted(recibido_agg.items())]

    return {
        'site': site,
        'uploadedAt': datetime.now(timezone.utc).isoformat(),
        'calles': calles,
        'zonas': zonas,
        'recibido': recibido
    }


def main():
    if len(sys.argv) < 2:
        print("Uso: python procesar.py <ruta_al_archivo>")
        sys.exit(1)

    filepath = sys.argv[1]
    if not os.path.exists(filepath):
        print(f"ERROR Archivo no encontrado: {filepath}")
        sys.exit(1)

    name = Path(filepath).name
    print(f"[*] Procesando: {name}")

    rows = read_file(filepath)
    print(f"   {len(rows)} filas leídas")

    site_from_name = detect_site_from_filename(name)

    if is_stock(name):
        result = process_stock(rows)
        site = result.get('site') or site_from_name
        if not site: print("ERROR No se pudo detectar el pais"); sys.exit(1)
        json_path = DATA / f"{site}_stock.json"
        save_json(json_path, result)
        total_pos = sum(result['calles'].values())
        total_rec = len(result['recibido'])
        print(f"OK Stock {site} | {total_pos} pos. ocupadas en calles | {len(result['zonas'])} zonas | {total_rec} item(s) RECIBIDO -> {json_path.name}")

    elif is_maquila(name):
        entries, site = process_maquila(rows)
        site = site or site_from_name
        if not site: print("ERROR No se pudo detectar el país"); sys.exit(1)
        json_path = DATA / f"{site}_maquila.json"
        data = load_json(json_path)
        for e in entries: data[e['fecha']] = e
        save_json(json_path, data)
        total = sum(e['total'] for e in entries)
        print(f"OK Maquila {site} | {len(entries)} fecha(s) | {total:,} uds -> {json_path.name}")

    elif is_asn(name):
        entries, site = process_asn(rows)
        site = site or site_from_name
        if not site: print("ERROR No se pudo detectar el país"); sys.exit(1)
        json_path = DATA / f"{site}_asn.json"
        data = load_json(json_path)
        for e in entries: data[e['fecha']] = e
        save_json(json_path, data)
        total_dev = sum(e['devoluciones']['total'] for e in entries)
        total_inb = sum(e['inbound']['total'] for e in entries)
        print(f"OK ASN {site} | {len(entries)} fecha(s) | Dev: {total_dev:,} | Inbound: {total_inb:,} uds -> {json_path.name}")

    elif is_despachos(name):
        entries, site = process_despachos(rows)
        site = site or site_from_name
        if not site: print("ERROR No se pudo detectar el país"); sys.exit(1)
        json_path = DATA / f"{site}.json"
        data = load_json(json_path)
        for e in entries: data[e['fecha']] = e
        save_json(json_path, data)
        total = sum(e['total_unidades'] for e in entries)
        print(f"OK Despachos {site} | {len(entries)} fecha(s) | {total:,} uds -> {json_path.name}")

    else:
        print(f"ERROR No se reconoció el tipo de archivo '{name}'.")
        print("   El nombre debe contener: detalleOs / HistLpnDestino (despachos), caja/maquila, o asn")
        sys.exit(1)

if __name__ == '__main__':
    main()
