#!/usr/bin/env python3
"""
Backtest de 🦅 Buitre en TODO el universo de futuros USDT-M de Binance
(no solo las 20 grandes del backtest original).

Reglas (las mismas del bot):
  - Velas de 1 h. Señal al cierre de una vela si en 4 h el precio cae 5 % o más
    Y el interés abierto (OI) cae 8 % o más.
  - Compra a la apertura de la vela siguiente.
  - Stop: mínimo de las 4 velas de la cascada menos 0,5 ATR(14).
  - Objetivo 2 R. Si en 48 h no tocó ninguno, cierra a mercado.
  - Si en la misma vela toca stop y objetivo, se cuenta el stop (conservador).
  - Una sola posición por moneda a la vez.
  - Cartera: 0,4 % de riesgo por operación y tope 6 % abierto => máximo 15 a la vez.

Datos: archivos públicos de https://data.binance.vision (velas 1h mensuales y
métricas de 5 min con el OI). Incluye monedas YA DESLISTADAS, para no tener sesgo
de supervivencia. Solo biblioteca estándar de Python.

Universo "en el momento": cada día se ordena por volumen en USDT del día anterior,
así "top 40" es el top 40 de ESE momento (como hace el bot en vivo).

Costos: comisión 0,05 % por lado + deslizamiento/spread según liquidez del momento
(top 20: 0,02 %, 21-100: 0,06 %, resto: 0,12 % por lado). No incluye funding.

Uso:
    python3 backtest_buitre_todas.py                       # 2022-01 -> último mes completo
    python3 backtest_buitre_todas.py --desde 2023-01 --hasta 2026-09
Resultados: resultado_buitre_todas.txt y resultado_buitre_todas.json
"""
import argparse, csv, io, json, math, os, random, sys, time, traceback, zipfile
import urllib.request, urllib.error
import xml.etree.ElementTree as ET
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta

BASE = "https://data.binance.vision"
LISTA = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
CACHE = "cache_buitre"
H = 3_600_000
DIA = 86_400_000

GRANDES = ["BTC", "ETH", "SOL", "XRP", "BNB", "DOGE", "ADA", "AVAX", "LINK", "LTC",
           "DOT", "NEAR", "SUI", "APT", "ARB", "OP", "TIA", "INJ", "WLD", "1000PEPE"]
EXCLUIR = {"BTCDOM", "DEFI", "FOOTBALL", "BLUEBIRD", "USDC", "FDUSD", "TUSD", "BUSD", "USDP"}

CAIDA_PRECIO = -0.05
CAIDA_OI = -0.08
VENTANA = 4
ATR_N = 14
ATR_MULT = 0.5
OBJ_R = 2.0
MAX_VELAS = 48
MAX_ABIERTAS = 15
COMISION = 0.0005
NUEVA_DIAS = 90


def deslizamiento(rank):
    if rank is None:
        return 0.0012
    if rank <= 20:
        return 0.0002
    if rank <= 100:
        return 0.0006
    return 0.0012


# ---------------------------------------------------------------- descarga
def get_bytes(url, intentos=6):
    ultimo = None
    for i in range(intentos):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "backtest-buitre"})
            with urllib.request.urlopen(req, timeout=60) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            ultimo = e
            time.sleep(2 * (i + 1) + (10 if e.code in (403, 429, 503) else 0))
        except Exception as e:
            ultimo = e
            time.sleep(2 * (i + 1))
    raise RuntimeError(f"no pude bajar {url}: {ultimo}")


def listar(prefijo):
    """Lista claves y subcarpetas bajo un prefijo del bucket público."""
    claves, carpetas, marker = [], [], ""
    while True:
        url = f"{LISTA}?delimiter=/&prefix={prefijo}" + (f"&marker={marker}" if marker else "")
        datos = get_bytes(url)
        if datos is None:
            break
        raiz = ET.fromstring(datos)
        ns = raiz.tag.split("}")[0] + "}" if raiz.tag.startswith("{") else ""
        for c in raiz.findall(f"{ns}Contents"):
            claves.append(c.find(f"{ns}Key").text)
        for p in raiz.findall(f"{ns}CommonPrefixes"):
            carpetas.append(p.find(f"{ns}Prefix").text)
        trunc = raiz.find(f"{ns}IsTruncated")
        if trunc is None or trunc.text != "true":
            break
        nm = raiz.find(f"{ns}NextMarker")
        marker = nm.text if nm is not None else (claves[-1] if claves else carpetas[-1])
    return claves, carpetas


def bajar_cache(ruta_remota):
    local = os.path.join(CACHE, ruta_remota.replace("data/futures/um/", ""))
    if os.path.exists(local):
        return local
    datos = get_bytes(f"{BASE}/{ruta_remota}")
    if datos is None:
        os.makedirs(os.path.dirname(local), exist_ok=True)
        open(local + ".404", "w").close()
        return None
    os.makedirs(os.path.dirname(local), exist_ok=True)
    tmp = local + ".tmp"
    with open(tmp, "wb") as f:
        f.write(datos)
    os.replace(tmp, local)
    return local


def leer_zip_csv(ruta):
    try:
        with zipfile.ZipFile(ruta) as z:
            nombre = z.namelist()[0]
            texto = z.read(nombre).decode("utf-8", "replace")
    except zipfile.BadZipFile:
        os.remove(ruta)
        return []
    filas = list(csv.reader(io.StringIO(texto)))
    if filas and filas[0] and not filas[0][0].strip().lstrip("-").replace(".", "").isdigit():
        filas = filas[1:]
    return filas


# ---------------------------------------------------------------- símbolos
def simbolos(log):
    _, carpetas = listar("data/futures/um/monthly/klines/")
    out = []
    for p in carpetas:
        s = p.rstrip("/").split("/")[-1]
        if not s.endswith("USDT") or "_" in s:
            continue
        base = s[:-4]
        if base in EXCLUIR:
            continue
        out.append(s)
    log(f"símbolos USDT-M encontrados (incluye deslistados): {len(out)}")
    return sorted(out)


def meses(desde, hasta):
    y, m = map(int, desde.split("-"))
    y2, m2 = map(int, hasta.split("-"))
    while (y, m) <= (y2, m2):
        yield f"{y:04d}-{m:02d}"
        m += 1
        if m == 13:
            y, m = y + 1, 1


def velas_simbolo(sym, desde, hasta):
    claves, _ = listar(f"data/futures/um/monthly/klines/{sym}/1h/")
    disp = {k.split("-1h-")[-1][:7] for k in claves if k.endswith(".zip")}
    filas = []
    for mes in meses(desde, hasta):
        if mes not in disp:
            continue
        ruta = bajar_cache(f"data/futures/um/monthly/klines/{sym}/1h/{sym}-1h-{mes}.zip")
        if ruta:
            filas.extend(leer_zip_csv(ruta))
    vistos, t, o, h, l, c, qv = set(), [], [], [], [], [], []
    for f in sorted(filas, key=lambda r: int(float(r[0]))):
        ts = int(float(f[0]))
        if ts > 10**14:
            ts //= 1000
        if ts in vistos:
            continue
        vistos.add(ts)
        t.append(ts); o.append(float(f[1])); h.append(float(f[2]))
        l.append(float(f[3])); c.append(float(f[4])); qv.append(float(f[7]))
    return {"t": t, "o": o, "h": h, "l": l, "c": c, "qv": qv}


def atr_wilder(v):
    n = len(v["t"])
    atr = [None] * n
    if n <= ATR_N:
        return atr
    trs = [v["h"][0] - v["l"][0]]
    for i in range(1, n):
        pc = v["c"][i - 1]
        trs.append(max(v["h"][i] - v["l"][i], abs(v["h"][i] - pc), abs(v["l"][i] - pc)))
    a = sum(trs[1:ATR_N + 1]) / ATR_N
    atr[ATR_N] = a
    for i in range(ATR_N + 1, n):
        a = (a * (ATR_N - 1) + trs[i]) / ATR_N
        atr[i] = a
    return atr


def candidatas_precio(v):
    """Índices i (vela de cierre de la cascada) donde el precio cayó >= 5 % en 4 h."""
    t, c = v["t"], v["c"]
    out = []
    for i in range(VENTANA, len(t) - 1):
        if t[i] - t[i - VENTANA] != VENTANA * H:
            continue
        if c[i - VENTANA] > 0 and c[i] / c[i - VENTANA] - 1 <= CAIDA_PRECIO:
            out.append(i)
    return out


def oi_dias(sym, dias):
    """{ms -> OI} con las métricas de 5 min de los días pedidos."""
    oi = {}
    for d in sorted(dias):
        ruta = bajar_cache(f"data/futures/um/daily/metrics/{sym}/{sym}-metrics-{d}.zip")
        if not ruta:
            continue
        for f in leer_zip_csv(ruta):
            try:
                ts = int(datetime.strptime(f[0][:19], "%Y-%m-%d %H:%M:%S")
                         .replace(tzinfo=timezone.utc).timestamp() * 1000)
                val = float(f[2])
            except (ValueError, IndexError):
                continue
            if val > 0:
                oi[ts] = val
    return oi


def oi_en(oi, ts):
    for atras in (0, 5, 10, 15):
        k = ts - atras * 60_000
        if k in oi:
            return oi[k]
    return None


def dia_str(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%d")


# ---------------------------------------------------------------- simulación
def simular(v, atr, i, entrada_override=None):
    """Simula una compra con señal al cierre de la vela i. Devuelve dict o None."""
    t, o, h, l, c = v["t"], v["o"], v["h"], v["l"], v["c"]
    n = len(t)
    if i + 1 >= n or t[i + 1] - t[i] != H or atr[i] is None:
        return None
    entrada = o[i + 1]
    minimo = min(l[i - VENTANA + 1:i + 1])
    stop = minimo - ATR_MULT * atr[i]
    if stop <= 0 or entrada <= stop:
        return None
    r = entrada - stop
    obj = entrada + OBJ_R * r
    resultado, salida_i, motivo = None, None, None
    ultimo = min(n - 1, i + MAX_VELAS)
    for j in range(i + 1, ultimo + 1):
        if j > i + 1 and t[j] - t[j - 1] != H:
            ultimo = j - 1
            break
        if l[j] <= stop:
            resultado, salida_i, motivo = -1.0, j, "stop"
            break
        if h[j] >= obj:
            resultado, salida_i, motivo = OBJ_R, j, "objetivo"
            break
    if resultado is None:
        salida_i = ultimo
        resultado = (c[ultimo] - entrada) / r
        motivo = "48h" if ultimo == i + MAX_VELAS else "fin de datos"
    return {"i": i, "entrada": entrada, "stop": stop, "r_pct": r / entrada,
            "bruto": resultado, "salida_i": salida_i, "motivo": motivo}


def procesar_simbolo(sym, args, rnd_seed):
    v = velas_simbolo(sym, args.desde, args.hasta)
    vol = defaultdict(float)
    for ts, q in zip(v["t"], v["qv"]):
        vol[ts // DIA] += q
    if len(v["t"]) < 100:
        return sym, dict(vol), [], []
    atr = atr_wilder(v)
    cands = candidatas_precio(v)
    # métricas OI solo de los días que hacen falta
    dias = set()
    for i in cands:
        dias.add(dia_str(v["t"][i] + H))
        dias.add(dia_str(v["t"][i] + H - VENTANA * H))
    oi = oi_dias(sym, dias) if dias else {}
    operaciones, sin_oi = [], 0
    libre_desde = -1
    for i in cands:
        if i <= libre_desde:
            continue
        cierre = v["t"][i] + H
        a, b = oi_en(oi, cierre), oi_en(oi, cierre - VENTANA * H)
        if a is None or b is None:
            sin_oi += 1
            continue
        if a / b - 1 > CAIDA_OI:
            continue
        op = simular(v, atr, i)
        if op is None:
            continue
        op["sym"] = sym
        op["t_senal"] = v["t"][i]
        op["t_entrada"] = v["t"][i + 1]
        op["t_salida"] = v["t"][op["salida_i"]] + H
        op["caida_precio"] = v["c"][i] / v["c"][i - VENTANA] - 1
        op["caida_oi"] = a / b - 1
        op["edad_dias"] = (v["t"][i] - v["t"][0]) / DIA
        # control al azar: misma moneda, hora al azar dentro de ±15 días, mismas reglas
        rnd = random.Random(rnd_seed + i)
        azar = []
        for _ in range(10):
            lo = max(ATR_N + VENTANA, i - 360)
            hi = min(len(v["t"]) - MAX_VELAS - 2, i + 360)
            if hi <= lo:
                break
            k = rnd.randint(lo, hi)
            z = simular(v, atr, k)
            if z:
                azar.append(z["bruto"])
        op["azar"] = sum(azar) / len(azar) if azar else None
        operaciones.append(op)
        libre_desde = op["salida_i"]
    del oi, v
    return sym, dict(vol), operaciones, [sin_oi, len(cands)]


# ---------------------------------------------------------------- estadística
def stats(ops, campo="neto"):
    n = len(ops)
    if n == 0:
        return {"ops": 0}
    rs = [o[campo] for o in ops]
    gan = [x for x in rs if x > 0]
    per = [x for x in rs if x <= 0]
    total = sum(rs)
    ordenadas = sorted(ops, key=lambda o: o["t_salida"])
    acum, pico, dd = 0.0, 0.0, 0.0
    for o in ordenadas:
        acum += o[campo]
        pico = max(pico, acum)
        dd = min(dd, acum - pico)
    media = total / n
    sd = math.sqrt(sum((x - media) ** 2 for x in rs) / (n - 1)) if n > 1 else 0.0
    azar = [o["azar"] for o in ops if o.get("azar") is not None]
    return {
        "ops": n,
        "ganadas_pct": round(100 * len(gan) / n, 1),
        "total_R": round(total, 1),
        "R_por_op": round(media, 3),
        "t_stat": round(media / (sd / math.sqrt(n)), 2) if sd > 0 else None,
        "profit_factor": round(sum(gan) / -sum(per), 2) if per and sum(per) < 0 else None,
        "peor_racha_R": round(dd, 1),
        "azar_R_por_op": round(sum(azar) / len(azar), 3) if azar else None,
    }


def cartera(ops):
    """Aplica el tope de 15 posiciones abiertas a la vez (0,4 % c/u, tope 6 %)."""
    tomadas, abiertas = [], []
    for o in sorted(ops, key=lambda x: (x["t_entrada"], x["sym"])):
        abiertas = [a for a in abiertas if a["t_salida"] > o["t_entrada"]]
        if len(abiertas) >= MAX_ABIERTAS:
            continue
        abiertas.append(o)
        tomadas.append(o)
    return tomadas


def tramo_rank(r):
    if r is None:
        return "sin ranking"
    if r <= 20:
        return "puesto 1-20"
    if r <= 40:
        return "puesto 21-40"
    if r <= 100:
        return "puesto 41-100"
    return "puesto 101+"


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--desde", default="2022-01")
    hoy = datetime.now(timezone.utc).replace(day=1) - timedelta(days=1)
    ap.add_argument("--hasta", default=hoy.strftime("%Y-%m"))
    ap.add_argument("--hilos", type=int, default=24)
    ap.add_argument("--max-simbolos", type=int, default=0, help="solo para pruebas")
    args = ap.parse_args()

    os.makedirs(CACHE, exist_ok=True)
    t0 = time.time()

    def log(m):
        print(f"[{time.time() - t0:7.0f}s] {m}", flush=True)

    syms = simbolos(log)
    if args.max_simbolos:
        syms = syms[:args.max_simbolos]

    vol_dia = defaultdict(dict)   # dia -> {sym: volumen USDT}
    todas, faltan_oi, cands_tot, sin_datos = [], 0, 0, 0
    with ThreadPoolExecutor(args.hilos) as ex:
        futuros = [ex.submit(procesar_simbolo, s, args, k * 100_000) for k, s in enumerate(syms)]
        for k, fu in enumerate(futuros, 1):
            sym, acum, ops, info = fu.result()
            fu._result = None
            if not acum:
                sin_datos += 1
            for d, q in acum.items():
                vol_dia[d][sym] = q
            todas.extend(ops)
            if info:
                faltan_oi += info[0]
                cands_tot += info[1]
            if k % 25 == 0 or k == len(syms):
                log(f"{k}/{len(syms)} monedas · {len(todas)} operaciones")

    # ranking del día anterior
    rank_dia = {}
    for d, vols in vol_dia.items():
        for pos, (s, _) in enumerate(sorted(vols.items(), key=lambda x: -x[1]), 1):
            rank_dia[(d, s)] = pos
    for o in todas:
        o["rank"] = rank_dia.get((o["t_senal"] // DIA - 1, o["sym"]))
        costo = 2 * (COMISION + deslizamiento(o["rank"]))
        o["costo_R"] = costo / o["r_pct"]
        o["neto"] = o["bruto"] - o["costo_R"]
        o["neto_x2"] = o["bruto"] - 2 * o["costo_R"]
        o["anio"] = datetime.fromtimestamp(o["t_senal"] / 1000, timezone.utc).year
        o["grande"] = o["sym"][:-4] in GRANDES

    universos = {
        "20 grandes (como el backtest original)": lambda o: o["grande"],
        "top 40 del momento (como el bot en vivo)": lambda o: o["rank"] is not None and o["rank"] <= 40,
        "top 100 del momento": lambda o: o["rank"] is not None and o["rank"] <= 100,
        "TODAS las monedas": lambda o: True,
        "todas MENOS las 20 grandes": lambda o: not o["grande"],
    }
    res = {"parametros": {
        "desde": args.desde, "hasta": args.hasta, "simbolos": len(syms),
        "simbolos_sin_velas": sin_datos, "candidatas_precio": cands_tot,
        "candidatas_sin_OI": faltan_oi,
        "costos": "comisión 0,05 %/lado + deslizamiento 0,02 % (top 20) / 0,06 % (21-100) / 0,12 % (resto) por lado; sin funding",
    }, "universos": {}, "tramos": {}, "edad": {}, "anios": {}, "monedas": {}}

    for nombre, f in universos.items():
        ops = [o for o in todas if f(o)]
        tom = cartera(ops)
        res["universos"][nombre] = {
            "todas_las_senales": stats(ops),
            "con_tope_15_abiertas": stats(tom),
            "bruto_sin_costos": stats(ops, "bruto"),
            "costos_dobles": stats(ops, "neto_x2"),
        }
    for tramo in ["puesto 1-20", "puesto 21-40", "puesto 41-100", "puesto 101+", "sin ranking"]:
        res["tramos"][tramo] = stats([o for o in todas if tramo_rank(o["rank"]) == tramo])
    res["edad"][f"listadas hace menos de {NUEVA_DIAS} días"] = stats([o for o in todas if o["edad_dias"] < NUEVA_DIAS])
    res["edad"][f"listadas hace {NUEVA_DIAS} días o más"] = stats([o for o in todas if o["edad_dias"] >= NUEVA_DIAS])
    for anio in sorted({o["anio"] for o in todas}):
        res["anios"][str(anio)] = {
            "TODAS": stats([o for o in todas if o["anio"] == anio]),
            "20 grandes": stats([o for o in todas if o["anio"] == anio and o["grande"]]),
            "resto": stats([o for o in todas if o["anio"] == anio and not o["grande"]]),
        }
    por_sym = defaultdict(list)
    for o in todas:
        por_sym[o["sym"]].append(o)
    ranking_sym = sorted(((s, sum(x["neto"] for x in v), len(v)) for s, v in por_sym.items()),
                         key=lambda x: x[1])
    res["monedas"] = {
        "peores": [{"moneda": s, "R": round(r, 1), "ops": n} for s, r, n in ranking_sym[:15]],
        "mejores": [{"moneda": s, "R": round(r, 1), "ops": n} for s, r, n in ranking_sym[-15:][::-1]],
        "con_operaciones": len(por_sym),
    }
    res["operaciones"] = [{
        "moneda": o["sym"], "senal": datetime.fromtimestamp(o["t_senal"] / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M"),
        "rank": o["rank"], "caida_precio_pct": round(100 * o["caida_precio"], 1),
        "caida_oi_pct": round(100 * o["caida_oi"], 1), "stop_pct": round(100 * o["r_pct"], 2),
        "bruto_R": round(o["bruto"], 3), "neto_R": round(o["neto"], 3), "salida": o["motivo"],
    } for o in sorted(todas, key=lambda x: x["t_senal"])]

    with open("resultado_buitre_todas.json", "w") as f:
        json.dump(res, f, ensure_ascii=False, indent=1)
    with open("resultado_buitre_todas.txt", "w") as f:
        f.write(texto(res))
    log("listo")
    print(texto(res))


def linea(nombre, s):
    if not s or s.get("ops", 0) == 0:
        return f"  {nombre}: sin operaciones\n"
    az = f" | al azar {s['azar_R_por_op']:+.3f} R/op" if s.get("azar_R_por_op") is not None else ""
    pf = f" | PF {s['profit_factor']}" if s.get("profit_factor") else ""
    t = f" | t {s['t_stat']}" if s.get("t_stat") is not None else ""
    return (f"  {nombre}: {s['ops']} ops, {s['ganadas_pct']} % ganadas, {s['total_R']:+.1f} R "
            f"({s['R_por_op']:+.3f} R/op){pf}{t} | peor racha {s['peor_racha_R']} R{az}\n")


def texto(res):
    p = res["parametros"]
    out = [f"BACKTEST 🦅 BUITRE EN TODAS LAS MONEDAS · {p['desde']} a {p['hasta']}\n",
           f"Monedas revisadas: {p['simbolos']} (incluye deslistadas). Costos: {p['costos']}.\n",
           f"Caídas de precio candidatas: {p['candidatas_precio']}; sin dato de OI: {p['candidatas_sin_OI']}.\n\n",
           "POR UNIVERSO (R netos de costos)\n"]
    for nombre, u in res["universos"].items():
        out.append(f"{nombre}\n")
        out.append(linea("todas las señales", u["todas_las_senales"]))
        out.append(linea("con tope 15 abiertas", u["con_tope_15_abiertas"]))
        out.append(linea("sin costos", u["bruto_sin_costos"]))
        out.append(linea("costos x2", u["costos_dobles"]))
    out.append("\nPOR PUESTO DE VOLUMEN EN ESE MOMENTO\n")
    for k, s in res["tramos"].items():
        out.append(linea(k, s))
    out.append("\nPOR ANTIGÜEDAD DE LA MONEDA\n")
    for k, s in res["edad"].items():
        out.append(linea(k, s))
    out.append("\nPOR AÑO\n")
    for anio, d in res["anios"].items():
        for k, s in d.items():
            out.append(linea(f"{anio} {k}", s))
    out.append("\nPEORES MONEDAS: " + ", ".join(f"{m['moneda']} {m['R']:+} R ({m['ops']})" for m in res["monedas"]["peores"]) + "\n")
    out.append("MEJORES MONEDAS: " + ", ".join(f"{m['moneda']} {m['R']:+} R ({m['ops']})" for m in res["monedas"]["mejores"]) + "\n")
    return "".join(out)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        with open("resultado_buitre_todas_error.txt", "w") as f:
            f.write(traceback.format_exc())
        raise
