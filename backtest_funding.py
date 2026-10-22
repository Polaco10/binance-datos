#!/usr/bin/env python3
"""
Backtest: arbitraje de funding (comprar spot + vender en corto el perpetuo del mismo tamaño).

Usa solo datos públicos de Binance y solo la biblioteca estándar de Python.
Correr en el droplet de Bangalore (desde ahí Binance responde):

    python3 backtest_funding.py            # últimos 2 años
    python3 backtest_funding.py --desde 2023-01-01

La primera vez baja los datos (unos 10-20 min por los límites de la API) y los guarda
en ./cache_funding/, así que las corridas siguientes son instantáneas.
Resultados: resultado_funding.txt y resultado_funding.json
"""
import argparse, json, os, sys, time, math, urllib.request, urllib.error
from urllib.parse import quote
from datetime import datetime, timezone, timedelta
from collections import defaultdict

FAPI = "https://fapi.binance.com"
SAPI = "https://api.binance.com"
CACHE = "cache_funding"
DIA_MS = 86_400_000


# ---------------------------------------------------------------- descarga
def get(url, intentos=5):
    for i in range(intentos):
        try:
            with urllib.request.urlopen(url, timeout=30) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            if e.code in (418, 429):          # límite de la API: esperar
                time.sleep(60 * (i + 1)); continue
            if e.code == 400:
                return []
            time.sleep(3 * (i + 1))
        except Exception:
            time.sleep(3 * (i + 1))
    raise RuntimeError("No pude bajar " + url)


def cacheado(nombre, funcion):
    os.makedirs(CACHE, exist_ok=True)
    ruta = os.path.join(CACHE, nombre + ".json")
    if os.path.exists(ruta):
        with open(ruta) as f:
            return json.load(f)
    datos = funcion()
    with open(ruta, "w") as f:
        json.dump(datos, f)
    return datos


def velas_diarias(simbolo, desde_ms):
    """Velas diarias del perpetuo: [dia_ms, apertura, maximo, cierre, volumenUSD]."""
    def bajar():
        out, t = [], desde_ms - 40 * DIA_MS
        while True:
            v = get(f"{FAPI}/fapi/v1/klines?symbol={quote(simbolo)}&interval=1d&startTime={t}&limit=1000")
            time.sleep(0.4)
            if not v:
                break
            out += [[int(x[0]), float(x[1]), float(x[2]), float(x[4]), float(x[7])] for x in v]
            if len(v) < 1000:
                break
            t = int(v[-1][0]) + DIA_MS
        return out
    return cacheado("velas_" + simbolo, bajar)


def funding(simbolo, desde_ms):
    """Pagos de funding: [hora_ms, tasa] (tasa en fracción, ej. 0.0001 = 0,01 %)."""
    def bajar():
        out, t = [], desde_ms
        while True:
            v = get(f"{FAPI}/fapi/v1/fundingRate?symbol={quote(simbolo)}&startTime={t}&limit=1000")
            time.sleep(0.7)               # límite: 500 pedidos cada 5 minutos
            if not v:
                break
            out += [[int(x["fundingTime"]), float(x["fundingRate"])] for x in v]
            if len(v) < 1000:
                break
            t = int(v[-1]["fundingTime"]) + 1
        return out
    return cacheado("funding_" + simbolo, bajar)


def universo():
    """Perpetuos USDT que también tienen spot (sin spot no se puede cubrir)."""
    fut = get(f"{FAPI}/fapi/v1/exchangeInfo")
    spot = get(f"{SAPI}/api/v3/exchangeInfo")
    bases_spot = {s["baseAsset"] for s in spot["symbols"]
                  if s["quoteAsset"] == "USDT" and s["status"] == "TRADING"}
    out = []
    for s in fut["symbols"]:
        if s["contractType"] != "PERPETUAL" or s["quoteAsset"] != "USDT" or s["status"] != "TRADING":
            continue
        base = s["baseAsset"]
        for pref in ("1000000", "1000"):          # 1000PEPE -> PEPE en spot
            if base.startswith(pref) and base[len(pref):] in bases_spot:
                base = base[len(pref):]
        if base in bases_spot:
            out.append(s["symbol"])
    return sorted(out)


# ---------------------------------------------------------------- simulación
def simular(datos, dias, p):
    """
    datos[sim] = {dia_ms: {"max":, "cierre":, "apertura":, "vol30":, "fund": suma de tasas del día}}
    p: parámetros (dict)
    """
    capital = p["capital"]
    lev = p["lev"]
    slots = p["slots"]
    costo_pierna = p["com_spot"] + p["com_fut"] + 2 * p["desliz"]   # por abrir o cerrar las 2 patas
    ent = p["entrada_anual"] / 365          # umbral diario
    sal = p["salida_anual"] / 365
    ventana = p["ventana"]

    efectivo = capital
    pos = {}       # sim -> {"S": tamaño spot USD, "precio_ref": precio último rebalanceo}
    hist_fund = defaultdict(list)
    curva, eventos = [], defaultdict(float)
    dias_invertido = 0

    for d in dias:
        # 1) funding del día para cada símbolo (se registra para las medias móviles)
        for sim, serie in datos.items():
            if d in serie:
                hist_fund[sim].append(serie[d]["fund"])
            else:
                hist_fund[sim].append(None)

        # 2) posiciones abiertas: cobrar funding, controlar el corto
        for sim in list(pos):
            x = datos[sim].get(d)
            if x is None:                      # el par dejó de cotizar: cerrar
                efectivo += pos[sim]["S"] * (1 + 1 / lev) * (1 - costo_pierna)
                eventos["cierres"] += 1
                del pos[sim]; continue
            S = pos[sim]["S"]
            ref = pos[sim]["precio_ref"]
            # valor nocional del corto hoy ≈ S * precio/ref; funding sobre el nocional
            nocional = S * x["cierre"] / ref
            cobro = nocional * x["fund"]
            eventos["funding_usd"] += cobro
            pos[sim]["S"] += cobro / (1 + 1 / lev)   # se reinvierte proporcionalmente
            # suba intradiaria contra el corto
            suba = x["max"] / ref - 1
            limite_liq = (1 / lev) * 0.9          # margen consumido casi entero
            if suba >= limite_liq and lev > 1:
                # liquidación: el spot compensa la pérdida del corto, pero se paga
                # la comisión de liquidación y hay que rearmar todo
                perdida = nocional * p["costo_liq"] + S * costo_pierna * 2
                pos[sim]["S"] -= perdida / (1 + 1 / lev)
                eventos["liquidaciones"] += 1
                eventos["costos_usd"] += perdida
                pos[sim]["precio_ref"] = x["cierre"]
            elif abs(x["cierre"] / ref - 1) >= 0.5 / lev:
                # rebalanceo: pasar ganancia de una pata a la otra (cuesta comisiones
                # sobre lo que se mueve)
                movido = S * abs(x["cierre"] / ref - 1)
                costo = movido * costo_pierna
                pos[sim]["S"] -= costo / (1 + 1 / lev)
                eventos["rebalanceos"] += 1
                eventos["costos_usd"] += costo
                pos[sim]["precio_ref"] = x["cierre"]

        # 3) señales: media de funding diaria de los últimos `ventana` días
        def media(sim):
            h = [v for v in hist_fund[sim][-ventana:] if v is not None]
            return sum(h) / len(h) if len(h) == ventana else None

        # salidas
        for sim in list(pos):
            m = media(sim)
            if m is None or m < sal:
                S = pos[sim]["S"]
                valor = S * (1 + 1 / lev)
                costo = S * costo_pierna
                efectivo += valor - costo
                eventos["costos_usd"] += costo
                eventos["cierres"] += 1
                del pos[sim]

        # entradas: los mejores pares con liquidez suficiente
        libres = slots - len(pos)
        if libres > 0:
            candidatos = []
            for sim, serie in datos.items():
                if sim in pos or d not in serie:
                    continue
                if serie[d]["vol30"] < p["vol_min"]:
                    continue
                m = media(sim)
                if m is not None and m >= ent:
                    candidatos.append((m, sim))
            candidatos.sort(reverse=True)
            valor_total = efectivo + sum(v["S"] * (1 + 1 / lev) for v in pos.values())
            por_slot = valor_total / slots
            for m, sim in candidatos[:libres]:
                usar = min(por_slot, efectivo)
                if usar < 10:
                    break
                S = usar / (1 + 1 / lev)
                costo = S * costo_pierna
                pos[sim] = {"S": S - costo / (1 + 1 / lev), "precio_ref": datos[sim][d]["cierre"]}
                efectivo -= usar
                eventos["costos_usd"] += costo
                eventos["aperturas"] += 1

        valor = efectivo + sum(v["S"] * (1 + 1 / lev) for v in pos.values())
        if pos:
            dias_invertido += 1
        curva.append((d, valor))

    return curva, dict(eventos), dias_invertido


def metricas(curva, capital):
    if not curva:
        return {}
    v0, vf = capital, curva[-1][1]
    anos = (curva[-1][0] - curva[0][0]) / DIA_MS / 365
    cagr = (vf / v0) ** (1 / anos) - 1 if anos > 0 and vf > 0 else float("nan")
    pico, dd = v0, 0
    for _, v in curva:
        pico = max(pico, v); dd = min(dd, v / pico - 1)
    por_ano = {}
    inicio = {}
    for d, v in curva:
        a = datetime.fromtimestamp(d / 1000, timezone.utc).year
        inicio.setdefault(a, v); por_ano[a] = v
    previo, anual = v0, {}
    for a in sorted(por_ano):
        anual[a] = por_ano[a] / previo - 1; previo = por_ano[a]
    return {"final": vf, "total": vf / v0 - 1, "anual_compuesto": cagr, "max_caida": dd, "por_ano": anual}


# ---------------------------------------------------------------- principal
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--desde", default=(datetime.now(timezone.utc) - timedelta(days=730)).strftime("%Y-%m-%d"))
    ap.add_argument("--capital", type=float, default=10_000)
    ap.add_argument("--vol-min", type=float, default=50e6, help="volumen diario promedio 30d mínimo (USD)")
    a = ap.parse_args()
    desde_ms = int(datetime.strptime(a.desde, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)

    print("Bajando lista de pares...", flush=True)
    sims = cacheado("universo", universo)
    print(f"{len(sims)} perpetuos con spot. Bajando velas diarias...", flush=True)

    velas = {}
    for i, s in enumerate(sims):
        velas[s] = velas_diarias(s, desde_ms)
        if i % 50 == 0:
            print(f"  velas {i}/{len(sims)}", flush=True)

    # solo pares que alguna vez tuvieron liquidez suficiente
    def vol30_serie(v):
        out, acc = {}, []
        for x in v:
            acc.append(x[4]); acc = acc[-30:]
            out[x[0]] = sum(acc) / len(acc)
        return out
    liquidos = [s for s in sims if velas[s] and max(vol30_serie(velas[s]).values()) >= a.vol_min]
    print(f"{len(liquidos)} pares con liquidez suficiente. Bajando funding...", flush=True)

    datos = {}
    for i, s in enumerate(liquidos):
        f = funding(s, desde_ms)
        fund_dia = defaultdict(float)
        for t, r in f:
            fund_dia[t // DIA_MS * DIA_MS] += r
        v30 = vol30_serie(velas[s])
        serie = {}
        for d, o, h, c, vol in velas[s]:
            if d >= desde_ms and o > 0:
                serie[d] = {"apertura": o, "max": h, "cierre": c, "vol30": v30[d], "fund": fund_dia.get(d, 0.0)}
        datos[s] = serie
        if i % 25 == 0:
            print(f"  funding {i}/{len(liquidos)}", flush=True)

    dias = sorted({d for s in datos.values() for d in s})
    # se descarta el último día (incompleto)
    dias = dias[:-1]

    base = dict(capital=a.capital, slots=5, com_spot=0.001, com_fut=0.0005, desliz=0.0005,
                costo_liq=0.015, vol_min=a.vol_min, ventana=3)
    escenarios = []
    for lev in (1, 2, 3):
        for ent, sal in ((0.10, 0.03), (0.20, 0.05), (0.30, 0.10)):
            escenarios.append(dict(base, lev=lev, entrada_anual=ent, salida_anual=sal))

    resultados = []
    for p in escenarios:
        curva, ev, inv = simular(datos, dias, p)
        m = metricas(curva, a.capital)
        resultados.append({"param": {k: p[k] for k in ("lev", "entrada_anual", "salida_anual", "slots")},
                           "metricas": m, "eventos": ev, "pct_dias_invertido": inv / max(1, len(dias))})

    # referencia: BTC comprar y aguantar
    btc = datos.get("BTCUSDT") or {d: x for d, x in
                                   {v[0]: {"cierre": v[3]} for v in velas.get("BTCUSDT", [])}.items() if d >= desde_ms}
    ref = None
    if btc:
        ks = [d for d in dias if d in btc]
        p0 = btc[ks[0]]["cierre"]
        ref = metricas([(d, a.capital * btc[d]["cierre"] / p0) for d in ks], a.capital)

    # funding promedio histórico de BTC y ETH, anualizado
    prom = {}
    for s in ("BTCUSDT", "ETHUSDT", "SOLUSDT"):
        if s in datos:
            f = [x["fund"] for d, x in datos[s].items() if d in set(dias)]
            if f:
                prom[s] = sum(f) / len(f) * 365

    # ---- informe
    pct = lambda x: f"{x*100:+.1f} %" if isinstance(x, float) and not math.isnan(x) else "—"
    lineas = [
        f"BACKTEST ARBITRAJE DE FUNDING · Binance · {datetime.fromtimestamp(dias[0]/1000, timezone.utc):%Y-%m-%d} a "
        f"{datetime.fromtimestamp(dias[-1]/1000, timezone.utc):%Y-%m-%d}",
        f"Capital {a.capital:,.0f} USD · hasta 5 pares a la vez · volumen mínimo {a.vol_min/1e6:.0f} M USD/día",
        "Costos: spot 0,10 % + futuros 0,05 % + deslizamiento 0,05 % por pata, en cada apertura, cierre y rebalanceo",
        "",
        "Funding promedio anualizado (lo que cobraría un corto quieto, sin costos):",
    ] + [f"  {s}: {pct(v)}" for s, v in prom.items()] + ["", "Resultados:"]
    for r in resultados:
        p, m, e = r["param"], r["metricas"], r["eventos"]
        lineas.append(
            f"  {p['lev']}x · entra >{p['entrada_anual']*100:.0f} % anual, sale <{p['salida_anual']*100:.0f} %: "
            f"anual {pct(m['anual_compuesto'])} · total {pct(m['total'])} · peor caída {pct(m['max_caida'])} · "
            f"invertido {r['pct_dias_invertido']*100:.0f} % de los días · "
            f"{int(e.get('aperturas',0))} entradas · {int(e.get('liquidaciones',0))} liquidaciones · "
            f"funding cobrado {e.get('funding_usd',0):,.0f} USD · costos {e.get('costos_usd',0):,.0f} USD")
        lineas.append("      por año: " + " · ".join(f"{k}: {pct(v)}" for k, v in m["por_ano"].items()))
    if ref:
        lineas += ["", f"Referencia BTC comprar y aguantar: anual {pct(ref['anual_compuesto'])} · "
                       f"total {pct(ref['total'])} · peor caída {pct(ref['max_caida'])}"]
    lineas += ["", "Limitaciones: solo incluye pares que cotizan hoy (los dados de baja no están, lo que mejora el",
               "resultado); se usan velas diarias, así que una suba y bajada dentro del mismo día puede subestimar",
               "liquidaciones; el efectivo sin usar no rinde nada."]
    texto = "\n".join(lineas)
    print("\n" + texto)
    with open("resultado_funding.txt", "w") as f:
        f.write(texto + "\n")
    with open("resultado_funding.json", "w") as f:
        json.dump({"resultados": resultados, "btc": ref, "funding_promedio": prom}, f, indent=1, default=str)


if __name__ == "__main__":
    main()
