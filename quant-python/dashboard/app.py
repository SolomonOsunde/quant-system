"""
Live trading dashboard — Plotly Dash.
Sections:
  1. Header + live price ticker strip
  2. Stats strip (P&L, signals, win rate, universe, positions, capital)
  3. Signal scanner — live per-symbol evaluation state (always visible)
  4. Executed trade setups (confidence ≥ 65%)
  5. Signal candidates (directional but below 65% gate)
  6. Portfolio P&L chart + Universe monitor
  7. Open positions + signal history
"""
import json
import time
import threading
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objects as go
import pandas as pd
import redis
from loguru import logger
import config

# ── Design tokens ──────────────────────────────────────────────────────────────
G    = "#00e5a0"   # green  — LONG / positive
R    = "#ff5050"   # red    — SHORT / negative
B    = "#4d9cff"   # blue   — info
Y    = "#ffd900"   # yellow — caution
OR   = "#ff9f40"   # orange — candidate signals
BG   = "#0b0e18"
C1   = "#131a2b"
C2   = "#1a2035"
BOR  = "#1e2942"
TP   = "#d4dce8"
TM   = "#4a5568"
TD   = "#2a3348"
MONO = "'JetBrains Mono','Fira Code','Courier New',monospace"


def start_dashboard(
    pnl_source      = None,
    signals_source  = None,
    portfolio       = None,
    prices_source   = None,
    state_lock      = None,
    universe_source = None,
    scores_source   = None,
):
    app = dash.Dash(
        __name__,
        title="Quant System",
        update_title=None,
        suppress_callback_exceptions=True,
    )

    try:
        rc = redis.Redis(
            host=config.REDIS_HOST, port=config.REDIS_PORT,
            decode_responses=True, socket_timeout=1,
        )
        rc.ping()
    except Exception:
        rc = None

    # ── Layout ─────────────────────────────────────────────────────────────────
    app.layout = html.Div([

        # Header
        html.Div([
            html.Div([
                html.Span("▣ QUANT SYSTEM", style={
                    "fontWeight": "700", "fontSize": "13px",
                    "letterSpacing": "2px", "color": TP,
                }),
                html.Span("PAPER MODE", style={
                    "background": f"rgba(0,229,160,0.12)", "color": G,
                    "fontSize": "10px", "fontWeight": "600",
                    "padding": "2px 8px", "borderRadius": "3px",
                    "border": f"1px solid {G}40", "marginLeft": "14px",
                    "letterSpacing": "1px",
                }),
                html.Span(id="live-badge", children="● LIVE", style={
                    "color": G, "fontSize": "10px", "marginLeft": "12px",
                    "letterSpacing": "1px",
                }),
            ], style={"display": "flex", "alignItems": "center"}),
            html.Span(id="clock", style={"color": TM, "fontSize": "12px"}),
        ], style={
            "display": "flex", "justifyContent": "space-between",
            "alignItems": "center", "padding": "10px 24px",
            "borderBottom": f"1px solid {BOR}", "background": "#0d1120",
        }),

        # Ops alerts strip
        html.Div([
            html.Span("OPS ALERTS", style={**_section_label(), "margin": "0 12px 0 0"}),
            html.Div(id="alerts-strip", style={"flex": 1, "overflowX": "auto"}),
        ], style={
            "display": "flex", "alignItems": "center",
            "padding": "8px 24px", "borderBottom": f"1px solid {BOR}",
            "background": "#0d1120", "minHeight": "36px",
        }),

        # Price ticker strip
        html.Div(id="ticker-strip", style={
            "display": "flex", "overflowX": "auto",
            "borderBottom": f"1px solid {BOR}",
            "background": "#0d1120", "scrollbarWidth": "none",
        }),

        # Stats strip
        html.Div(id="stats-strip", style={
            "display": "flex", "gap": "12px", "padding": "14px 24px 0",
        }),

        # Signal Scanner — always-on live evaluation state
        html.Div([
            html.Div([
                html.Span("SIGNAL SCANNER", style=_section_label()),
                html.Span("— live engine state per symbol",
                          style={"color": TM, "fontSize": "10px", "marginLeft": "8px"}),
            ], style={"display": "flex", "alignItems": "baseline", "marginBottom": "10px"}),
            html.Div(id="scanner-table"),
        ], style={
            "margin": "20px 24px 0",
            "background": C1, "border": f"1px solid {BOR}",
            "borderRadius": "10px", "padding": "14px 16px",
        }),

        # Executed trade setups
        html.Div([
            html.Div(f"EXECUTED TRADE SETUPS — confidence ≥ {config.MIN_CONFIDENCE:.0%}",
                     style=_section_label()),
            html.Div(id="signal-cards"),
        ], style={"padding": "20px 24px 0"}),

        # Signal candidates (sub-threshold)
        html.Div([
            html.Div("SIGNAL CANDIDATES — directional but below "
                     f"{config.MIN_CONFIDENCE:.0%} confidence",
                     style={**_section_label(), "color": OR}),
            html.Div(id="candidate-cards"),
        ], style={"padding": "16px 24px 0"}),

        # P&L chart + Universe monitor
        html.Div([
            html.Div([
                dcc.Graph(id="pnl-chart", config={"displayModeBar": False}),
            ], style={"flex": "1.4", "minWidth": 0}),
            html.Div([
                html.Div("UNIVERSE MONITOR", style=_section_label(mb="10px")),
                html.Div(id="universe-table"),
            ], style={
                "flex": "1", "minWidth": 0, "background": C1,
                "border": f"1px solid {BOR}", "borderRadius": "10px",
                "padding": "14px 16px", "overflowY": "auto", "maxHeight": "320px",
            }),
        ], style={"display": "flex", "gap": "14px",
                  "padding": "14px 24px 0", "alignItems": "flex-start"}),

        # Positions + history
        html.Div([
            html.Div([
                html.Div("OPEN POSITIONS", style=_section_label(mb="10px")),
                html.Div(id="positions-table"),
            ], style={
                "flex": "1", "background": C1, "border": f"1px solid {BOR}",
                "borderRadius": "10px", "padding": "14px 16px",
            }),
            html.Div([
                html.Div("SIGNAL HISTORY", style=_section_label(mb="10px")),
                html.Div(id="history-table"),
            ], style={
                "flex": "2.2", "background": C1, "border": f"1px solid {BOR}",
                "borderRadius": "10px", "padding": "14px 16px", "overflowX": "auto",
            }),
        ], style={"display": "flex", "gap": "14px", "padding": "14px 24px 28px"}),

        dcc.Interval(id="tick", interval=2_000, n_intervals=0),

    ], style={
        "background": BG, "color": TP, "minHeight": "100vh",
        "fontFamily": MONO, "fontSize": "13px",
    })

    # ── Callbacks ──────────────────────────────────────────────────────────────

    @app.callback(Output("clock", "children"), Input("tick", "n_intervals"))
    def _clock(_):
        return time.strftime("%Y-%m-%d  %H:%M:%S  UTC")

    @app.callback(
        Output("live-badge", "children"),
        Output("live-badge", "style"),
        Output("alerts-strip", "children"),
        Input("tick", "n_intervals"),
    )
    def _ops_status(_):
        hb = None
        alerts_raw = []
        if rc:
            try:
                raw = rc.get("quant:heartbeat:python")
                if raw:
                    hb = json.loads(raw)
                alerts_raw = rc.lrange("quant:alerts", 0, 7) or []
            except Exception:
                pass
        age = (time.time() - float(hb["ts"])) if hb and hb.get("ts") else 999
        if age < 15:
            badge, color = "● LIVE", G
        elif age < 45:
            badge, color = "● STALE", Y
        else:
            badge, color = "● DOWN", R
        style = {
            "color": color, "fontSize": "10px", "marginLeft": "12px",
            "letterSpacing": "1px",
        }
        chips = []
        for a in alerts_raw:
            try:
                ev = json.loads(a) if isinstance(a, str) else a
            except Exception:
                continue
            lvl = ev.get("level", "INFO")
            col = {"CRITICAL": R, "ERROR": R, "WARN": Y, "INFO": B}.get(lvl, TM)
            chips.append(html.Span(
                f"{ev.get('code', '?')}: {str(ev.get('message', ''))[:60]}",
                style={
                    "color": col, "fontSize": "10px", "marginRight": "14px",
                    "whiteSpace": "nowrap",
                },
            ))
        if not chips:
            chips = [html.Span("No recent alerts", style={"color": TM, "fontSize": "10px"})]
        return badge, style, chips

    @app.callback(
        Output("ticker-strip",    "children"),
        Output("stats-strip",     "children"),
        Output("scanner-table",   "children"),
        Output("signal-cards",    "children"),
        Output("candidate-cards", "children"),
        Output("pnl-chart",       "figure"),
        Output("universe-table",  "children"),
        Output("positions-table", "children"),
        Output("history-table",   "children"),
        Input("tick", "n_intervals"),
    )
    def _refresh(_):
        pnl_hist   = _fetch_pnl(rc, pnl_source, state_lock)
        signals    = _fetch_signals(rc, signals_source, state_lock)
        candidates = _fetch_candidates(rc)
        positions  = _fetch_positions(rc, portfolio, prices_source)
        prices     = _safe_prices(prices_source, state_lock)
        u_scores   = universe_source.scores          if universe_source else []
        u_active   = universe_source.active_universe if universe_source else set()
        scores     = dict(scores_source) if scores_source else {}

        total_pnl = pnl_hist[-1]["pnl"] if pnl_hist else 0.0
        n_buys    = sum(1 for s in signals if s.get("side") == "BUY")
        n_sells   = sum(1 for s in signals if s.get("side") == "SELL")
        win_rate  = _win_rate(signals)

        return (
            _ticker_strip(prices),
            _stats_strip(total_pnl, n_buys, n_sells,
                         len(positions), win_rate, len(u_active),
                         len(candidates)),
            _signal_scanner(scores),
            _signal_cards(signals, u_scores, u_active),
            _candidate_cards(candidates),
            _pnl_chart(pnl_hist),
            _universe_monitor(u_scores, u_active, prices),
            _positions_table(positions),
            _history_table(signals),
        )

    try:
        logger.info("Dashboard → http://{}:{}", config.DASHBOARD_HOST, config.DASHBOARD_PORT)
        app.run(host=config.DASHBOARD_HOST, port=config.DASHBOARD_PORT,
                debug=False, use_reloader=False)
    except Exception as e:
        logger.error("Dashboard error: {}", e)


# ── Price ticker strip ──────────────────────────────────────────────────────────

def _ticker_strip(prices: dict):
    if not prices:
        return [html.Span("Connecting to market feed…",
                          style={"color": TM, "fontSize": "11px",
                                 "padding": "8px 24px"})]
    return [
        html.Div([
            html.Span(sym.replace("/USD", ""),
                      style={"color": TM, "fontSize": "10px",
                             "marginRight": "5px"}),
            html.Span(_fmt(price),
                      style={"color": TP, "fontWeight": "600", "fontSize": "12px"}),
        ], style={
            "padding": "7px 18px", "borderRight": f"1px solid {BOR}",
            "whiteSpace": "nowrap", "flexShrink": "0",
        })
        for sym, price in sorted(prices.items())
    ]


# ── Signal Scanner ─────────────────────────────────────────────────────────────

_STATUS_ORDER = {"SIGNAL": 0, "CANDIDATE": 1, "NEUTRAL": 2, "CRISIS": 3, "SPREAD_WIDE": 4, "WARMING": 5}
_STATUS_COLOR = {
    "SIGNAL":      G,
    "CANDIDATE":   OR,
    "NEUTRAL":     TM,
    "CRISIS":      R,
    "SPREAD_WIDE": TM,
    "WARMING":     TD,
}

def _signal_scanner(scores: dict):
    if not scores:
        return html.Div("Waiting for first evaluation cycle…",
                        style={"color": TM, "fontStyle": "italic", "fontSize": "11px"})

    col = lambda w, color=TP: {
        "width": w, "flexShrink": "0", "padding": "3px 6px",
        "fontSize": "11px", "color": color, "fontFamily": MONO,
        "overflow": "hidden", "whiteSpace": "nowrap",
    }
    hcol = lambda w: {**col(w, TM), "fontSize": "9px", "fontWeight": "600",
                      "letterSpacing": "1px", "paddingBottom": "6px"}

    header = html.Div([
        html.Span("SYMBOL",    style=hcol("110px")),
        html.Span("PRICE",     style=hcol("90px")),
        html.Span("STATUS",    style=hcol("90px")),
        html.Span("DIR",       style=hcol("52px")),
        html.Span("COMPOSITE", style=hcol("84px")),
        html.Span("CONF",      style=hcol("58px")),
        html.Span("RSI",       style=hcol("50px")),
        html.Span("REGIME",    style=hcol("90px")),
        html.Span("TICKS",     style=hcol("52px")),
        html.Span("UPDATED",   style=hcol("64px")),
        html.Span("NOTE",      style={**hcol("200px"), "flex": "1"}),
    ], style={"display": "flex", "borderBottom": f"1px solid {BOR}"})

    rows = [header]
    for sc in sorted(scores.values(),
                     key=lambda x: (_STATUS_ORDER.get(x.get("status", "WARMING"), 9),
                                    x.get("symbol", ""))):
        status     = sc.get("status", "WARMING")
        direction  = sc.get("direction", "")
        confidence = sc.get("confidence") or 0.0
        composite  = sc.get("composite")
        rsi        = sc.get("rsi")
        regime     = sc.get("regime", "")
        ticks      = sc.get("ticks", 0)
        price      = sc.get("price", 0)

        sc_color   = _STATUS_COLOR.get(status, TM)
        dir_color  = G if direction == "BUY" else (R if direction == "SELL" else TM)
        conf_color = (G if confidence >= config.MIN_CONFIDENCE
                      else OR if confidence >= 0.35 else TM)
        if composite is not None:
            comp_str   = f"+{composite:.3f}" if composite > 0 else f"{composite:.3f}"
            comp_color = G if composite > 0.08 else (R if composite < -0.08 else TM)
        else:
            comp_str, comp_color = "—", TM

        note = sc.get("risk_flags") or sc.get("reasoning", "")
        if len(note) > 60:
            note = note[:57] + "…"

        # Highlight rows that are approaching or at signal threshold
        row_bg = f"rgba(0,229,160,0.04)" if status == "SIGNAL" else (
                 f"rgba(255,159,64,0.04)" if status == "CANDIDATE" else "transparent")

        rows.append(html.Div([
            html.Span(sc.get("symbol", ""),           style=col("110px")),
            html.Span(_fmt(price) if price else "—",  style=col("90px")),
            html.Span(status,                         style=col("90px",  sc_color)),
            html.Span(direction or "—",               style=col("52px",  dir_color)),
            html.Span(comp_str,                       style=col("84px",  comp_color)),
            html.Span(f"{confidence:.0%}" if confidence else "—",
                                                      style=col("58px",  conf_color)),
            html.Span(f"{rsi:.1f}" if rsi else "—",  style=col("50px")),
            html.Span(regime or "—",                  style=col("90px",  TM)),
            html.Span(str(ticks),                     style=col("52px",  TM)),
            html.Span(sc.get("updated", ""),          style=col("64px",  TD)),
            html.Span(note,                           style={**col("200px", TM), "flex": "1"}),
        ], style={
            "display": "flex", "alignItems": "center",
            "borderBottom": f"1px solid {BOR}22",
            "background": row_bg,
            "transition": "background 0.3s",
        }))

    return html.Div(rows, style={"overflowX": "auto"})


# ── Stats strip ────────────────────────────────────────────────────────────────

def _stats_strip(total_pnl, n_buys, n_sells, n_pos, win_rate, n_universe, n_cands):
    pc = G if total_pnl >= 0 else R
    items = [
        ("Net P&L",        f"${total_pnl:+,.2f}",               pc),
        ("Signals",        f"{n_buys}L  {n_sells}S",             TP),
        ("Candidates",     str(n_cands),                         OR,
         f"Near-miss signals below {config.MIN_CONFIDENCE:.0%} threshold"),
        ("Win Rate",       f"{win_rate:.0%}" if win_rate else "—",
         G if win_rate and win_rate >= 0.5 else Y),
        ("Universe",       str(n_universe),                      B),
        ("Open Positions", str(n_pos),                           Y if n_pos else TM),
    ]
    cards = []
    for item in items:
        label, value, color = item[0], item[1], item[2]
        hint = item[3] if len(item) > 3 else ""
        cards.append(html.Div([
            html.Div(label, style={"color": TM, "fontSize": "10px",
                                   "letterSpacing": "0.6px", "marginBottom": "5px"}),
            html.Div(value, style={"color": color, "fontSize": "22px",
                                   "fontWeight": "700", "lineHeight": "1"}),
            html.Div(hint,  style={"color": TD, "fontSize": "9px",
                                   "marginTop": "5px"}),
        ], style={
            "background": C1, "border": f"1px solid {BOR}",
            "borderRadius": "8px", "padding": "12px 16px", "flex": "1",
        }))
    return cards


# ── Executed signal cards ──────────────────────────────────────────────────────

def _signal_cards(signals, u_scores, u_active):
    actionable = [s for s in signals if s.get("side") in ("BUY", "SELL")]
    if not actionable:
        return _warming_up(u_scores, u_active)
    cards = [_signal_card(s, executed=True) for s in reversed(actionable[-4:])]
    return html.Div(cards, style={
        "display": "grid",
        "gridTemplateColumns": "repeat(auto-fill, minmax(300px, 1fr))",
        "gap": "14px",
    })


def _warming_up(u_scores, u_active):
    total  = len(u_scores)
    active = len(u_active)
    if total == 0:
        msg = "Warming up — collecting market data…"
    else:
        msg = (f"{active}/{total} symbols scored  •  "
               f"waiting for ≥{config.MIN_CONFIDENCE:.0%} confidence setup")

    rows = []
    for s in (u_scores or [])[:6]:
        bar_w  = min(100, int(s.tick_count / 100 * 100))
        active_dot = html.Span("●", style={"color": G, "marginLeft": "6px",
                                            "fontSize": "10px"}) if s.symbol in u_active else None
        rows.append(html.Div([
            html.Span(s.symbol, style={"width": "90px", "color": TP,
                                        "fontSize": "11px"}),
            html.Div([
                html.Div(style={
                    "width": f"{bar_w}%", "height": "100%",
                    "background": G if s.symbol in u_active else B,
                    "borderRadius": "2px",
                }),
            ], style={"flex": "1", "height": "6px", "background": C2,
                      "borderRadius": "2px", "margin": "0 10px"}),
            html.Span(f"{s.tick_count} ticks",
                      style={"color": TM, "fontSize": "10px",
                             "width": "70px", "textAlign": "right"}),
            active_dot,
        ], style={"display": "flex", "alignItems": "center", "marginBottom": "8px"}))

    return html.Div([
        html.Div(msg, style={"color": TM, "fontSize": "12px",
                              "marginBottom": "14px" if rows else "0"}),
        html.Div(rows) if rows else None,
    ], style={
        "background": C1, "border": f"1px solid {BOR}",
        "borderRadius": "10px", "padding": "20px 24px",
    })


# ── Candidate cards (sub-threshold signals) ────────────────────────────────────

def _candidate_cards(candidates: list):
    if not candidates:
        return html.Div(
            f"No candidates yet — will appear when the engine generates "
            f"directional signals below the {config.MIN_CONFIDENCE:.0%} gate.",
            style={"color": TM, "fontStyle": "italic", "fontSize": "12px",
                   "padding": "16px 20px", "background": C1,
                   "border": f"1px solid {BOR}", "borderRadius": "10px"},
        )

    recent = [c for c in candidates if c.get("side") in ("BUY", "SELL")][-6:]
    cards  = [_signal_card(c, executed=False) for c in reversed(recent)]
    return html.Div(cards, style={
        "display": "grid",
        "gridTemplateColumns": "repeat(auto-fill, minmax(280px, 1fr))",
        "gap": "12px",
    })


def _signal_card(sig: dict, executed: bool = True):
    side    = sig.get("side", "BUY")
    symbol  = sig.get("symbol", "")
    market  = sig.get("market", "")
    ts      = sig.get("timestamp", "")
    is_long = side == "BUY"
    label   = "LONG" if is_long else "SHORT"
    accent  = G if is_long else R
    if not executed:
        accent = f"{OR}"

    entry   = sig.get("entry",       sig.get("price", 0))
    sl      = sig.get("stop_loss",   0)
    tp      = sig.get("take_profit", 0)
    sl_pct  = sig.get("sl_pct",      0)
    tp_pct  = sig.get("tp_pct",      0)
    lev     = sig.get("leverage",    1)
    conf    = sig.get("confidence",  0)
    rsi     = sig.get("rsi",         0)
    comp    = sig.get("composite",   0)
    reason  = sig.get("reasoning",   "")
    flags   = sig.get("risk_flags",  [])

    if "Trade setup →" in reason:
        reason = reason[:reason.index("Trade setup →")].strip()

    rr = (tp_pct / sl_pct) if sl_pct else 0

    return html.Div([

        # Header row
        html.Div([
            html.Span(label, style={
                "background": accent, "color": "#000" if executed else BG,
                "fontWeight": "700", "padding": "2px 10px",
                "borderRadius": "4px", "fontSize": "11px",
                "letterSpacing": "1px", "marginRight": "10px",
                "border": f"1px solid {accent}" if not executed else "none",
            }),
            html.Span(symbol, style={"fontSize": "14px", "fontWeight": "700"}),
            html.Span(f"  {market}", style={"color": TM, "fontSize": "10px",
                                             "marginLeft": "6px"}),
            html.Span(ts, style={"marginLeft": "auto", "color": TD,
                                  "fontSize": "10px"}),
        ], style={"display": "flex", "alignItems": "center",
                  "marginBottom": "12px"}),

        # Confidence bar
        html.Div([
            html.Div([
                html.Span("Confidence",
                          style={"color": TM, "fontSize": "10px"}),
                html.Span(f"{conf:.0%}",
                          style={"color": B if executed else OR,
                                 "fontWeight": "700", "marginLeft": "auto"}),
            ], style={"display": "flex", "marginBottom": "4px"}),
            html.Div([
                html.Div(style={
                    "width": f"{conf * 100:.0f}%", "height": "100%",
                    "background": (f"linear-gradient(90deg,{B}80,{B})"
                                   if executed
                                   else f"linear-gradient(90deg,{OR}80,{OR})"),
                    "borderRadius": "2px",
                }),
            ], style={"height": "5px", "background": C2,
                      "borderRadius": "2px", "marginBottom": "10px"}),
        ]),

        # Levels (only for executed signals that have SL/TP)
        html.Div([
            _lv("Entry",       _fmt(entry),                    TP),
            _lv("Stop Loss",   f"{_fmt(sl)}  −{sl_pct:.2f}%",  R)  if sl else None,
            _lv("Take Profit", f"{_fmt(tp)}  +{tp_pct:.2f}%",  G)  if tp else None,
            _lv("R:R",         f"{rr:.1f} : 1",
                G if rr >= 2 else (Y if rr >= 1 else R))             if tp else None,
            _lv("Leverage",    f"{lev}x",                       Y)  if lev and executed else None,
            _lv("Composite",   f"{comp:+.4f}",                  TM),
        ] if executed or entry else [
            _lv("Price",     _fmt(sig.get("price", 0)),        TP),
            _lv("Composite", f"{comp:+.4f}",                   OR),
            _lv("RSI",       f"{rsi:.1f}",                     TM),
        ], style={"marginBottom": "10px"}),

        # Risk flags
        html.Div([
            html.Span(f"⚠ {f}", style={"color": Y, "fontSize": "10px",
                                         "marginRight": "8px"})
            for f in flags[:3]
        ]) if flags else None,

        # Reasoning
        html.Div([
            html.Div("AI ANALYSIS" if executed else "REASON BELOW GATE",
                     style={"color": TD, "fontSize": "9px",
                            "letterSpacing": "1px", "marginBottom": "4px"}),
            html.Div(reason[:180] + ("…" if len(reason) > 180 else ""),
                     style={"color": TM, "fontSize": "11px",
                            "lineHeight": "1.5", "fontStyle": "italic"}),
        ]),

    ], style={
        "background": C1,
        "border": f"1px solid {accent}30",
        "borderLeft": f"3px solid {accent}",
        "borderRadius": "8px",
        "padding": "14px 16px",
        "opacity": "0.85" if not executed else "1",
    })


def _lv(label, value, color):
    if value is None:
        return None
    return html.Div([
        html.Span(label, style={"color": TM, "fontSize": "10px",
                                 "width": "88px", "display": "inline-block"}),
        html.Span(value, style={"color": color, "fontWeight": "600",
                                 "fontSize": "12px"}),
    ], style={"padding": "3px 0", "borderBottom": f"1px solid {BOR}20"})


# ── P&L chart ──────────────────────────────────────────────────────────────────

def _pnl_chart(pnl_hist: list) -> go.Figure:
    fig = go.Figure()
    if pnl_hist:
        df    = pd.DataFrame(pnl_hist)
        last  = df["pnl"].iloc[-1]
        color = G if last >= 0 else R
        fill  = "rgba(0,229,160,0.08)" if last >= 0 else "rgba(255,80,80,0.08)"
        fig.add_trace(go.Scatter(
            x=df["time"], y=df["pnl"], mode="lines",
            fill="tozeroy", fillcolor=fill,
            line=dict(color=color, width=2),
            hovertemplate="<b>%{x}</b><br>P&L: $%{y:+,.2f}<extra></extra>",
        ))
        fig.add_hline(y=0, line_dash="dot", line_color=BOR, line_width=1)

    fig.update_layout(
        title=dict(text="PORTFOLIO P&L  (USD)", font=dict(size=11, color=TM),
                   x=0, xanchor="left"),
        paper_bgcolor=C1, plot_bgcolor=C1,
        font=dict(color=TP, family=MONO, size=11),
        margin=dict(t=40, b=24, l=8, r=8),
        showlegend=False,
        xaxis=dict(showgrid=False, tickangle=-30,
                   tickfont=dict(size=9, color=TM), color=TM, linecolor=BOR),
        yaxis=dict(gridcolor=BOR, tickprefix="$",
                   tickfont=dict(size=9, color=TM), color=TM, zeroline=False),
        height=300, hovermode="x unified",
    )
    return fig


# ── Universe monitor ───────────────────────────────────────────────────────────

def _universe_monitor(u_scores, u_active, prices):
    if not u_scores:
        return html.Div("Universe populates after first 5-min refresh…",
                        style={"color": TM, "fontSize": "11px",
                               "fontStyle": "italic", "padding": "8px 0"})

    header = html.Div([
        html.Span("SYMBOL",  style=_uh("90px")),
        html.Span("PRICE",   style=_uh("80px")),
        html.Span("SCORE",   style=_uh("56px")),
        html.Span("ANN VOL", style=_uh("62px")),
        html.Span("SPREAD",  style=_uh("54px")),
        html.Span("TICKS",   style=_uh("50px")),
        html.Span("STATUS",  style=_uh("60px")),
    ], style={"display": "flex", "paddingBottom": "6px",
              "borderBottom": f"1px solid {BOR}", "marginBottom": "4px"})

    rows = []
    for s in u_scores[:14]:
        active = s.symbol in u_active
        price  = prices.get(s.symbol, 0)
        sc     = G if s.score > 0.4 else (Y if s.score > 0.1 else TM)
        rows.append(html.Div([
            html.Span(s.symbol.replace("/USD", ""),
                      style={"width": "90px", "color": TP if active else TM,
                             "fontSize": "11px",
                             "fontWeight": "600" if active else "normal"}),
            html.Span(_fmt(price) if price else "—",
                      style={"width": "80px", "color": TM, "fontSize": "11px"}),
            html.Span(f"{s.score:.3f}",
                      style={"width": "56px", "color": sc,
                             "fontSize": "11px", "fontWeight": "600"}),
            html.Span(f"{s.realized_vol:.0%}",
                      style={"width": "62px", "color": TM, "fontSize": "11px"}),
            html.Span(f"{s.spread_bps:.1f}bp",
                      style={"width": "54px", "color": TM, "fontSize": "11px"}),
            html.Span(str(s.tick_count),
                      style={"width": "50px", "color": TD, "fontSize": "10px"}),
            html.Span("● ACTIVE" if active else "○ idle",
                      style={"width": "60px",
                             "color": G if active else TD, "fontSize": "10px"}),
        ], style={"display": "flex", "alignItems": "center",
                  "padding": "5px 0", "borderBottom": f"1px solid {BOR}20"}))

    return html.Div([header] + rows)


# ── Positions table ────────────────────────────────────────────────────────────

def _positions_table(positions: list):
    if not positions:
        return html.Div("No open positions.",
                        style={"color": TM, "fontStyle": "italic", "fontSize": "12px"})
    rows = [html.Div([
        html.Span("SYMBOL",  style=_uh("90px")),
        html.Span("SIDE",    style=_uh("60px")),
        html.Span("QTY",     style=_uh("80px")),
        html.Span("ENTRY",   style=_uh("90px")),
        html.Span("CURRENT", style=_uh("90px")),
        html.Span("P&L",     style=_uh("80px")),
    ], style={"display": "flex", "paddingBottom": "6px",
              "borderBottom": f"1px solid {BOR}", "marginBottom": "4px"})]
    for pos in positions:
        pnl  = pos.get("pnl", 0)
        side = pos.get("side", "")
        sc   = G if side == "BUY" else R
        pc   = G if pnl >= 0 else R
        rows.append(html.Div([
            html.Span(pos.get("symbol", ""),
                      style={"width": "90px", "color": TP, "fontSize": "11px"}),
            html.Span("LONG" if side == "BUY" else "SHORT",
                      style={"width": "60px", "color": sc,
                             "fontSize": "11px", "fontWeight": "600"}),
            html.Span(str(pos.get("quantity", "")),
                      style={"width": "80px", "color": TM, "fontSize": "11px"}),
            html.Span(_fmt(pos.get("avg_price", 0)),
                      style={"width": "90px", "color": TM, "fontSize": "11px"}),
            html.Span(_fmt(pos.get("price", 0)),
                      style={"width": "90px", "color": TP, "fontSize": "11px"}),
            html.Span(f"${pnl:+,.2f}",
                      style={"width": "80px", "color": pc,
                             "fontWeight": "700", "fontSize": "12px"}),
        ], style={"display": "flex", "alignItems": "center",
                  "padding": "5px 0", "borderBottom": f"1px solid {BOR}20"}))
    return html.Div(rows)


# ── Signal history ─────────────────────────────────────────────────────────────

def _history_table(signals: list):
    if not signals:
        return html.Div("No signals yet.",
                        style={"color": TM, "fontStyle": "italic", "fontSize": "12px"})
    rows = [html.Div([
        html.Span("TIME",   style=_uh("70px")),
        html.Span("SYMBOL", style=_uh("90px")),
        html.Span("DIR",    style=_uh("60px")),
        html.Span("ENTRY",  style=_uh("90px")),
        html.Span("SL",     style=_uh("80px")),
        html.Span("TP",     style=_uh("80px")),
        html.Span("R:R",    style=_uh("50px")),
        html.Span("CONF",   style=_uh("50px")),
        html.Span("LEV",    style=_uh("40px")),
    ], style={"display": "flex", "paddingBottom": "6px",
              "borderBottom": f"1px solid {BOR}", "marginBottom": "4px"})]

    for sig in reversed((signals or [])[-20:]):
        side   = sig.get("side", "")
        sc     = G if side == "BUY" else R
        entry  = sig.get("entry", sig.get("price", 0))
        sl_pct = sig.get("sl_pct", 0)
        tp_pct = sig.get("tp_pct", 0)
        rr     = (tp_pct / sl_pct) if sl_pct else 0
        conf   = sig.get("confidence", 0)
        lev    = sig.get("leverage", 1)

        rows.append(html.Div([
            html.Span(sig.get("timestamp", "")[-8:],
                      style={"width": "70px", "color": TD, "fontSize": "10px"}),
            html.Span(sig.get("symbol", ""),
                      style={"width": "90px", "color": TP, "fontSize": "11px"}),
            html.Span("LONG" if side == "BUY" else "SHORT",
                      style={"width": "60px", "color": sc,
                             "fontSize": "11px", "fontWeight": "600"}),
            html.Span(_fmt(entry),
                      style={"width": "90px", "color": TM, "fontSize": "11px"}),
            html.Span(f"−{sl_pct:.1f}%",
                      style={"width": "80px", "color": R, "fontSize": "11px"}),
            html.Span(f"+{tp_pct:.1f}%",
                      style={"width": "80px", "color": G, "fontSize": "11px"}),
            html.Span(f"{rr:.1f}x",
                      style={"width": "50px",
                             "color": G if rr >= 2 else (Y if rr >= 1 else R),
                             "fontSize": "11px"}),
            html.Span(f"{conf:.0%}",
                      style={"width": "50px", "color": B, "fontSize": "11px"}),
            html.Span(f"{lev}x",
                      style={"width": "40px", "color": Y, "fontSize": "11px"}),
        ], style={"display": "flex", "alignItems": "center",
                  "padding": "4px 0", "borderBottom": f"1px solid {BOR}20"}))

    return html.Div(rows, style={"overflowX": "auto"})


# ── Helpers ────────────────────────────────────────────────────────────────────

def _section_label(mb="14px"):
    return {"color": TM, "fontSize": "10px", "letterSpacing": "1.5px",
            "fontWeight": "600", "marginBottom": mb}


def _uh(w="auto"):
    return {"width": w, "color": TD, "fontSize": "9px",
            "letterSpacing": "0.8px", "display": "inline-block"}


def _fmt(p) -> str:
    if not p:
        return "—"
    if p >= 10_000: return f"{p:,.0f}"
    if p >= 1_000:  return f"{p:,.2f}"
    if p >= 1:      return f"{p:.4f}"
    if p >= 0.001:  return f"{p:.6f}"
    return f"{p:.8f}"


def _win_rate(signals):
    closed = [s for s in signals if s.get("pnl") is not None]
    if not closed:
        return None
    return sum(1 for s in closed if s.get("pnl", 0) > 0) / len(closed)


def _safe_prices(src, lock):
    if src is None:
        return {}
    with (lock if lock else _noop()):
        return dict(src)


def _fetch_pnl(r, source, lock):
    if source is not None:
        with (lock if lock else _noop()):
            return list(source)
    if r:
        try:
            return [json.loads(x) for x in r.lrange("quant:pnl_history", 0, -1)]
        except Exception:
            pass
    return []


def _fetch_signals(r, source, lock):
    if source is not None:
        with (lock if lock else _noop()):
            return list(source)
    if r:
        try:
            return [json.loads(x) for x in reversed(r.lrange("quant:signals", 0, 49))]
        except Exception:
            pass
    return []


def _fetch_candidates(r):
    if r:
        try:
            return [json.loads(x) for x in reversed(r.lrange("quant:candidates", 0, 49))]
        except Exception:
            pass
    return []


def _fetch_positions(r, portfolio, prices_source):
    if portfolio is not None and prices_source is not None:
        return portfolio.positions_snapshot(prices_source)
    if r:
        try:
            return [json.loads(v) for v in r.hgetall("quant:positions").values()]
        except Exception:
            pass
    return []


class _noop:
    def __enter__(self): return self
    def __exit__(self, *_): pass


if __name__ == "__main__":
    start_dashboard()
