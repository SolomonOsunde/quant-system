"""
Live trading dashboard — Plotly Dash.
Primary view: trade setup cards with entry / stop loss / take profit / leverage.
Secondary: open positions, P&L curve, signal history.
"""
import json
import threading
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objects as go
import pandas as pd
import redis
from loguru import logger
import config


def start_dashboard(
    pnl_source:     list  = None,
    signals_source: list  = None,
    portfolio             = None,
    prices_source:  dict  = None,
    state_lock:     threading.Lock = None,
):
    app = dash.Dash(
        __name__,
        title="Quant Signals — Live",
        update_title=None,
        suppress_callback_exceptions=True,
    )

    try:
        r = redis.Redis(
            host=config.REDIS_HOST, port=config.REDIS_PORT,
            decode_responses=True, socket_timeout=1,
        )
        r.ping()
        redis_client = r
    except Exception:
        redis_client = None

    # ── Layout ────────────────────────────────────────────────────────────────
    app.layout = html.Div([

        # Header bar
        html.Div([
            html.Span("QUANT SIGNAL ENGINE",
                      style={"fontWeight": "bold", "fontSize": "16px",
                             "letterSpacing": "3px"}),
            html.Span(" │ PAPER MODE",
                      style={"color": "#3fb950", "fontSize": "12px",
                             "marginLeft": "10px"}),
            html.Span(id="header-clock",
                      style={"color": "#484f58", "fontSize": "12px",
                             "marginLeft": "auto"}),
        ], style={"display": "flex", "alignItems": "center",
                  "padding": "12px 24px", "borderBottom": "1px solid #21262d",
                  "background": "#010409"}),

        # Stats strip
        html.Div(id="stats-row",
                 style={"display": "flex", "gap": "10px",
                        "padding": "14px 24px 0"}),

        # ── TRADE SETUP CARDS (primary section) ───────────────────────────
        html.Div([
            html.H3("LATEST TRADE SETUPS",
                    style={"margin": "0 0 14px", "color": "#58a6ff",
                           "fontSize": "12px", "letterSpacing": "2px",
                           "fontWeight": "normal", "borderBottom": "1px solid #21262d",
                           "paddingBottom": "8px"}),
            html.Div(id="trade-cards"),
        ], style={"padding": "18px 24px 10px"}),

        # ── Charts row ────────────────────────────────────────────────────
        html.Div([
            dcc.Graph(id="pnl-chart",
                      style={"flex": 2}, config={"displayModeBar": False}),
            dcc.Graph(id="market-chart",
                      style={"flex": 1}, config={"displayModeBar": False}),
        ], style={"display": "flex", "gap": "12px", "padding": "0 24px"}),

        # ── Positions + signal history ────────────────────────────────────
        html.Div([
            html.Div([
                html.H4("OPEN POSITIONS",
                        style={"margin": "0 0 10px", "color": "#e3b341",
                               "fontSize": "11px", "letterSpacing": "1.5px",
                               "fontWeight": "normal"}),
                html.Div(id="positions-table"),
            ], style={"flex": 1, "minWidth": 0}),

            html.Div([
                html.H4("SIGNAL HISTORY",
                        style={"margin": "0 0 10px", "color": "#484f58",
                               "fontSize": "11px", "letterSpacing": "1.5px",
                               "fontWeight": "normal"}),
                html.Div(id="signals-table"),
            ], style={"flex": 2, "minWidth": 0}),
        ], style={"display": "flex", "gap": "16px",
                  "padding": "16px 24px 32px"}),

        dcc.Interval(id="tick", interval=2_000, n_intervals=0),

    ], style={"background": "#0d1117", "color": "#e6edf3",
              "minHeight": "100vh",
              "fontFamily": "'Courier New', monospace",
              "fontSize": "13px"})

    # ── Callbacks ─────────────────────────────────────────────────────────────

    @app.callback(Output("header-clock", "children"), Input("tick", "n_intervals"))
    def update_clock(_):
        import time
        return time.strftime("%Y-%m-%d  %H:%M:%S UTC")

    @app.callback(
        Output("stats-row",       "children"),
        Output("trade-cards",     "children"),
        Output("pnl-chart",       "figure"),
        Output("market-chart",    "figure"),
        Output("signals-table",   "children"),
        Output("positions-table", "children"),
        Input("tick", "n_intervals"),
    )
    def refresh(_):
        pnl_hist  = _fetch_pnl(redis_client, pnl_source, state_lock)
        signals   = _fetch_signals(redis_client, signals_source, state_lock)
        positions = _fetch_positions(redis_client, portfolio, prices_source)

        total_pnl = pnl_hist[-1]["pnl"] if pnl_hist else 0.0
        pnl_color = "#3fb950" if total_pnl >= 0 else "#f85149"
        n_buys    = sum(1 for s in signals if s.get("side") == "BUY")
        n_sells   = sum(1 for s in signals if s.get("side") == "SELL")

        return (
            _render_stats(total_pnl, pnl_color, n_buys, n_sells, len(positions)),
            _render_trade_cards(signals),
            _pnl_chart(pnl_hist),
            _market_chart(signals),
            _signals_history_table(signals),
            _positions_table(positions),
        )

    try:
        logger.info("Dashboard → http://{}:{}", config.DASHBOARD_HOST, config.DASHBOARD_PORT)
        app.run(host=config.DASHBOARD_HOST, port=config.DASHBOARD_PORT,
                debug=False, use_reloader=False)
    except Exception as e:
        logger.error("Dashboard error: {}", e)


# ── Trade setup cards ─────────────────────────────────────────────────────────

def _render_trade_cards(signals: list):
    """Show the 4 most recent BUY/SELL signals as trade setup cards."""
    actionable = [s for s in signals if s.get("side") in ("BUY", "SELL")]

    if not actionable:
        return html.Div(
            "Waiting for first signal — feeds are warming up (takes ~5 min)…",
            style={"color": "#484f58", "fontStyle": "italic",
                   "padding": "20px 0", "fontSize": "13px"},
        )

    cards = []
    for sig in reversed(actionable[-4:]):
        cards.append(_trade_card(sig))

    return html.Div(cards, style={
        "display": "grid",
        "gridTemplateColumns": "repeat(auto-fill, minmax(320px, 1fr))",
        "gap": "16px",
    })


def _trade_card(sig: dict):
    side   = sig.get("side", "BUY")
    symbol = sig.get("symbol", "")
    market = sig.get("market", "")
    ts     = sig.get("timestamp", "")

    is_long   = side == "BUY"
    direction = "LONG" if is_long else "SHORT"
    accent    = "#3fb950" if is_long else "#f85149"
    bg_accent = "rgba(63,185,80,0.07)" if is_long else "rgba(248,81,73,0.07)"

    entry  = sig.get("entry",       sig.get("price", 0))
    sl     = sig.get("stop_loss",   0)
    tp     = sig.get("take_profit", 0)
    sl_pct = sig.get("sl_pct",      0)
    tp_pct = sig.get("tp_pct",      0)
    lev    = sig.get("leverage",    1)
    conf   = sig.get("confidence",  0)
    rsi    = sig.get("rsi",         0)
    reason = sig.get("reasoning",   "")

    # Strip the "Trade setup →" appendix from reasoning for the card body
    if "Trade setup →" in reason:
        reason = reason[:reason.index("Trade setup →")].strip()

    def price_fmt(p):
        if p >= 1000:
            return f"{p:,.2f}"
        elif p >= 1:
            return f"{p:.4f}"
        else:
            return f"{p:.6f}"

    return html.Div([

        # Card header
        html.Div([
            html.Div([
                html.Span(direction, style={
                    "background": accent, "color": "#0d1117",
                    "fontWeight": "bold", "padding": "3px 10px",
                    "borderRadius": "4px", "fontSize": "13px",
                    "marginRight": "10px",
                }),
                html.Span(symbol, style={
                    "fontWeight": "bold", "fontSize": "16px",
                    "letterSpacing": "1px",
                }),
                html.Span(f"  {market}",
                          style={"color": "#484f58", "fontSize": "11px",
                                 "marginLeft": "6px"}),
            ]),
            html.Div(f"Reviewed at {ts}",
                     style={"color": "#484f58", "fontSize": "11px",
                            "marginTop": "4px"}),
        ], style={"marginBottom": "14px"}),

        # Trade levels grid
        html.Div([
            _level_row("Entry",       price_fmt(entry), "#e6edf3"),
            _level_row("Stop Loss",   f"{price_fmt(sl)}  (-{sl_pct:.2f}%)", "#f85149"),
            _level_row("Take Profit", f"{price_fmt(tp)}  (+{tp_pct:.2f}%)", "#3fb950"),
            _level_row("Leverage",    f"{lev}x", "#e3b341"),
            _level_row("Confidence",  f"{conf:.0%}", "#58a6ff"),
            _level_row("RSI",         f"{rsi:.1f}", "#8b949e"),
        ], style={"marginBottom": "12px"}),

        # Reasoning
        html.Div([
            html.Div("ANALYSIS", style={"color": "#484f58", "fontSize": "10px",
                                        "letterSpacing": "1px",
                                        "marginBottom": "4px"}),
            html.Div(reason[:220] + ("…" if len(reason) > 220 else ""),
                     style={"color": "#8b949e", "fontStyle": "italic",
                            "fontSize": "12px", "lineHeight": "1.5"}),
        ]),

    ], style={
        "background": "#161b22",
        "border": f"1px solid {accent}50",
        "borderTop": f"3px solid {accent}",
        "borderRadius": "8px",
        "padding": "16px 18px",
        "background": bg_accent if False else "#161b22",
    })


def _level_row(label: str, value: str, color: str):
    return html.Div([
        html.Span(label, style={"color": "#484f58", "fontSize": "11px",
                                "width": "100px", "display": "inline-block"}),
        html.Span(value, style={"color": color, "fontWeight": "bold",
                                "fontSize": "13px"}),
    ], style={"padding": "3px 0", "borderBottom": "1px solid #21262d10"})


# ── Stats strip ───────────────────────────────────────────────────────────────

def _render_stats(total_pnl, pnl_color, n_buys, n_sells, n_pos):
    cards = [
        ("Net P&L",      f"${total_pnl:+,.2f}", pnl_color),
        ("LONG signals", str(n_buys),             "#3fb950"),
        ("SHORT signals", str(n_sells),           "#f85149"),
        ("Open positions", str(n_pos),            "#58a6ff"),
        ("Capital",      f"${config.PAPER_INITIAL_CAPITAL_USD:,.0f}", "#484f58"),
    ]
    return html.Div([
        html.Div([
            html.Div(label, style={"color": "#8b949e", "fontSize": "11px",
                                   "letterSpacing": "0.5px"}),
            html.Div(value, style={"color": color, "fontSize": "20px",
                                   "fontWeight": "bold", "marginTop": "3px"}),
        ], style={"background": "#161b22", "border": "1px solid #21262d",
                  "borderRadius": "8px", "padding": "10px 16px",
                  "flex": "1", "minWidth": "100px"})
        for label, value, color in cards
    ], style={"display": "flex", "gap": "10px"})


# ── Charts ────────────────────────────────────────────────────────────────────

def _pnl_chart(pnl_hist: list) -> go.Figure:
    fig = go.Figure()
    if pnl_hist:
        df    = pd.DataFrame(pnl_hist)
        last  = df["pnl"].iloc[-1]
        color = "#3fb950" if last >= 0 else "#f85149"
        fill  = "rgba(63,185,80,0.10)" if last >= 0 else "rgba(248,81,73,0.10)"
        fig.add_trace(go.Scatter(
            x=df["time"], y=df["pnl"], mode="lines",
            fill="tozeroy", fillcolor=fill,
            line=dict(color=color, width=2), name="P&L",
        ))
        fig.add_hline(y=0, line_dash="dot", line_color="#484f58", line_width=1)
    _style_fig(fig, "Paper P&L (USD)", height=240)
    fig.update_layout(yaxis=dict(tickprefix="$", gridcolor="#21262d"))
    return fig


def _market_chart(signals: list) -> go.Figure:
    fig = go.Figure()
    if signals:
        df = pd.DataFrame(signals)
        if "market" in df.columns and "side" in df.columns:
            counts = df.groupby(["market", "side"]).size().reset_index(name="n")
            for side, color in [("BUY", "#3fb950"), ("SELL", "#f85149")]:
                sub = counts[counts["side"] == side]
                if not sub.empty:
                    fig.add_trace(go.Bar(
                        x=sub["market"], y=sub["n"],
                        name="LONG" if side == "BUY" else "SHORT",
                        marker_color=color,
                    ))
    _style_fig(fig, "Signals by Market", height=240)
    fig.update_layout(barmode="group")
    return fig


def _style_fig(fig, title, height=260):
    fig.update_layout(
        title=dict(text=title, font=dict(size=12)),
        paper_bgcolor="#0d1117",
        plot_bgcolor="#0d1117",
        font=dict(color="#e6edf3", family="'Courier New', monospace", size=11),
        margin=dict(t=36, b=20, l=4, r=4),
        showlegend=True,
        legend=dict(bgcolor="#0d1117", font=dict(size=11)),
        xaxis=dict(showgrid=False, tickangle=-30, tickfont=dict(size=10)),
        yaxis=dict(gridcolor="#21262d"),
        height=height,
    )


# ── Signal history table (compact) ────────────────────────────────────────────

def _signals_history_table(signals: list):
    if not signals:
        return html.P("No signals yet.",
                      style={"color": "#484f58", "fontStyle": "italic"})

    cols    = ["timestamp", "symbol", "side", "entry", "stop_loss",
               "take_profit", "leverage", "confidence"]
    headers = ["TIME", "SYMBOL", "DIR", "ENTRY", "SL", "TP", "LEV", "CONF"]

    header_row = html.Tr([html.Th(h, style=_th()) for h in headers])

    rows = []
    for sig in reversed(signals[-20:]):
        side  = sig.get("side", "")
        color = "#3fb950" if side == "BUY" else "#f85149"
        entry = sig.get("entry", sig.get("price", 0))

        def fmt(v):
            if isinstance(v, float):
                if v >= 1000: return f"{v:,.2f}"
                if v >= 1:    return f"{v:.4f}"
                return f"{v:.6f}"
            return str(v)

        cells = []
        for col in cols:
            val = sig.get(col, "")
            if col == "side":
                label = "LONG" if side == "BUY" else "SHORT"
                cells.append(html.Td(label,
                    style={**_td(), "color": color, "fontWeight": "bold"}))
            elif col == "entry":
                cells.append(html.Td(fmt(entry), style=_td()))
            elif col == "stop_loss":
                cells.append(html.Td(fmt(val),
                    style={**_td(), "color": "#f85149"}))
            elif col == "take_profit":
                cells.append(html.Td(fmt(val),
                    style={**_td(), "color": "#3fb950"}))
            elif col == "leverage":
                cells.append(html.Td(f"{val}x" if val else "—",
                    style={**_td(), "color": "#e3b341"}))
            elif col == "confidence":
                cells.append(html.Td(
                    f"{val:.0%}" if isinstance(val, float) else str(val),
                    style={**_td(), "color": "#58a6ff"}))
            else:
                cells.append(html.Td(str(val), style=_td()))

        rows.append(html.Tr(cells, style={"borderBottom": "1px solid #0d1117"}))

    return html.Table(
        [html.Thead(header_row), html.Tbody(rows)],
        style={"width": "100%", "borderCollapse": "collapse",
               "background": "#161b22", "borderRadius": "8px",
               "overflow": "hidden"},
    )


# ── Positions table ───────────────────────────────────────────────────────────

def _positions_table(positions: list):
    if not positions:
        return html.P("No open positions.",
                      style={"color": "#484f58", "fontStyle": "italic"})

    cols    = ["symbol", "side", "quantity", "avg_price", "price", "pnl"]
    headers = ["SYMBOL", "SIDE", "QTY", "ENTRY", "CURRENT", "P&L"]

    header_row = html.Tr([html.Th(h, style=_th()) for h in headers])
    rows = []
    for pos in positions:
        pnl  = pos.get("pnl", 0)
        side = pos.get("side", "")
        cells = []
        for col in cols:
            val = pos.get(col, "")
            if col == "side":
                label = "LONG" if side == "BUY" else "SHORT"
                color = "#3fb950" if side == "BUY" else "#f85149"
                cells.append(html.Td(label,
                    style={**_td(), "color": color, "fontWeight": "bold"}))
            elif col == "pnl":
                c = "#3fb950" if pnl >= 0 else "#f85149"
                cells.append(html.Td(f"${pnl:+,.2f}",
                    style={**_td(), "color": c, "fontWeight": "bold"}))
            else:
                cells.append(html.Td(str(val), style=_td()))
        rows.append(html.Tr(cells, style={"borderBottom": "1px solid #0d1117"}))

    return html.Table(
        [html.Thead(header_row), html.Tbody(rows)],
        style={"width": "100%", "borderCollapse": "collapse",
               "background": "#161b22", "borderRadius": "8px",
               "overflow": "hidden"},
    )


def _th():
    return {"padding": "7px 10px", "color": "#8b949e", "fontSize": "10px",
            "fontWeight": "normal", "letterSpacing": "0.8px",
            "textAlign": "left", "borderBottom": "1px solid #21262d",
            "whiteSpace": "nowrap"}


def _td():
    return {"padding": "6px 10px", "color": "#e6edf3", "whiteSpace": "nowrap"}


# ── Data fetchers ──────────────────────────────────────────────────────────────

def _fetch_pnl(r, source, lock):
    if source is not None:
        with lock if lock else _noop():
            return list(source)
    if r:
        try:
            return [json.loads(x) for x in r.lrange("quant:pnl_history", 0, -1)]
        except Exception:
            pass
    return []


def _fetch_signals(r, source, lock):
    if source is not None:
        with lock if lock else _noop():
            return list(source)
    if r:
        try:
            raw = r.lrange("quant:signals", 0, 49)
            return [json.loads(x) for x in reversed(raw)]
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
