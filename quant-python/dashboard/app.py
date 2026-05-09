"""
Live trading dashboard — Plotly Dash.
Shows paper P&L, open positions, and Claude AI signal reasoning.
Data flows from the QuantPythonLayer via shared in-process lists
and optionally Redis for cross-process access.
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
    """Launch the live P&L and signals dashboard."""
    app = dash.Dash(
        __name__,
        title="Polyglot Quant — Live",
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

        # Header
        html.Div([
            html.Span("POLYGLOT QUANT SYSTEM",
                      style={"fontWeight": "bold", "fontSize": "18px",
                             "letterSpacing": "2px"}),
            html.Span(" │ PAPER TRADING",
                      style={"color": "#3fb950", "fontSize": "13px",
                             "marginLeft": "8px"}),
            html.Span(id="header-clock",
                      style={"color": "#8b949e", "fontSize": "12px",
                             "marginLeft": "auto"}),
        ], style={"display": "flex", "alignItems": "center",
                  "padding": "14px 24px", "borderBottom": "1px solid #21262d",
                  "background": "#010409"}),

        # Latest signal recommendation banner
        html.Div(id="signal-banner", style={"padding": "10px 24px"}),

        # Stats row
        html.Div(id="stats-row",
                 style={"display": "flex", "gap": "12px",
                        "padding": "0 24px 12px"}),

        # Charts row
        html.Div([
            dcc.Graph(id="pnl-chart",     style={"flex": 2},
                      config={"displayModeBar": False}),
            dcc.Graph(id="market-chart",  style={"flex": 1},
                      config={"displayModeBar": False}),
        ], style={"display": "flex", "gap": "12px", "padding": "0 24px"}),

        # Positions + signals
        html.Div([
            html.Div([
                html.H4("Open Positions",
                        style={"margin": "0 0 8px", "color": "#e3b341",
                               "fontSize": "13px", "letterSpacing": "1px"}),
                html.Div(id="positions-table"),
            ], style={"flex": 1, "minWidth": 0}),

            html.Div([
                html.H4("Trade Signals & Claude Reasoning",
                        style={"margin": "0 0 8px", "color": "#58a6ff",
                               "fontSize": "13px", "letterSpacing": "1px"}),
                html.Div(id="signals-table"),
            ], style={"flex": 2, "minWidth": 0}),
        ], style={"display": "flex", "gap": "16px",
                  "padding": "16px 24px 24px"}),

        dcc.Interval(id="tick", interval=2_000, n_intervals=0),

    ], style={"background": "#0d1117", "color": "#e6edf3",
              "minHeight": "100vh", "fontFamily": "'Courier New', monospace",
              "fontSize": "13px"})

    # ── Callbacks ─────────────────────────────────────────────────────────────

    @app.callback(Output("header-clock", "children"), Input("tick", "n_intervals"))
    def update_clock(_):
        import time
        return time.strftime("%Y-%m-%d  %H:%M:%S")

    @app.callback(
        Output("signal-banner",   "children"),
        Output("stats-row",       "children"),
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

        n_buys  = sum(1 for s in signals if s.get("side") == "BUY")
        n_sells = sum(1 for s in signals if s.get("side") == "SELL")
        avg_conf = (
            sum(s.get("confidence", 0) for s in signals) / len(signals)
            if signals else 0
        )

        banner    = _render_banner(signals[-1] if signals else None)
        stats     = _render_stats(total_pnl, pnl_color, n_buys, n_sells,
                                  avg_conf, len(positions))
        pnl_fig   = _pnl_chart(pnl_hist)
        mkt_fig   = _market_chart(signals)
        sig_tbl   = _signals_table(signals)
        pos_tbl   = _positions_table(positions)

        return banner, stats, pnl_fig, mkt_fig, sig_tbl, pos_tbl

    try:
        logger.info("Dashboard → http://{}:{}", config.DASHBOARD_HOST, config.DASHBOARD_PORT)
        app.run(host=config.DASHBOARD_HOST, port=config.DASHBOARD_PORT,
                debug=False, use_reloader=False)
    except Exception as e:
        logger.error("Dashboard error: {}", e)


# ── Renderers ─────────────────────────────────────────────────────────────────

def _render_banner(sig: dict | None):
    if not sig:
        return html.Div(
            "Waiting for first signal — market data is streaming…",
            style={"color": "#8b949e", "padding": "6px 0",
                   "fontStyle": "italic", "fontSize": "12px"},
        )

    side  = sig.get("side", "HOLD")
    color = "#3fb950" if side == "BUY" else "#f85149" if side == "SELL" else "#e3b341"
    conf  = sig.get("confidence", 0)
    flags = sig.get("risk_flags", "")

    return html.Div([
        html.Div([
            html.Span(f" {side} ", style={
                "background": color, "color": "#0d1117",
                "fontWeight": "bold", "padding": "2px 10px",
                "borderRadius": "4px", "marginRight": "10px",
                "fontSize": "14px",
            }),
            html.Span(sig.get("symbol", ""), style={
                "fontWeight": "bold", "fontSize": "15px", "marginRight": "8px",
            }),
            html.Span(f"@ {sig.get('price', 0):.5f}",
                      style={"color": "#8b949e", "marginRight": "16px"}),
            html.Span(f"CONF {conf:.0%}",
                      style={"color": "#e3b341", "marginRight": "16px",
                             "fontWeight": "bold"}),
            html.Span(f"RSI {sig.get('rsi', 0):.1f}  "
                      f"MACD {sig.get('macd_hist', 0):.5f}  "
                      f"ML {sig.get('ml_dir', 0):+.3f}",
                      style={"color": "#58a6ff", "fontSize": "12px",
                             "marginRight": "16px"}),
            html.Span(sig.get("timestamp", ""),
                      style={"color": "#484f58", "fontSize": "11px"}),
        ], style={"display": "flex", "alignItems": "center",
                  "flexWrap": "wrap", "gap": "4px"}),

        html.Div(f"Claude: {sig.get('reasoning', '')[:160]}",
                 style={"color": "#8b949e", "fontStyle": "italic",
                        "marginTop": "4px", "fontSize": "12px"}),

        html.Div(f"⚠ {flags}" if flags else "",
                 style={"color": "#e3b341", "fontSize": "11px",
                        "marginTop": "2px"}),
    ], style={
        "background": "#161b22",
        "border":     f"1px solid {color}40",
        "borderLeft": f"4px solid {color}",
        "borderRadius": "6px",
        "padding": "10px 16px",
    })


def _render_stats(total_pnl, pnl_color, n_buys, n_sells, avg_conf, n_pos):
    cards = [
        ("Net P&L",     f"${total_pnl:+,.2f}",  pnl_color),
        ("BUY signals", str(n_buys),              "#3fb950"),
        ("SELL signals", str(n_sells),            "#f85149"),
        ("Avg Conf",    f"{avg_conf:.1%}",        "#e3b341"),
        ("Positions",   str(n_pos),               "#58a6ff"),
        ("Capital",     f"${config.PAPER_INITIAL_CAPITAL_USD:,.0f}", "#484f58"),
    ]
    return html.Div([
        html.Div([
            html.Div(label, style={"color": "#8b949e", "fontSize": "11px",
                                   "letterSpacing": "0.5px"}),
            html.Div(value, style={"color": color, "fontSize": "22px",
                                   "fontWeight": "bold", "marginTop": "2px"}),
        ], style={"background": "#161b22", "border": "1px solid #21262d",
                  "borderRadius": "8px", "padding": "12px 16px",
                  "minWidth": "110px", "flex": "1"})
        for label, value, color in cards
    ], style={"display": "flex", "gap": "12px"})


def _pnl_chart(pnl_hist: list) -> go.Figure:
    fig = go.Figure()
    if pnl_hist:
        df = pd.DataFrame(pnl_hist)
        last_pnl = df["pnl"].iloc[-1]
        color = "#3fb950" if last_pnl >= 0 else "#f85149"
        fill  = "rgba(63,185,80,0.12)" if last_pnl >= 0 else "rgba(248,81,73,0.12)"

        fig.add_trace(go.Scatter(
            x=df["time"], y=df["pnl"],
            mode="lines",
            fill="tozeroy",
            fillcolor=fill,
            line=dict(color=color, width=2),
            name="P&L",
        ))
        fig.add_hline(y=0, line_dash="dot",
                      line_color="#484f58", line_width=1)
        # Mark each trade signal on the P&L curve would be nice but complex —
        # keeping it simple here.

    _style_fig(fig, "Cumulative Paper P&L (USD)", height=270)
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
                        name=side, marker_color=color,
                    ))
    _style_fig(fig, "Signals by Market", height=270)
    fig.update_layout(barmode="group")
    return fig


def _style_fig(fig, title, height=300):
    fig.update_layout(
        title=dict(text=title, font=dict(size=13)),
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


def _signals_table(signals: list):
    if not signals:
        return html.P(
            "No signals yet — feeds are warming up…",
            style={"color": "#8b949e", "fontStyle": "italic", "padding": "8px 0"},
        )

    cols    = ["timestamp", "symbol", "market", "side", "price",
               "confidence", "rsi", "ml_dir", "reasoning"]
    headers = ["TIME", "SYMBOL", "MKT", "SIDE", "PRICE",
               "CONF", "RSI", "ML", "CLAUDE REASONING"]

    header_row = html.Tr([
        html.Th(h, style=_th_style()) for h in headers
    ])

    rows = []
    for sig in reversed(signals[-25:]):
        side  = sig.get("side", "")
        color = "#3fb950" if side == "BUY" else "#f85149"

        cells = []
        for col in cols:
            val = sig.get(col, "")
            if col == "side":
                style = {**_td_style(), "color": color, "fontWeight": "bold"}
            elif col == "confidence":
                style = {**_td_style(), "color": "#e3b341"}
                val   = f"{val:.0%}" if isinstance(val, float) else val
            elif col == "ml_dir":
                style = {**_td_style(), "color": "#58a6ff"}
                val   = f"{val:+.3f}" if isinstance(val, float) else val
            elif col == "reasoning":
                style = {**_td_style(), "color": "#8b949e", "fontStyle": "italic",
                         "maxWidth": "340px", "overflow": "hidden",
                         "whiteSpace": "nowrap", "textOverflow": "ellipsis"}
                val   = str(val)[:120]
            else:
                style = _td_style()
            cells.append(html.Td(str(val), style=style))

        rows.append(html.Tr(cells,
                            style={"borderBottom": "1px solid #0d1117"}))

    return html.Table(
        [html.Thead(header_row), html.Tbody(rows)],
        style={"width": "100%", "borderCollapse": "collapse",
               "background": "#161b22", "borderRadius": "8px",
               "overflow": "hidden"},
    )


def _positions_table(positions: list):
    if not positions:
        return html.P(
            "No open positions.",
            style={"color": "#8b949e", "fontStyle": "italic", "padding": "8px 0"},
        )

    cols    = ["symbol", "side", "quantity", "avg_price", "price", "pnl", "market"]
    headers = ["SYMBOL", "SIDE", "QTY", "ENTRY", "CURRENT", "P&L", "MKT"]

    header_row = html.Tr([html.Th(h, style=_th_style()) for h in headers])

    rows = []
    for pos in positions:
        pnl  = pos.get("pnl", 0)
        side = pos.get("side", "")
        side_color = "#3fb950" if side == "BUY" else "#f85149"
        pnl_color  = "#3fb950" if pnl >= 0 else "#f85149"

        cells = []
        for col in cols:
            val = pos.get(col, "")
            if col == "side":
                style = {**_td_style(), "color": side_color, "fontWeight": "bold"}
            elif col == "pnl":
                style = {**_td_style(), "color": pnl_color, "fontWeight": "bold"}
                val   = f"${pnl:+,.2f}"
            else:
                style = _td_style()
            cells.append(html.Td(str(val), style=style))

        rows.append(html.Tr(cells, style={"borderBottom": "1px solid #0d1117"}))

    return html.Table(
        [html.Thead(header_row), html.Tbody(rows)],
        style={"width": "100%", "borderCollapse": "collapse",
               "background": "#161b22", "borderRadius": "8px",
               "overflow": "hidden"},
    )


def _th_style():
    return {"padding": "7px 10px", "color": "#8b949e", "fontSize": "11px",
            "fontWeight": "normal", "letterSpacing": "0.5px",
            "textAlign": "left", "borderBottom": "1px solid #21262d",
            "whiteSpace": "nowrap"}


def _td_style():
    return {"padding": "6px 10px", "color": "#e6edf3", "whiteSpace": "nowrap"}


# ── Data fetchers ──────────────────────────────────────────────────────────────

def _fetch_pnl(r, source, lock):
    if source is not None:
        if lock:
            with lock:
                return list(source)
        return list(source)
    if r:
        try:
            raw = r.lrange("quant:pnl_history", 0, -1)
            return [json.loads(x) for x in raw]
        except Exception:
            pass
    return []


def _fetch_signals(r, source, lock):
    if source is not None:
        if lock:
            with lock:
                return list(source)
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
            raw = r.hgetall("quant:positions")
            return [json.loads(v) for v in raw.values()]
        except Exception:
            pass
    return []


if __name__ == "__main__":
    start_dashboard()
