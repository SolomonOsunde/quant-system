import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import redis
import json
from loguru import logger
import config


def start_dashboard(pnl_source: list = None):
    """Launch the live P&L and signals monitoring dashboard."""
    app = dash.Dash(
        __name__,
        title="Polyglot Quant — Live Dashboard",
        update_title=None,
    )

    try:
        r = redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT,
                        decode_responses=True)
        r.ping()
        redis_client = r
    except Exception:
        logger.warning("Redis not available — dashboard running without live state.")
        redis_client = None

    # ---- Layout ----
    app.layout = html.Div([
        html.H2("Polyglot Quant System — Live Dashboard",
                style={"fontFamily": "monospace", "padding": "16px 24px",
                       "marginBottom": 0, "borderBottom": "1px solid #333"}),

        # Top stats row
        html.Div(id="stats-row", style={
            "display": "flex", "gap": "16px", "padding": "16px 24px",
        }),

        # Charts row
        html.Div([
            dcc.Graph(id="pnl-chart",
                      style={"flex": 1},
                      config={"displayModeBar": False}),
            dcc.Graph(id="signals-chart",
                      style={"flex": 1},
                      config={"displayModeBar": False}),
        ], style={"display": "flex", "gap": "16px", "padding": "0 24px"}),

        # Recent signals table
        html.Div([
            html.H4("Recent Signals", style={"fontFamily": "monospace"}),
            html.Div(id="signals-table"),
        ], style={"padding": "16px 24px"}),

        # Active positions table
        html.Div([
            html.H4("Active Positions", style={"fontFamily": "monospace"}),
            html.Div(id="positions-table"),
        ], style={"padding": "0 24px 24px"}),

        dcc.Interval(id="interval", interval=2_000, n_intervals=0),
    ], style={"background": "#0d1117", "color": "#e6edf3",
              "minHeight": "100vh", "fontFamily": "monospace"})

    # ---- Callbacks ----
    @app.callback(
        Output("stats-row", "children"),
        Output("pnl-chart", "figure"),
        Output("signals-chart", "figure"),
        Output("signals-table", "children"),
        Output("positions-table", "children"),
        Input("interval", "n_intervals"),
    )
    def update_dashboard(n):
        # Load state from Redis or mock data
        pnl_history  = _get_pnl_history(redis_client, pnl_source)
        signals      = _get_recent_signals(redis_client)
        positions    = _get_positions(redis_client)

        # Stats cards
        total_pnl   = pnl_history[-1]["pnl"] if pnl_history else 0.0
        n_signals   = len(signals)
        win_rate    = _compute_win_rate(signals)
        pnl_color   = "#3fb950" if total_pnl >= 0 else "#f85149"

        stats = html.Div([
            _stat_card("Daily P&L", f"${total_pnl:+,.2f}", pnl_color),
            _stat_card("Signals Today", str(n_signals), "#58a6ff"),
            _stat_card("Win Rate", f"{win_rate:.1f}%", "#58a6ff"),
            _stat_card("Open Positions", str(len(positions)), "#e3b341"),
        ], style={"display": "flex", "gap": "16px"})

        # P&L chart
        pnl_fig = go.Figure()
        if pnl_history:
            df = pd.DataFrame(pnl_history)
            pnl_fig.add_trace(go.Scatter(
                x=df.get("time", list(range(len(df)))),
                y=df["pnl"],
                mode="lines",
                fill="tozeroy",
                line=dict(color="#3fb950", width=2),
                fillcolor="rgba(63,185,80,0.1)",
                name="P&L"
            ))
        pnl_fig.update_layout(
            title="Cumulative P&L",
            paper_bgcolor="#0d1117",
            plot_bgcolor="#0d1117",
            font=dict(color="#e6edf3", family="monospace"),
            margin=dict(t=40, b=20, l=10, r=10),
            showlegend=False,
            xaxis=dict(showgrid=False),
            yaxis=dict(gridcolor="#21262d"),
        )

        # Signals by market chart
        sig_fig = go.Figure()
        if signals:
            df_s = pd.DataFrame(signals)
            if "market" in df_s.columns:
                counts = df_s.groupby(["market", "side"]).size().reset_index(name="count")
                for side, color in [("BUY", "#3fb950"), ("SELL", "#f85149")]:
                    sub = counts[counts["side"] == side]
                    sig_fig.add_trace(go.Bar(
                        x=sub["market"], y=sub["count"],
                        name=side, marker_color=color,
                    ))
        sig_fig.update_layout(
            title="Signals by Market",
            paper_bgcolor="#0d1117",
            plot_bgcolor="#0d1117",
            font=dict(color="#e6edf3", family="monospace"),
            margin=dict(t=40, b=20, l=10, r=10),
            barmode="group",
            xaxis=dict(showgrid=False),
            yaxis=dict(gridcolor="#21262d"),
        )

        # Signals table
        sig_table = _make_table(
            signals[-20:] if signals else [],
            ["timestamp", "symbol", "side", "confidence", "strategy", "reasoning"]
        )

        # Positions table
        pos_table = _make_table(
            positions,
            ["symbol", "side", "quantity", "price", "pnl"]
        )

        return stats, pnl_fig, sig_fig, sig_table, pos_table

    try:
        logger.info("Dashboard starting at http://{}:{}", config.DASHBOARD_HOST, config.DASHBOARD_PORT)
        app.run(host=config.DASHBOARD_HOST, port=config.DASHBOARD_PORT, debug=False)
    except Exception as e:
        logger.error("Dashboard error: {}", e)


# ---- Helpers ----

def _stat_card(label, value, color):
    return html.Div([
        html.Div(label, style={"color": "#8b949e", "fontSize": "12px"}),
        html.Div(value, style={"color": color, "fontSize": "24px",
                               "fontWeight": "bold", "marginTop": "4px"}),
    ], style={"background": "#161b22", "border": "1px solid #21262d",
              "borderRadius": "8px", "padding": "16px 20px", "minWidth": "140px"})


def _make_table(data: list, columns: list):
    if not data:
        return html.P("No data yet.", style={"color": "#8b949e"})
    rows = []
    for item in reversed(data[-20:]):
        row = [html.Td(str(item.get(col, ""))[:40],
                       style={"padding": "6px 12px", "borderBottom": "1px solid #21262d",
                              "color": _cell_color(col, item)})
               for col in columns]
        rows.append(html.Tr(row))
    header = html.Tr([html.Th(c.upper(), style={"padding": "8px 12px",
                                                  "color": "#8b949e",
                                                  "textAlign": "left"})
                      for c in columns])
    return html.Table([html.Thead(header), html.Tbody(rows)],
                      style={"width": "100%", "borderCollapse": "collapse",
                             "background": "#161b22", "borderRadius": "8px"})


def _cell_color(col, item):
    if col == "side":
        return "#3fb950" if item.get("side") == "BUY" else "#f85149"
    if col == "pnl":
        pnl = item.get("pnl", 0)
        return "#3fb950" if pnl >= 0 else "#f85149"
    return "#e6edf3"


def _get_pnl_history(r, source):
    if source:
        return source
    if r:
        try:
            raw = r.lrange("quant:pnl_history", 0, -1)
            return [json.loads(x) for x in raw]
        except Exception:
            pass
    # Mock data for UI demo
    import random
    pnl = 0
    history = []
    for i in range(100):
        pnl += random.gauss(10, 80)
        history.append({"time": i, "pnl": round(pnl, 2)})
    return history


def _get_recent_signals(r):
    if r:
        try:
            raw = r.lrange("quant:signals", 0, 49)
            return [json.loads(x) for x in raw]
        except Exception:
            pass
    return []


def _get_positions(r):
    if r:
        try:
            raw = r.hgetall("quant:positions")
            return [json.loads(v) for v in raw.values()]
        except Exception:
            pass
    return []


def _compute_win_rate(signals):
    wins = sum(1 for s in signals if s.get("pnl", 0) > 0)
    return (wins / len(signals) * 100) if signals else 0.0


if __name__ == "__main__":
    start_dashboard()
