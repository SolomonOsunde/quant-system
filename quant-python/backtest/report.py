"""
Statistical analysis of backtest results.

Sections:
  1. Overall summary (5-bar hold baseline)
  2. Multi-horizon comparison: 5-bar vs 15-bar vs 30-bar hold
  3. Hit rate by composite threshold
  4. Trend alignment: with-trend vs counter-trend performance
  5. Regime breakdown
  6. Momentum signal contribution
  7. Per-symbol breakdown
"""
import numpy as np
import pandas as pd
from pathlib import Path

OUTPUT_DIR = Path(__file__).parent / "results"
OUTPUT_DIR.mkdir(exist_ok=True)


def _expectancy(sub: pd.DataFrame, col: str = "pnl") -> float:
    wins   = sub.loc[sub[col] > 0, col]
    losses = sub.loc[sub[col] < 0, col]   # strict <0; excludes zero-PnL
    w_rate = (sub[col] > 0).mean()
    l_rate = (sub[col] < 0).mean()
    avg_w  = wins.mean()   if len(wins)   > 0 else 0.0
    avg_l  = losses.mean() if len(losses) > 0 else 0.0
    return round(w_rate * avg_w + l_rate * avg_l, 4)


def _horizon_stats(df: pd.DataFrame, horizon: int, spread_bps: float = 3.0) -> dict:
    """
    Compute hit/win/expectancy for a given hold horizon (5, 15, or 30 bars).

    P&L = same Kelly sizing as the 5-bar trade, but exit at the horizon's close
    price (direction-adjusted, spread-adjusted). No ratio-scaling — computes
    the actual gross return for that hold period and deducts spread cost.
    """
    ret_col = f"return_{horizon}b"
    hit_col = f"hit_{horizon}b"
    if ret_col not in df.columns or hit_col not in df.columns:
        return {}

    spread_half_frac = (spread_bps / 2) / 10_000
    sign = df["side"].map({"BUY": 1, "SELL": -1})

    # Gross P&L using the same position size (sizing.recommended_usd ≈ pnl/5b_gross_per_unit)
    # Reconstruct quantity from 5b pnl and 5b return where possible, else use entry_price proxy
    entry = df["entry_price"]
    raw_ret_Nb = df[ret_col]

    # Net return for this horizon: direction * raw_return - 2 * spread (entry + exit)
    net_ret_Nb = sign * raw_ret_Nb - 2 * spread_half_frac

    # Use same notional as 5b (approximated via pnl / (sign * return_5b - 2*spread))
    # Fall back to a fixed $1000 notional if 5b return is near zero
    ret5_net = sign * df["return_5b"] - 2 * spread_half_frac
    notional = (df["pnl"] / ret5_net.replace(0, np.nan)).fillna(1000.0).clip(10, 10_000)
    pnl_Nb = notional * net_ret_Nb

    hit_rate = df[hit_col].mean()
    win_rate = (pnl_Nb > 0).mean()
    avg_pnl  = pnl_Nb.mean()
    exp_val  = _expectancy(pd.DataFrame({"pnl": pnl_Nb}))

    return {
        "horizon_bars": horizon,
        "hit_rate":     round(hit_rate, 4),
        "win_rate":     round(win_rate, 4),
        "avg_pnl":      round(avg_pnl, 4),
        "expectancy":   exp_val,
        "total_pnl":    round(pnl_Nb.sum(), 2),
    }


def generate(df: pd.DataFrame, capital: float = 100_000.0) -> dict:
    if df.empty:
        print("No data to analyze.")
        return {}

    df = df.copy()
    df["entry_time"]    = pd.to_datetime(df["entry_time"])
    df["abs_composite"] = df["composite"].abs()

    stats = {}

    # ── 1. Overall summary (path-dependent PnL; hit_* still multi-horizon) ───
    stats["total_signals"]    = len(df)
    stats["buy_signals"]      = (df["side"] == "BUY").sum()
    stats["sell_signals"]     = (df["side"] == "SELL").sum()
    stats["overall_hit_rate"] = df["hit_5b"].mean()
    stats["win_rate"]         = (df["pnl"] > 0).mean()
    stats["avg_win"]          = df.loc[df["pnl"] > 0, "pnl"].mean() if (df["pnl"] > 0).any() else 0.0
    stats["avg_loss"]         = df.loc[df["pnl"] < 0, "pnl"].mean() if (df["pnl"] < 0).any() else 0.0
    loss_rate                 = (df["pnl"] < 0).mean()
    stats["payoff_ratio"]     = round(abs(stats["avg_win"] / stats["avg_loss"]), 3) if stats["avg_loss"] != 0 else 0.0
    stats["expectancy_usd"]   = round(stats["win_rate"] * stats["avg_win"] + loss_rate * stats["avg_loss"], 4)
    stats["total_pnl"]        = df["pnl"].sum()

    df_s = df.sort_values("entry_time").reset_index(drop=True)
    df_s["cum_pnl"]  = df_s["pnl"].cumsum()
    df_s["equity"]   = capital + df_s["cum_pnl"]
    df_s["peak"]     = df_s["equity"].cummax()
    df_s["drawdown"] = (df_s["equity"] - df_s["peak"]) / df_s["peak"]

    stats["final_equity"]     = round(df_s["equity"].iloc[-1], 2)
    stats["max_drawdown_pct"] = round(df_s["drawdown"].min() * 100, 2)
    stats["return_pct"]       = round((stats["final_equity"] - capital) / capital * 100, 2)

    daily = df_s.set_index("entry_time")["pnl"].resample("1D").sum()
    daily_pct = daily / capital
    stats["sharpe"] = round(daily_pct.mean() / daily_pct.std() * np.sqrt(252), 3) if daily_pct.std() > 0 else 0.0

    # ── 2. Multi-horizon comparison ───────────────────────────────────────────
    stats["horizon_analysis"] = [_horizon_stats(df, h) for h in [5, 15, 30]]

    # ── 3. Hit rate by composite threshold ───────────────────────────────────
    thresholds = [0.20, 0.25, 0.30, 0.35, 0.40, 0.50]
    threshold_stats = []
    for t in thresholds:
        sub = df[df["abs_composite"] >= t]
        if len(sub) < 10:
            continue
        wins   = sub.loc[sub["pnl"] > 0, "pnl"]
        losses = sub.loc[sub["pnl"] < 0, "pnl"]
        w_rate = (sub["pnl"] > 0).mean()
        l_rate = (sub["pnl"] < 0).mean()
        exp    = round(w_rate * (wins.mean() if len(wins) > 0 else 0.0)
                       + l_rate * (losses.mean() if len(losses) > 0 else 0.0), 4)
        threshold_stats.append({
            "composite_min": t,
            "n_signals":     len(sub),
            "hit_rate":      round(sub["hit_5b"].mean(), 4),
            "hit_15b":       round(sub["hit_15b"].mean(), 4) if "hit_15b" in sub else 0,
            "hit_30b":       round(sub["hit_30b"].mean(), 4) if "hit_30b" in sub else 0,
            "win_rate":      round(w_rate, 4),
            "avg_pnl":       round(sub["pnl"].mean(), 4),
            "expectancy":    exp,
        })
    stats["threshold_analysis"] = threshold_stats

    # ── 4. Trend alignment analysis ───────────────────────────────────────────
    trend_stats = {}
    for t_label in ["UP", "DOWN", "FLAT"]:
        sub = df[df["trend"] == t_label]
        if len(sub) < 5:
            continue
        buy_sub  = sub[sub["side"] == "BUY"]
        sell_sub = sub[sub["side"] == "SELL"]
        with_trend = (
            (buy_sub if t_label == "UP" else sell_sub if t_label == "DOWN" else pd.DataFrame())
        )
        trend_stats[t_label] = {
            "n":             len(sub),
            "buy_pct":       round(len(buy_sub) / len(sub), 3),
            "sell_pct":      round(len(sell_sub) / len(sub), 3),
            "hit_rate_5b":   round(sub["hit_5b"].mean(), 4),
            "hit_rate_15b":  round(sub["hit_15b"].mean(), 4) if "hit_15b" in sub else 0,
            "hit_rate_30b":  round(sub["hit_30b"].mean(), 4) if "hit_30b" in sub else 0,
            "avg_pnl":       round(sub["pnl"].mean(), 4),
            "expectancy":    _expectancy(sub),
        }
        if len(with_trend) > 5:
            trend_stats[t_label]["with_trend_hit_5b"]  = round(with_trend["hit_5b"].mean(), 4)
            trend_stats[t_label]["with_trend_hit_15b"] = round(with_trend["hit_15b"].mean(), 4) if "hit_15b" in with_trend else 0
    stats["trend_analysis"] = trend_stats

    # ── 5. Sell bias breakdown ────────────────────────────────────────────────
    buy_df  = df[df["side"] == "BUY"]
    sell_df = df[df["side"] == "SELL"]
    stats["side_analysis"] = {
        "buy":  {"n": len(buy_df),  "hit_5b": round(buy_df["hit_5b"].mean(), 4)  if len(buy_df)  > 0 else 0,
                 "avg_pnl": round(buy_df["pnl"].mean(), 4)  if len(buy_df)  > 0 else 0},
        "sell": {"n": len(sell_df), "hit_5b": round(sell_df["hit_5b"].mean(), 4) if len(sell_df) > 0 else 0,
                 "avg_pnl": round(sell_df["pnl"].mean(), 4) if len(sell_df) > 0 else 0},
    }

    # ── 6. Momentum signal contribution ──────────────────────────────────────
    has_mom = df[df["mom_score"] != 0.0]
    no_mom  = df[df["mom_score"] == 0.0]
    stats["momentum_analysis"] = {
        "pct_with_momentum": round(len(has_mom) / len(df), 3),
        "hit_5b_with_mom":   round(has_mom["hit_5b"].mean(), 4) if len(has_mom) > 0 else 0,
        "hit_5b_no_mom":     round(no_mom["hit_5b"].mean(), 4)  if len(no_mom)  > 0 else 0,
        "avg_mom_score":     round(df["mom_score"].abs().mean(), 4),
    }

    # ── 7. Regime breakdown ───────────────────────────────────────────────────
    regime_stats = {}
    for regime in df["regime"].unique():
        sub = df[df["regime"] == regime]
        regime_stats[regime] = {
            "n":       len(sub),
            "buy_pct": round((sub["side"] == "BUY").mean(), 3),
            "hit_5b":  round(sub["hit_5b"].mean(), 4),
            "hit_15b": round(sub["hit_15b"].mean(), 4) if "hit_15b" in sub else 0,
            "avg_pnl": round(sub["pnl"].mean(), 4),
        }
    stats["regime_analysis"] = regime_stats

    # ── 8. Exit reason breakdown (path-dependent SL/TP/time) ─────────────────
    if "exit_reason" in df.columns:
        stats["exit_reasons"] = df["exit_reason"].value_counts().to_dict()
    else:
        stats["exit_reasons"] = {}

    # ── 9. Per-symbol breakdown ───────────────────────────────────────────────
    sym_stats = (df.groupby("symbol")
                 .agg(n=("pnl","count"),
                      hit_5b=("hit_5b","mean"),
                      hit_15b=("hit_15b","mean"),
                      hit_30b=("hit_30b","mean"),
                      win_rate=("pnl", lambda x: (x > 0).mean()),
                      total_pnl=("pnl","sum"),
                      avg_composite=("abs_composite","mean"),
                      sell_bias=("side", lambda x: (x == "SELL").mean()))
                 .round(4).sort_values("total_pnl", ascending=False))
    stats["per_symbol"] = sym_stats.to_dict("index")

    # ── Save outputs ──────────────────────────────────────────────────────────
    df_s.to_csv(OUTPUT_DIR / "trades.csv", index=False)
    sym_stats.to_csv(OUTPUT_DIR / "by_symbol.csv")
    df_s[["entry_time","equity","drawdown","cum_pnl"]].to_csv(OUTPUT_DIR / "equity_curve.csv", index=False)

    _write_text_report(stats, OUTPUT_DIR / "report.txt")
    return stats


def _write_text_report(stats: dict, path: Path):
    W = 66
    lines = [
        "=" * W, "  BACKTEST REPORT", "=" * W, "",
        f"  Total signals       : {stats['total_signals']}",
        f"  BUY / SELL          : {stats['buy_signals']} / {stats['sell_signals']}"
        f"  ({stats['sell_signals']/max(stats['total_signals'],1):.0%} SELL bias)",
        f"  Overall hit rate    : {stats['overall_hit_rate']:.1%}",
        f"  Win rate (P&L>0)    : {stats['win_rate']:.1%}",
        f"  Avg win / avg loss  : ${stats['avg_win']:.2f} / ${stats['avg_loss']:.2f}",
        f"  Payoff ratio        : {stats['payoff_ratio']:.2f}x",
        f"  Expectancy/trade    : ${stats['expectancy_usd']:.2f}",
        "",
        f"  Total P&L           : ${stats['total_pnl']:.2f}",
        f"  Return              : {stats['return_pct']:.2f}%",
        f"  Final equity        : ${stats['final_equity']:.2f}",
        f"  Max drawdown        : {stats['max_drawdown_pct']:.2f}%",
        f"  Sharpe (annual)     : {stats['sharpe']:.3f}",
        "",
        "── Multi-horizon comparison (same signals, different exit) ──────",
        f"  {'Hold':>6}  {'Hit%':>7}  {'Win%':>7}  {'AvgPnL':>8}  {'Expectancy':>10}  {'TotalPnL':>10}",
    ]
    for h in stats.get("horizon_analysis", []):
        lines.append(
            f"  {str(h['horizon_bars'])+'b':>6}  {h['hit_rate']:>7.1%}  {h['win_rate']:>7.1%}"
            f"  {h['avg_pnl']:>8.2f}  {h['expectancy']:>10.2f}  {h['total_pnl']:>10.2f}"
        )

    lines += ["",
              "── Hit rate by composite threshold (5b / 15b / 30b) ─────────",
              f"  {'Comp≥':>6}  {'N':>6}  {'Hit5b':>7}  {'Hit15b':>7}  {'Hit30b':>7}  {'Win%':>7}  {'Expect':>8}"]
    for row in stats.get("threshold_analysis", []):
        lines.append(
            f"  {row['composite_min']:>6.2f}  {row['n_signals']:>6}"
            f"  {row['hit_rate']:>7.1%}  {row['hit_15b']:>7.1%}  {row['hit_30b']:>7.1%}"
            f"  {row['win_rate']:>7.1%}  {row['expectancy']:>8.2f}"
        )

    lines += ["", "── Trend alignment ──────────────────────────────────────────",
              f"  {'Trend':>6}  {'N':>6}  {'BUY%':>6}  {'SELL%':>6}  {'Hit5b':>7}  {'Hit15b':>7}  {'Hit30b':>7}  {'Expect':>8}"]
    for label, row in stats.get("trend_analysis", {}).items():
        lines.append(
            f"  {label:>6}  {row['n']:>6}  {row['buy_pct']:>6.0%}  {row['sell_pct']:>6.0%}"
            f"  {row['hit_rate_5b']:>7.1%}  {row['hit_rate_15b']:>7.1%}"
            f"  {row['hit_rate_30b']:>7.1%}  {row['expectancy']:>8.2f}"
        )

    lines += ["", "── BUY vs SELL signal quality ───────────────────────────────"]
    for side, row in stats.get("side_analysis", {}).items():
        lines.append(f"  {side.upper():<6}  N={row['n']:>6}  hit={row['hit_5b']:.1%}  avg_pnl=${row['avg_pnl']:.2f}")

    lines += ["", "── Momentum signal contribution ─────────────────────────────"]
    m = stats.get("momentum_analysis", {})
    lines.append(f"  Signals with momentum : {m.get('pct_with_momentum',0):.0%}")
    lines.append(f"  Hit rate WITH momentum: {m.get('hit_5b_with_mom',0):.1%}")
    lines.append(f"  Hit rate WITHOUT      : {m.get('hit_5b_no_mom',0):.1%}")
    lines.append(f"  Avg |momentum score|  : {m.get('avg_mom_score',0):.4f}")

    lines += ["", "── Regime breakdown ─────────────────────────────────────────",
              f"  {'Regime':>10}  {'N':>6}  {'BUY%':>6}  {'Hit5b':>7}  {'Hit15b':>7}  {'AvgPnL':>8}"]
    for regime, row in stats.get("regime_analysis", {}).items():
        lines.append(
            f"  {regime:>10}  {row['n']:>6}  {row['buy_pct']:>6.0%}"
            f"  {row['hit_5b']:>7.1%}  {row['hit_15b']:>7.1%}  {row['avg_pnl']:>8.2f}"
        )

    lines += ["", "── Exit reasons (path-dependent SL/TP/time) ──────────────────"]
    for reason, n in stats.get("exit_reasons", {}).items():
        lines.append(f"  {reason:<12} {n}")

    lines += ["", "── Per-symbol breakdown ─────────────────────────────────────",
              f"  {'Symbol':<14}  {'N':>5}  {'Hit5b':>7}  {'Hit15b':>7}  {'Hit30b':>7}  {'Win%':>6}  {'TotalPnL':>10}  {'Sell%':>6}"]
    for sym, row in stats.get("per_symbol", {}).items():
        lines.append(
            f"  {sym:<14}  {int(row.get('n',0)):>5}"
            f"  {row.get('hit_5b',0):>7.1%}  {row.get('hit_15b',0):>7.1%}  {row.get('hit_30b',0):>7.1%}"
            f"  {row.get('win_rate',0):>6.1%}  {row.get('total_pnl',0):>10.2f}"
            f"  {row.get('sell_bias',0):>6.0%}"
        )

    lines += ["", "=" * W]
    text = "\n".join(lines)
    path.write_text(text)
    print(text)
