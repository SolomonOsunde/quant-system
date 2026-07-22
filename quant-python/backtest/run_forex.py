"""
Forex backtest entry point.

Usage:
  python backtest/run_forex.py [--months 1] [--symbols EURUSD=X GBPUSD=X ...]

Same shared ensemble / SL-TP path as crypto. Yahoo 1m FX has little/no volume,
so OFI is disabled (weight folded into tech) rather than monkey-patched thresholds.
yfinance only serves ~29 days of 1-minute data regardless of --months.
"""
import argparse
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from backtest.forex_data_loader import load_forex, DEFAULT_FOREX_SYMBOLS, FOREX_SPREAD_BPS
from backtest.engine import BacktestEngine
from backtest import report
from signals.ensemble import DIRECTION_THRESHOLD, MIN_COMPOSITE


def main():
    parser = argparse.ArgumentParser(description="Run forex strategy backtest")
    parser.add_argument("--months", type=int, default=1,
                        help="Requested history (capped at ~29 days by yfinance 1m)")
    parser.add_argument("--symbols", nargs="+", default=None)
    parser.add_argument("--capital", type=float, default=100_000.0)
    parser.add_argument("--min-confidence", type=float, default=None,
                        help="Override config.MIN_CONFIDENCE (default: live parity)")
    parser.add_argument("--research", action="store_true",
                        help="Study mode: floors at 0.20 (see crypto --research)")
    args = parser.parse_args()

    symbols = args.symbols or DEFAULT_FOREX_SYMBOLS
    min_conf = args.min_confidence
    direction_thr = DIRECTION_THRESHOLD
    min_comp = MIN_COMPOSITE
    if args.research:
        if min_conf is None:
            min_conf = 0.20
        direction_thr = 0.20
        min_comp = 0.20

    print(f"\n{'='*62}")
    print(f"  FOREX BACKTEST  |  up to {min(args.months * 30, 29)}d of 1m bars  |  {len(symbols)} pairs")
    print(f"  Direction ≥ {direction_thr}  |  composite ≥ {min_comp}  |  OFI off")
    print(f"  Confidence ≥ {min_conf if min_conf is not None else 'config.MIN_CONFIDENCE'}")
    print(f"{'='*62}\n")

    bars_by_symbol = load_forex(symbols=symbols, months=args.months)

    if not bars_by_symbol:
        print("ERROR: No forex data loaded. Check internet connection.")
        sys.exit(1)

    print("Running signal pipeline replay...\n")

    engine = BacktestEngine(
        capital=args.capital,
        market="FOREX",
        enable_ofi=False,
        spread_bps_by_symbol=FOREX_SPREAD_BPS,
        spread_bps=sum(FOREX_SPREAD_BPS.values()) / len(FOREX_SPREAD_BPS),
        min_confidence=min_conf,
        min_composite=min_comp,
        direction_threshold=direction_thr,
    )
    df = engine.run(bars_by_symbol)

    if df.empty:
        print("No signals generated.")
        sys.exit(0)

    print(f"\nAnalyzing {len(df)} signals...\n")
    stats = report.generate(df, capital=args.capital)

    results_dir = Path(__file__).parent / "results"
    for f in results_dir.glob("*.csv"):
        if not f.name.startswith("forex_"):
            shutil.copy(f, results_dir / f"forex_{f.name}")
    report_txt = results_dir / "report.txt"
    if report_txt.exists():
        report_txt.replace(results_dir / "forex_report.txt")

    print(f"\nForex results saved to: {results_dir}/")
    _print_comparison(stats)


def _print_comparison(stats: dict):
    hit = stats.get("overall_hit_rate", 0.5)
    exp = stats.get("expectancy_usd", 0.0)
    sharpe = stats.get("sharpe", 0.0)
    dd = stats.get("max_drawdown_pct", 0.0)

    print("\n" + "=" * 62)
    print("  FOREX EDGE VERDICT")
    print("=" * 62)

    if hit > 0.52 and exp > 0:
        grade = "POSITIVE EDGE"
        note = "Forex signals show edge. Compare hit rate vs crypto results."
    elif hit >= 0.50 and exp >= 0:
        grade = "MARGINAL"
        note = "Barely above 50%. Forex may need different parameters."
    else:
        grade = "NO EDGE"
        note = "Signals not predictive for forex under live-parity thresholds."

    print(f"  Result  : {grade}")
    print(f"  Details : hit={hit:.1%}  sharpe={sharpe:.2f}  expectancy=${exp:.2f}  maxDD={dd:.1f}%")
    print(f"  Advice  : {note}")

    best = sorted(
        stats.get("threshold_analysis", []),
        key=lambda x: x.get("expectancy", -999),
        reverse=True,
    )
    if best:
        b = best[0]
        print(f"\n  Best composite ≥ {b['composite_min']:.2f}: "
              f"N={b['n_signals']}  hit={b['hit_rate']:.1%}  "
              f"win={b['win_rate']:.1%}  expect=${b['expectancy']:.2f}")

    if "exit_reasons" in stats:
        print("\n  Exit reasons:")
        for reason, n in stats["exit_reasons"].items():
            print(f"    {reason:<12} {n}")
    print("=" * 62 + "\n")


if __name__ == "__main__":
    main()
