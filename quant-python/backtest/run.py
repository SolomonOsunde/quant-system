"""
Backtest entry point.

Usage:
  python backtest/run.py [--months 6] [--symbols BTC/USD ETH/USD ...]

Downloads historical bars (or loads from cache), replays through the full
signal pipeline, and prints a statistical report to stdout + backtest/results/.
"""
import argparse
import sys
from pathlib import Path

# Ensure quant-python root is on sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

from backtest.data_loader import load_all, DEFAULT_SYMBOLS
from backtest.engine import BacktestEngine
from backtest import report
from signals.ensemble import DIRECTION_THRESHOLD, MIN_COMPOSITE


def main():
    parser = argparse.ArgumentParser(description="Run crypto strategy backtest")
    parser.add_argument("--months",  type=int,   default=6,
                        help="Months of history to download (default: 6)")
    parser.add_argument("--symbols", nargs="+",  default=None,
                        help="Symbols to backtest (default: all 15 live symbols)")
    parser.add_argument("--capital", type=float, default=100_000.0,
                        help="Starting capital in USD (default: 100000)")
    parser.add_argument("--min-confidence", type=float, default=None,
                        help="Override config.MIN_CONFIDENCE (default: live parity)")
    parser.add_argument("--research", action="store_true",
                        help="Study mode: direction/composite/confidence floors at 0.20 "
                             "(live default 0.35/0.65 is sparse without online ML)")
    parser.add_argument("--no-ofi", action="store_true",
                        help="Disable bar-approximated OFI (fold weight into tech)")
    args = parser.parse_args()

    symbols = args.symbols or DEFAULT_SYMBOLS
    min_conf = args.min_confidence
    direction_thr = DIRECTION_THRESHOLD
    min_comp = MIN_COMPOSITE
    if args.research:
        if min_conf is None:
            min_conf = 0.20
        direction_thr = 0.20
        min_comp = 0.20

    print(f"\n{'='*62}")
    print(f"  BACKTEST RUN  |  {args.months}m history  |  {len(symbols)} symbols")
    print(f"  Direction ≥ {direction_thr}  |  composite ≥ {min_comp}")
    print(f"  Confidence ≥ {min_conf if min_conf is not None else 'config.MIN_CONFIDENCE'}")
    print(f"  OFI {'OFF' if args.no_ofi else 'ON (bar-approx)'}")
    print(f"{'='*62}\n")

    bars_by_symbol = load_all(symbols=symbols, months=args.months)

    if not bars_by_symbol:
        print("ERROR: No data loaded. Check Alpaca credentials in infra/.env")
        sys.exit(1)

    print("Running signal pipeline replay...\n")
    engine = BacktestEngine(
        capital=args.capital,
        market="CRYPTO",
        enable_ofi=not args.no_ofi,
        min_confidence=min_conf,
        min_composite=min_comp,
        direction_threshold=direction_thr,
    )
    df = engine.run(bars_by_symbol)

    if df.empty:
        print("No signals were generated. Check MIN_CONFIDENCE and signal thresholds.")
        sys.exit(0)

    print(f"\nAnalyzing {len(df)} signals...\n")
    stats = report.generate(df, capital=args.capital)

    print(f"\nResults saved to: {Path(__file__).parent / 'results'}/")
    _print_verdict(stats)


def _print_verdict(stats: dict):
    hit  = stats.get("overall_hit_rate", 0.5)
    sharpe = stats.get("sharpe", 0.0)
    expect = stats.get("expectancy_usd", 0.0)
    dd   = stats.get("max_drawdown_pct", 0.0)

    print("\n" + "="*62)
    print("  EDGE VERDICT")
    print("="*62)

    if hit > 0.52 and sharpe > 0.5 and expect > 0:
        grade = "POSITIVE EDGE DETECTED"
        note  = "Strategy shows statistical edge. Refine thresholds and size."
    elif hit >= 0.50 and expect >= 0:
        grade = "MARGINAL / NEUTRAL"
        note  = "Barely above 50%. Transaction costs will erode this — signals need strengthening."
    else:
        grade = "NO EDGE / NEGATIVE"
        note  = "Hit rate below 50% or negative expectancy. Signals need redesign before live trading."

    print(f"  Result  : {grade}")
    print(f"  Details : hit={hit:.1%}  sharpe={sharpe:.2f}  expectancy=${expect:.2f}  maxDD={dd:.1f}%")
    print(f"  Advice  : {note}")
    print("="*62 + "\n")

    # Threshold-specific insight
    threshold_stats = stats.get("threshold_analysis", [])
    best = sorted(threshold_stats, key=lambda x: x.get("expectancy", -999), reverse=True)
    if best:
        b = best[0]
        print(f"  Best composite threshold: >= {b['composite_min']:.2f}")
        print(f"    N={b['n_signals']}  hit={b['hit_rate']:.1%}  win={b['win_rate']:.1%}  "
              f"avgPnL=${b['avg_pnl']:.2f}  expectancy=${b['expectancy']:.2f}")
    print()


if __name__ == "__main__":
    main()
