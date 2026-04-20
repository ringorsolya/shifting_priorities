"""
Master runner — executes the full analysis pipeline in sequence.

Usage:
    cd scripts/
    python run_all.py
"""

import importlib
import time
import sys

STEPS = [
    ("01_load_and_filter", "Step 1–2: Load data & Ukraine filter"),
    ("02_descriptives",    "Step 3:   Descriptive statistics"),
    ("03_indices",         "Step 4:   EFI & HFI indices"),
    ("04_plots",           "Step 5:   Visualisations"),
    ("05_hypothesis_tests","Step 6:   Hypothesis tests"),
]


def main():
    print("=" * 70)
    print("V4 UKRAINE-WAR MEDIA ANALYSIS — FULL PIPELINE")
    print("=" * 70)

    t0 = time.time()
    for module_name, description in STEPS:
        print(f"\n{'─' * 70}")
        print(f"▶ {description}")
        print(f"{'─' * 70}")
        t1 = time.time()
        try:
            mod = importlib.import_module(module_name)
            mod.main()
            print(f"  ✓ Done in {time.time() - t1:.1f}s")
        except Exception as e:
            print(f"  ✗ FAILED: {e}", file=sys.stderr)
            raise

    print(f"\n{'=' * 70}")
    print(f"Pipeline complete in {time.time() - t0:.1f}s")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
