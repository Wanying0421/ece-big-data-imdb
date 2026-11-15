"""Run Q6/Q7 regressions through spark-submit so Spark supplies its Python path."""

from pathlib import Path
import sys

import pytest


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(root))
    raise SystemExit(pytest.main([
        str(root / "tests" / "test_exploration_q6_q7.py"), "-q",
    ]))
