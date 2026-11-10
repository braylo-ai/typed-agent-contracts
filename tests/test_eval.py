import json
import subprocess
import sys
from pathlib import Path


def test_fuzz_full_arm_has_no_violations_and_baselines_do():
    root = Path(__file__).resolve().parents[1]
    subprocess.run([sys.executable, str(root / "eval" / "run_fuzz.py")], check=True, capture_output=True)
    arms = {a["arm"]: a for a in json.loads((root / "results" / "fuzz_results.json").read_text())["arms"]}
    f = arms["full"]
    assert f["fifo_violations"] == f["duplicate_deliveries"] == f["hung_waits"] == f["refunds_below_privileged"] == 0
    assert arms["no_ordering"]["fifo_violations"] > 0
    assert arms["no_deadlock_detection"]["hung_waits"] > 0
