import json
import subprocess
import sys
from pathlib import Path

root = Path(__file__).resolve().parent
labels = {}
for trial in sorted((root / "results" / "benchmark").glob("trial-*")):
    manifest = json.loads((trial / "manifest.json").read_text())
    scenario = next(
        (t["scenario"] for t in
         json.load(open(root / "results/benchmark/scorecard.json"))["trials"]
         if t["run_id"] == manifest["run_id"]), "control")
    labels[str(trial).replace("\\", "/")] = (
        "bad_payload" if scenario == "poison" else "control")
out = root / "results" / "benchmark" / "labels.json"
out.write_text(json.dumps(labels, indent=1))
print("labels:", len(labels))
