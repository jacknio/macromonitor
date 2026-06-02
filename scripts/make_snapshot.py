#!/usr/bin/env python3
import json
import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

import server


def main():
    payload = server.build_monitor(refresh=False, demo=False)
    payload["snapshot"] = True
    payload["snapshotNote"] = "Bootstrap payload for fast first paint on free hosting. The app refreshes live data after load."
    path = os.path.join(ROOT, "data", "bootstrap_monitor.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))
    print("wrote %s with %s/%s metrics" % (path, payload["coverage"]["ok"], payload["coverage"]["total"]))


if __name__ == "__main__":
    main()
