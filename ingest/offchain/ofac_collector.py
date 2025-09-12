import argparse
import csv
import io
import re
import sys
import time
import requests
from datetime import datetime, timezone

ADDR_RE = re.compile(r"0x[0-9a-fA-F]{40}")


def fetch(url: str) -> bytes:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.content


def extract_addresses(csv_bytes: bytes) -> set[str]:
    out: set[str] = set()
    f = io.StringIO(csv_bytes.decode("utf-8", errors="ignore"))
    reader = csv.reader(f)
    for row in reader:
        for cell in row:
            for m in ADDR_RE.findall(cell or ""):
                out.add(m.lower())
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sdn", required=True)
    ap.add_argument("--consolidated", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    collected_at = datetime.now(timezone.utc).isoformat()
    addrs: set[str] = set()

    for url, source in [(args.sdn, "sdn"), (args.consolidated, "consolidated")]:
      try:
        csv_bytes = fetch(url)
        addrs |= extract_addresses(csv_bytes)
      except Exception as e:
        print(f"[warn] fetch failed {source}: {e}", file=sys.stderr)

    with open(args.out, "w", encoding="utf-8") as w:
        w.write("address,source,first_seen_ts,last_seen_ts,program,collected_at\n")
        for a in sorted(addrs):
            w.write(f"{a},OFAC,,,{''},{collected_at}\n")

    print(f"[done] wrote {len(addrs)} addresses -> {args.out}")


if __name__ == "__main__":
    main()


