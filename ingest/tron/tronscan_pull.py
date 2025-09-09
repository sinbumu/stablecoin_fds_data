import requests, time, csv, os
BASE = "https://apilist.tronscanapi.com/api/token_trc20/transfers"
CONTR = os.environ.get("TRON_USDT", "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t")
HEADERS = {"TRON-PRO-API-KEY": os.environ.get("TRON_API_KEY", "")}

def pull(start_ts: int, end_ts: int, out_csv: str, limit: int = 200):
    page = 0
    with open(out_csv, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ts","tx_hash","from","to","amount_raw","decimals","block"])
        while True:
            url = f"{BASE}?contract_address={CONTR}&limit={limit}&start={page}&start_timestamp={start_ts}&end_timestamp={end_ts}"
            r = requests.get(url, headers=HEADERS, timeout=30)
            r.raise_for_status()
            j = r.json()
            rows = j.get("token_transfers", [])
            if not rows:
                break
            for r in rows:
                w.writerow([r.get("block_ts"), r.get("transaction_id"), r.get("from_address"), r.get("to_address"), r.get("quant"), 6, r.get("block")])
            page += limit
            time.sleep(0.3)

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=int, required=True, help="start timestamp ms")
    ap.add_argument("--end", type=int, required=True, help="end timestamp ms")
    ap.add_argument("--out", type=str, required=True)
    args = ap.parse_args()
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    pull(args.start, args.end, args.out)
