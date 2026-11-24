logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("collector.log", encoding="utf-8")],
)

def drms_tai_string(dt_utc: datetime) -> str:
    # JSOC time-range syntax uses TAI. This approximate conversion adds the
    # current UTC/TAI offset (37 seconds as of 2026); the 12-hour window is
    # insensitive to this small boundary difference.
    dt_tai = dt_utc + timedelta(seconds=37)
    return dt_tai.strftime("%Y.%m.%d_%H:%M:%S_TAI")

def get_recent_keywords(client: drms.Client) -> pd.DataFrame:
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=WINDOW_HOURS)
    q = f"{SERIES}[{drms_tai_string(start)}-{drms_tai_string(now)}]"
    logging.info("Querying JSOC record set: %s", q)
    df = client.query(q, key=KEYS)
    if df is None or df.empty:
        return pd.DataFrame(columns=KEYS)
    # Normalize timestamps; preserve original T_REC as provenance field.
    df["T_REC"] = df["T_REC"].astype(str)
    for feature in LIVE_SCALAR_FEATURES:
        if feature not in df.columns:
            df[feature] = pd.NA
    df["HARPNUM"] = pd.to_numeric(df["HARPNUM"], errors="coerce")
    return df[KEYS]

def merge_save(df_new: pd.DataFrame) -> None:
    if df_new.empty:
        logging.warning("No new records returned.")
        return
    if os.path.exists(CSV_PATH):
        df_old = pd.read_csv(CSV_PATH)
        combined = pd.concat([df_old, df_new], ignore_index=True)
    else:
        combined = df_new.copy()

    # Keep a unique row per active region/time and sort.
    combined = combined.drop_duplicates(["HARPNUM", "T_REC"], keep="last")
    combined["_sort_time"] = pd.to_datetime(
        combined["T_REC"].str.replace("_TAI", "", regex=False),
        format="%Y.%m.%d_%H:%M:%S", errors="coerce", utc=True
    )
    cutoff = datetime.now(timezone.utc) - timedelta(hours=WINDOW_HOURS)
    combined = combined[combined["_sort_time"] >= cutoff]
    combined = combined.sort_values(["HARPNUM", "_sort_time"])
    combined = combined.drop(columns=["_sort_time"])
    # Keep all 24 model columns in exact order; map-derived features remain blank.
    for f in FEATURES_24:
        if f not in combined:
            combined[f] = pd.NA
    output_cols = ["T_REC", "HARPNUM"] + FEATURES_24
    combined = combined[output_cols]
    combined.to_csv(CSV_PATH, index=False)
    logging.info("Saved %d rows to %s", len(combined), CSV_PATH)

def main():
    client = drms.Client(email=JSOC_EMAIL) if JSOC_EMAIL else drms.Client()
    logging.info("SWAN-SF live collector starting for series %s", SERIES)
    while True:
        cycle_started = time.time()
        try:
            df = get_recent_keywords(client)
            merge_save(df)
        except Exception:
            logging.exception("Collection cycle failed; will retry next cycle.")
        elapsed = time.time() - cycle_started
        time.sleep(max(5, INTERVAL_SECONDS - elapsed))

if __name__ == "__main__":
    main()

