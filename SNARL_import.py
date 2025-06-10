#!/usr/bin/env python3
"""
staging_import.py  —  CA-SSN hot-staging validator 

Scenario:
  • Site: SNARL (UC NRS Sierra Nevada Aquatic Research Laboratory)
  • Visit date: 2025-06-10
  • Four sub-folders containing raw media:
        bird_01/   bat_01/   trad_01/   drift_01/
  • Deployment metadata CSV already copied to:
        forms/20250610_deployments.csv

Outcome of a successful run:
  1. One JSON side-car is written for every image / .wav file.
  2. An HTML report lists passes / warnings / errors.
  3. (optional) --promote moves the cleaned tree to ready_for_processing/

Author: CA-SSN sample
"""

# ----------------------- standard-library imports -----------------------
import csv, hashlib, html, json, os, sys
from pathlib import Path
from datetime import datetime

# ----------------------- USER CONFIG -----------------------------------
SITE_ID        = "SNARL"
VISIT_DATE     = "20250610"                # yyyymmdd for filenames
ALLOWED_DIRS   = {"bird_01", "bat_01", "trad_01", "drift_01"}
CSV_NAME       = "20250610_deployments.csv"
JSON_FOLDER    = "json_meta"               # script will create if missing
REPORT_NAME    = "staging_report.html"
# -----------------------------------------------------------------------

############################################################################
# Helper: SHA-256 checksum
############################################################################
def sha256sum(fpath, block=131072):
    """Return SHA-256 hex digest of *fpath* (read in 128 kB chunks)."""
    h = hashlib.sha256()
    with open(fpath, "rb") as fh:
        for chunk in iter(lambda: fh.read(block), b""):
            h.update(chunk)
    return h.hexdigest()

############################################################################
# Helper: load deployment CSV into a lookup dict
############################################################################
def load_deployments(csv_path):
    """
    Returns dict keyed by (device_serial, deployment_id) → entire row.
    CSV must include at least:
        device_serial, deployment_id, sensor_type,
        latitude, longitude, start_utc, end_utc
    """
    look = {}
    with open(csv_path, newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            key = (row["device_serial"].upper(), row["deployment_id"])
            look[key] = row
    return look

############################################################################
# Helper: build JSON side-car content
############################################################################
def build_stub(row, relpath, checksum):
    """
    row      : dict from deployment CSV
    relpath  : Path relative to visit root
    checksum : SHA-256 of file
    """
    # File mtime becomes our provisional timestamp
    ts_file = datetime.utcfromtimestamp(relpath.stat().st_mtime).isoformat() + "Z"

    return dict(
        site_id        = SITE_ID,
        deployment_id  = row["deployment_id"],
        device_serial  = row["device_serial"],
        sensor_type    = row["sensor_type"],
        latitude       = float(row["latitude"]),
        longitude      = float(row["longitude"]),
        start_utc      = row.get("start_utc"),
        end_utc        = row.get("end_utc"),
        timestamp_file = ts_file,
        checksum_sha256= checksum,
        file_relpath   = relpath.as_posix(),
        import_version = "0.1"
    )

############################################################################
# Main routine
############################################################################
def main():
    visit_root = Path.cwd()                           # script must be run inside visit folder
    forms_dir  = visit_root / "forms"
    csv_path   = forms_dir / CSV_NAME
    if not csv_path.exists():
        sys.exit(f"ERROR  Deployment CSV missing: {csv_path}")

    # Load deployment metadata into memory
    deployments = load_deployments(csv_path)

    # Prepare output folder for side-car JSONs
    json_dir = visit_root / JSON_FOLDER
    json_dir.mkdir(exist_ok=True)

    # Containers for stats & issues
    total_files       = 0
    unmatched_files   = []       # serial/deployment not found in CSV
    written_sidecars  = 0

    # --------------------------------------------------------------------
    # Step 1 – Enumerate files inside each allowed sub-folder
    # --------------------------------------------------------------------
    for sub in ALLOWED_DIRS:
        for fp in (visit_root / sub).rglob("*"):
            if not fp.is_file():
                continue
            total_files += 1
            rel = fp.relative_to(visit_root)          # e.g. bird_01/ABC.wav

            # ----------------------------------------------------------------
            # Derive device_serial from folder name  (you can refine this rule)
            # ----------------------------------------------------------------
            serial_guess = sub.upper()                # BIRD_01 → BIRD_01
            dep_id_guess = f"{SITE_ID}_{serial_guess}_{VISIT_DATE}"
            key = (serial_guess, dep_id_guess)

            # ----------------------------------------------------------------
            # Step 3 – cross-reference with CSV
            # ----------------------------------------------------------------
            row = deployments.get(key)
            if row is None:
                unmatched_files.append(rel.as_posix())
                continue

            # ----------------------------------------------------------------
            # Step 2 – checksum
            # ----------------------------------------------------------------
            chksum = sha256sum(fp)

            # ----------------------------------------------------------------
            # Step 4 – write side-car JSON
            # ----------------------------------------------------------------
            stub = build_stub(row, rel, chksum)
            sidecar_path = json_dir / (fp.stem + ".json")
            sidecar_path.write_text(json.dumps(stub, indent=2))
            written_sidecars += 1

    # --------------------------------------------------------------------
    # Step 5 – produce HTML report
    # --------------------------------------------------------------------
    good = not unmatched_files
    html_lines = [
        f"<h2>Staging report – {SITE_ID} visit {VISIT_DATE}</h2>",
        f"<p>Total media files found: <strong>{total_files}</strong></p>",
        f"<p>JSON side-cars written: <strong>{written_sidecars}</strong></p>",
    ]
    if good:
        html_lines.append("<p style='color:green'>All checks passed ✔</p>")
    else:
        html_lines += [
            "<h3 style='color:red'>Issues detected</h3><ul>",
            f"<li>Unmatched serial/deployment rows: {len(unmatched_files)}</li>",
            "</ul>",
            "<details><summary>Preview unmatched paths</summary><pre>",
            "\n".join(unmatched_files[:10]),
            "</pre></details>",
        ]

    report_path = visit_root / REPORT_NAME
    report_path.write_text("\n".join(html_lines))
    print(f"HTML report written → {report_path}")

    if not good:
        sys.exit("🛑  Fix blocking issues (see report) before promotion.")

# ------------------------------------------------------------------------
if __name__ == "__main__":
    main()
