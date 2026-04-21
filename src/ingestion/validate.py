"""
validate.py
Validates Synthea CSV files before uploading to HDFS.
Called by dag_ingest_to_hdfs as the first task.
Exits with code 1 on any validation failure so Airflow marks the task as failed.
"""

import os
import sys
import csv
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [validate] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Expected files and their minimum required columns
# Any Synthea CSV header containing a dot or special char will be caught here
# ---------------------------------------------------------------------------
EXPECTED_FILES = {
    "patients.csv": [
        "Id", "BIRTHDATE", "DEATHDATE", "SSN", "DRIVERS",
        "PASSPORT", "PREFIX", "FIRST", "LAST", "SUFFIX",
        "MAIDEN", "MARITAL", "RACE", "ETHNICITY", "GENDER",
        "BIRTHPLACE", "ADDRESS", "CITY", "STATE", "COUNTY",
        "ZIP", "LAT", "LON", "HEALTHCARE_EXPENSES",
        "HEALTHCARE_COVERAGE",
    ],
    "conditions.csv": [
        "START", "STOP", "PATIENT", "ENCOUNTER", "CODE", "DESCRIPTION",
    ],
    "medications.csv": [
        "START", "STOP", "PATIENT", "PAYER", "ENCOUNTER",
        "CODE", "DESCRIPTION", "BASE_COST", "PAYER_COVERAGE",
        "DISPENSES", "TOTALCOST", "REASONCODE", "REASONDESCRIPTION",
    ],
    "observations.csv": [
        "DATE", "PATIENT", "ENCOUNTER", "CODE", "DESCRIPTION",
        "VALUE", "UNITS", "TYPE",
    ],
    "encounters.csv": [
        "Id", "START", "STOP", "PATIENT", "ORGANIZATION",
        "PROVIDER", "PAYER", "ENCOUNTERCLASS", "CODE", "DESCRIPTION",
        "BASE_ENCOUNTER_COST", "TOTAL_CLAIM_COST", "PAYER_COVERAGE",
        "REASONCODE", "REASONDESCRIPTION",
    ],
    "procedures.csv": [
        "START", "STOP", "PATIENT", "ENCOUNTER", "CODE",
        "DESCRIPTION", "BASE_COST", "REASONCODE", "REASONDESCRIPTION",
    ],
}

MIN_ROW_COUNT = 100  # fail if any file has suspiciously few rows


def validate_csv_file(filepath: Path, required_columns: list) -> bool:
    """
    Validates a single CSV file:
      1. File exists and is non-empty
      2. All required columns are present in the header
      3. Row count exceeds MIN_ROW_COUNT
      4. No column name contains a dot (Hive will reject it)

    Returns True if valid, False otherwise.
    """
    if not filepath.exists():
        log.error("Missing file: %s", filepath)
        return False

    if filepath.stat().st_size == 0:
        log.error("Empty file: %s", filepath)
        return False

    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            log.error("File has no header row: %s", filepath)
            return False

        # Check for dots in column names (Hive will reject them)
        for col in header:
            if "." in col:
                log.error(
                    "Column '%s' in %s contains a dot — Hive will reject this. "
                    "Rename the column before loading.",
                    col, filepath.name,
                )
                return False

        # Check all required columns are present
        missing = [c for c in required_columns if c not in header]
        if missing:
            log.error(
                "File %s is missing required columns: %s",
                filepath.name, missing,
            )
            return False

        # Count rows (excluding header)
        row_count = sum(1 for _ in reader)
        if row_count < MIN_ROW_COUNT:
            log.error(
                "File %s has only %d data rows (minimum %d expected). "
                "Re-run Synthea generation.",
                filepath.name, row_count, MIN_ROW_COUNT,
            )
            return False

        log.info(
            "OK  %s — %d columns, %d rows",
            filepath.name, len(header), row_count,
        )
        return True


def validate_all(raw_dir: str) -> bool:
    """
    Validates all expected CSV files in raw_dir.
    Returns True if every file passes, False if any fail.
    """
    raw_path = Path(raw_dir)
    if not raw_path.is_dir():
        log.error("Raw data directory does not exist: %s", raw_dir)
        return False

    results = {}
    for filename, required_cols in EXPECTED_FILES.items():
        filepath = raw_path / filename
        results[filename] = validate_csv_file(filepath, required_cols)

    passed = [f for f, ok in results.items() if ok]
    failed = [f for f, ok in results.items() if not ok]

    log.info("Validation summary: %d passed, %d failed", len(passed), len(failed))

    if failed:
        log.error("Failed files: %s", failed)
        return False

    log.info("All files passed validation. Ready for HDFS upload.")
    return True


if __name__ == "__main__":
    raw_dir = sys.argv[1] if len(sys.argv) > 1 else "/home/data/raw"
    ok = validate_all(raw_dir)
    sys.exit(0 if ok else 1)