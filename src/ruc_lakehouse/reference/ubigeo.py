"""Build the dbt seed of Peru's districts from INEI's official ubigeo list.

    python -m ruc_lakehouse.reference.ubigeo dbt/seeds/ubigeo.csv

Source: INEI, "Ubigeos 2022, 1 891 distritos" (datosabiertos.gob.pe, ODbL).
The file is Latin-1, semicolon-separated and ends with footnotes, which are
dropped. SUNAT uses the same INEI codes in the padron.
"""

from __future__ import annotations

import argparse
import csv
import io
import sys
import urllib.request
import zipfile
from pathlib import Path

SOURCE_URL = "https://www.inei.gob.pe/media/DATOS_ABIERTOS/UBIGEOS/UBIGEOS_2022_1891_distritos.zip"
EXPECTED_DISTRICTS = 1891
COLUMNS = ("ubigeo", "department", "province", "district", "natural_region")


def download(url: str = SOURCE_URL) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(request, timeout=60) as response:
        archive = zipfile.ZipFile(io.BytesIO(response.read()))
    (name,) = archive.namelist()
    return archive.read(name)


def parse(raw: bytes) -> list[tuple[str, ...]]:
    """District rows; header and footnotes (no 6-digit code) are skipped."""
    reader = csv.reader(io.StringIO(raw.decode("latin-1")), delimiter=";")
    rows = []
    for record in reader:
        code = record[0].strip() if record else ""
        if len(code) == 6 and code.isdigit():
            department, province, district = (value.strip() for value in record[1:4])
            rows.append((code, department, province, district, record[6].strip()))
    if len(rows) != EXPECTED_DISTRICTS:
        raise ValueError(f"expected {EXPECTED_DISTRICTS} districts, got {len(rows)}")
    return rows


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("output", type=Path)
    args = parser.parse_args(argv)
    rows = parse(download())
    with args.output.open("w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file, lineterminator="\n")
        writer.writerow(COLUMNS)
        writer.writerows(sorted(rows))
    print(f"wrote {len(rows)} districts to {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
