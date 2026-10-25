"""Write a synthetic padron snapshot in the collector's layout.

    python tests/fixtures/make_snapshot.py <snapshots_dir> <YYYY-MM-DD> [valid_rows]

Names and RUCs are invented. The file mimics the real one: Latin-1, CRLF line
ends, "-" for empty values, a trailing delimiter, and two lines that break the
contract (wrong field count and an invalid RUC). Every fifth company has an
escaped delimiter in its name, as some real ones do.
"""

from __future__ import annotations

import sys
import zipfile
from datetime import date
from pathlib import Path

from ruc_lakehouse import contracts
from ruc_lakehouse.storage import SOURCE_FILE

STATUSES = ("ACTIVO", "BAJA DE OFICIO", "BAJA DEFINITIVA", "SUSPENSION TEMPORAL")
CONDITIONS = ("HABIDO", "NO HABIDO", "NO HALLADO SE MUDO D")
BAD_LINES = (
    "20999999999|LINEA CORTA S.A.C.|ACTIVO|HABIDO|",
    "2099999999X|RUC INVALIDO S.A.|ACTIVO|HABIDO|-|-|-|-|-|-|-|-|-|-|-|",
)


def valid_line(i: int) -> str:
    """Line i of the fixture: a person (RUC 10) or, every third line, a company."""
    if i % 3:
        values = [f"10{40_000_000 + i:08d}{i % 10}", f"PEÑA QUISPE PERSONA {i}"]
        values += [STATUSES[i % 4], CONDITIONS[i % 3]] + ["-"] * 11
    else:
        name = rf"EMPRESA ÑANDÚ {i} \| SERVICIOS S.A.C." if i % 5 == 0 else f"EMPRESA {i} S.A.C."
        values = [f"20{600_000_000 + i:09d}", name]
        values += [STATUSES[i % 4], CONDITIONS[i % 3], "150101", "AV.", f"AVENIDA {i}"]
        values += ["URB.", "LIMA", str(i)] + ["-"] * 5
    return contracts.SOURCE_DELIMITER.join(values) + contracts.SOURCE_DELIMITER


def make_snapshot(snapshots_dir: Path, snapshot_date: date, valid_rows: int = 3000) -> Path:
    lines = [contracts.SOURCE_HEADER]
    lines += [valid_line(i) for i in range(1, valid_rows + 1)]
    lines += BAD_LINES
    folder = snapshots_dir / "raw" / f"snapshot_date={snapshot_date.isoformat()}"
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / SOURCE_FILE
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        text = "\r\n".join(lines) + "\r\n"
        archive.writestr("padron_reducido_ruc.txt", text.encode(contracts.SOURCE_ENCODING))
    return path


if __name__ == "__main__":
    rows = int(sys.argv[3]) if len(sys.argv) > 3 else 3000
    print(make_snapshot(Path(sys.argv[1]), date.fromisoformat(sys.argv[2]), rows))
