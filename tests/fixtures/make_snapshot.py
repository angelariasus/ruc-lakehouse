"""Write a synthetic padron snapshot in the collector's layout.

    python tests/fixtures/make_snapshot.py <snapshots_dir> <YYYY-MM-DD> [valid_rows] [revision]

Names and RUCs are invented. The file mimics the real one: Latin-1, CRLF line
ends, "-" for empty values, a trailing delimiter, and two lines that break the
contract (wrong field count and an invalid RUC). Every fifth company has an
escaped delimiter in its name, as some real ones do.

Revision 1 is "the next day" of revision 0 with known changes, see
``revision_changes``.
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
# Revision 1 drops every 100th RUC, renames the 50th of every 100, and adds
# NEW_RUCS rows after the last one.
NEW_RUCS = 20


def revision_changes(valid_rows: int) -> dict[str, int]:
    """Changes between revision 0 and 1 of a fixture with ``valid_rows`` rows."""
    return {
        "removed": valid_rows // 100,
        "changed": (valid_rows + 50) // 100,
        "new": NEW_RUCS,
    }


BAD_LINES = (
    "20999999999|LINEA CORTA S.A.C.|ACTIVO|HABIDO|",
    "2099999999X|RUC INVALIDO S.A.|ACTIVO|HABIDO|-|-|-|-|-|-|-|-|-|-|-|",
)


def valid_line(i: int, revision: int = 0) -> str:
    """Line i of the fixture: a person (RUC 10) or, every third line, a company."""
    suffix = " NUEVO NOMBRE" if revision and i % 100 == 50 else ""
    if i % 3:
        values = [f"10{40_000_000 + i:08d}{i % 10}", f"PEÑA QUISPE PERSONA {i}{suffix}"]
        values += [STATUSES[i % 4], CONDITIONS[i % 3]] + ["-"] * 11
    else:
        name = rf"EMPRESA ÑANDÚ {i} \| SERVICIOS" if i % 5 == 0 else f"EMPRESA {i}"
        values = [f"20{600_000_000 + i:09d}", f"{name}{suffix} S.A.C."]
        values += [STATUSES[i % 4], CONDITIONS[i % 3], "150101", "AV.", f"AVENIDA {i}"]
        values += ["URB.", "LIMA", str(i)] + ["-"] * 5
    return contracts.SOURCE_DELIMITER.join(values) + contracts.SOURCE_DELIMITER


def make_snapshot(
    snapshots_dir: Path, snapshot_date: date, valid_rows: int = 3000, revision: int = 0
) -> Path:
    lines = [contracts.SOURCE_HEADER]
    if revision:
        ids = [i for i in range(1, valid_rows + NEW_RUCS + 1) if i % 100]
    else:
        ids = list(range(1, valid_rows + 1))
    lines += [valid_line(i, revision) for i in ids]
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
    revision = int(sys.argv[4]) if len(sys.argv) > 4 else 0
    print(make_snapshot(Path(sys.argv[1]), date.fromisoformat(sys.argv[2]), rows, revision))
