"""A few invented padron lines and a tiny zip, as inputs for the unit tests.

They exist for the cases a real snapshot almost never has: a line with the
wrong field count, an invalid RUC, an escaped delimiter. The file mimics the
real one: Latin-1, CRLF line ends, "-" for empty values and a trailing
delimiter.
"""

from __future__ import annotations

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
    """Line i: a person (RUC 10) or, every third line, a company."""
    if i % 3:
        values = [f"10{40_000_000 + i:08d}{i % 10}", f"PEÑA QUISPE PERSONA {i}"]
        values += [STATUSES[i % 4], CONDITIONS[i % 3]] + ["-"] * 11
    else:
        name = rf"EMPRESA ÑANDÚ {i} \| SERVICIOS" if i % 5 == 0 else f"EMPRESA {i}"
        values = [f"20{600_000_000 + i:09d}", f"{name} S.A.C."]
        values += [STATUSES[i % 4], CONDITIONS[i % 3], "150101", "AV.", f"AVENIDA {i}"]
        values += ["URB.", "LIMA", str(i)] + ["-"] * 5
    return contracts.SOURCE_DELIMITER.join(values) + contracts.SOURCE_DELIMITER


def make_snapshot(snapshots_dir: Path, snapshot_date: date, valid_rows: int) -> Path:
    """Write a zip in the collector's layout: the header, valid lines, BAD_LINES."""
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
