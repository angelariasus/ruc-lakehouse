"""Cut a small real sample out of two consecutive padron snapshots.

    python tests/e2e/build_sample.py <collector_dir> <day1> <day2> [random_rows]

The sample keeps the lines as SUNAT published them, in the collector's layout,
so the end-to-end CI job can load two real days:

- a random set of legal entities (RUC 20...), present on both days;
- every legal entity whose line changed between the days;
- every legal entity that appeared on the second day;
- every line that breaks the contract (an invalid RUC), kept on both days.

Natural persons (RUC 10...) are left out, as in the gold models. Names, RUCs
and addresses are real public data. Prints the counts the CI job checks.
"""

from __future__ import annotations

import random
import re
import sys
import zipfile
from pathlib import Path

SOURCE_FILE = "padron_reducido_ruc.zip"
INNER_FILE = "padron_reducido_ruc.txt"
VALID_RUC = re.compile(rb"\d{11}")
SEED = 20261026
OUT = Path(__file__).parent / "snapshots"


def read_lines(path: Path):
    with zipfile.ZipFile(path) as archive, archive.open(INNER_FILE) as handle:
        yield from handle  # bytes, line end included


def ruc_of(line: bytes) -> bytes:
    return line.split(b"|", 1)[0]


def write_zip(day: str, lines: list[bytes]) -> Path:
    folder = OUT / "raw" / f"snapshot_date={day}"
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / SOURCE_FILE
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        archive.writestr(INNER_FILE, b"".join(lines))
    return path


def main(collector: Path, day1: str, day2: str, random_rows: int) -> None:
    def source(day: str) -> Path:
        return collector / "raw" / f"snapshot_date={day}" / SOURCE_FILE

    first: dict[bytes, bytes] = {}
    header1 = b""
    rejects1: list[bytes] = []
    for number, line in enumerate(read_lines(source(day1))):
        ruc = ruc_of(line)
        if number == 0:
            header1 = line
        elif not VALID_RUC.fullmatch(ruc):
            rejects1.append(line)
        elif ruc.startswith(b"20"):
            first[ruc] = line

    chosen = set(random.Random(SEED).sample(sorted(first), random_rows))

    second: dict[bytes, bytes] = {}
    rejects2: list[bytes] = []
    header2 = b""
    seen = set()
    changed, new = set(), set()
    for number, line in enumerate(read_lines(source(day2))):
        ruc = ruc_of(line)
        if number == 0:
            header2 = line
        elif not VALID_RUC.fullmatch(ruc):
            rejects2.append(line)
        elif ruc.startswith(b"20"):
            seen.add(ruc)
            if ruc not in first:
                new.add(ruc)
                second[ruc] = line
            elif line != first[ruc]:
                changed.add(ruc)
                second[ruc] = line
            elif ruc in chosen:
                second[ruc] = line
    removed = set(first) - seen

    assert header1 == header2, "the header changed between the days"
    keep = sorted((chosen | changed) - removed)
    write_zip(day1, [header1, *(first[r] for r in keep), *rejects1])
    day2_rucs = keep + sorted(new)
    write_zip(day2, [header2, *(second[r] for r in day2_rucs), *rejects2])

    print(f"day 1: {len(keep)} valid + {len(rejects1)} rejected lines")
    print(f"day 2: {len(keep) + len(new)} valid + {len(rejects2)} rejected lines")
    print(f"new={len(new)} changed={len(changed)} removed_in_full_file={len(removed)}")
    for day in (day1, day2):
        print(day, source_size(day))


def source_size(day: str) -> str:
    path = OUT / "raw" / f"snapshot_date={day}" / SOURCE_FILE
    return f"{path.stat().st_size / 1024:.0f} KiB"


if __name__ == "__main__":
    rows = int(sys.argv[4]) if len(sys.argv) > 4 else 3000
    main(Path(sys.argv[1]), sys.argv[2], sys.argv[3], rows)
