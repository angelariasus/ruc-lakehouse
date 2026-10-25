import pytest

from ruc_lakehouse.reference import ubigeo

HEADER = "IDDIST;NOMBDEP;NOMBPROV;NOMBDIST;NOM_CAPITAL (LEGAL);COD_ REG_NAT;REGION NATURAL"
ROW = "150101;LIMA;LIMA;LIMA;LIMA;1;COSTA"
FOOTNOTE = ";;;;16/ SU CAPITAL LEGAL ES TAMBO DE PAJA;;"


def test_parse_keeps_districts_and_drops_header_and_footnotes(monkeypatch):
    monkeypatch.setattr(ubigeo, "EXPECTED_DISTRICTS", 2)
    raw = "\r\n".join([HEADER, ROW, "160101;LORETO;MAYNAS;IQUITOS;IQUITOS;5;SELVA BAJA", FOOTNOTE])
    rows = ubigeo.parse(raw.encode("latin-1"))
    assert rows == [
        ("150101", "LIMA", "LIMA", "LIMA", "COSTA"),
        ("160101", "LORETO", "MAYNAS", "IQUITOS", "SELVA BAJA"),
    ]


def test_parse_refuses_an_incomplete_list():
    with pytest.raises(ValueError, match="1891"):
        ubigeo.parse("\n".join([HEADER, ROW]).encode("latin-1"))
