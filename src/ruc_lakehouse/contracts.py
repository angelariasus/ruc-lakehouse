"""Data contract of the SUNAT padron reducido RUC file.

The landing step refuses a file whose header differs from ``SOURCE_HEADER``
(schema drift), and the bronze job sends every line that breaks the contract
to the rejects table instead of failing on it, up to ``MAX_REJECT_RATIO``.
"""

from __future__ import annotations

SOURCE_ENCODING = "latin-1"
SOURCE_DELIMITER = "|"
# A delimiter inside a value is escaped with a backslash, as in the company
# name "EBEN EZER \| INGENIERIA Y ARQUITECTURA S.A.C.".
SOURCE_ESCAPE = "\\"

# Every line ends with the delimiter, so a valid line splits into the source
# columns plus one empty trailing field.
SOURCE_HEADER = (
    "RUC|NOMBRE O RAZÓN SOCIAL|ESTADO DEL CONTRIBUYENTE|CONDICIÓN DE DOMICILIO|UBIGEO|"
    "TIPO DE VÍA|NOMBRE DE VÍA|CÓDIGO DE ZONA|TIPO DE ZONA|NÚMERO|INTERIOR|LOTE|"
    "DEPARTAMENTO|MANZANA|KILÓMETRO|"
)

# Bronze column for each source column, in file order. In the address,
# DEPARTAMENTO is the apartment number, not the region.
COLUMNS: tuple[str, ...] = (
    "ruc",
    "name",
    "taxpayer_status",
    "address_condition",
    "ubigeo",
    "street_type",
    "street_name",
    "zone_code",
    "zone_type",
    "street_number",
    "interior",
    "lot",
    "apartment",
    "block",
    "kilometer",
)

# SUNAT writes "-" for an empty value.
NULL_TOKEN = "-"
RUC_PATTERN = r"^[0-9]{11}$"

# Above this share of rejected lines the file is considered broken, not dirty.
MAX_REJECT_RATIO = 0.001
