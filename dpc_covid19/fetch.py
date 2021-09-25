"""
functions to get data from DPC GitHub repository at
https://github.com/pcm-dpc/COVID-19
"""

import re

import pandas as pd
import requests

# GitHub REST API v3
API_HEADERS = {"Accept": "application/vnd.github.v3+json"}
URL_API = (
    "https://api.github.com/repos/{owner}/{repo}/contents/{path}?ref={branch}"
).format
URL_REG = URL_API(
    owner="pcm-dpc", repo="COVID-19", path="dati-regioni", branch="master"
)
URL_PRO = URL_API(
    owner="pcm-dpc",
    repo="COVID-19",
    path="dati-province",
    branch="master",
)

DROP_COLS = ["stato", "lat", "long"]
DATA_COLS_REG = [
    "ricoverati_con_sintomi",
    "terapia_intensiva",
    "totale_ospedalizzati",
    "isolamento_domiciliare",
    "totale_positivi",
    "variazione_totale_positivi",
    "nuovi_positivi",
    "dimessi_guariti",
    "deceduti",
    "totale_casi",
    "tamponi",
    "casi_testati",
]
TEXT_COLS_REG = ["note_it", "note_en"]

INDEX_COLS_REG = ["data", "codice_regione", "codice_provincia"]
INDEX_COLS_PRO = ["data", "codice_provincia"]

# regex of province and regioni pathnames
# names for files for each date
# re_pro = re.compile(r"dpc-covid19-ita-province-\d{8}.csv")
# re_reg = re.compile(r"dpc-covid19-ita-regioni-\d{8}.csv")
# name of cumulative file
re_pro = re.compile(r"dpc-covid19-ita-province.csv")
re_reg = re.compile(r"dpc-covid19-ita-regioni.csv")

__all__ = ["province", "regioni"]


def _read_csv(p):
    """read CSV files"""

    frame = pd.read_csv(
        p,
        encoding="UTF-8",
        na_values=[""],
        keep_default_na=False,
        parse_dates=["data"],
    )

    frame["data"] = frame["data"].dt.tz_localize("Europe/Rome")

    return frame


def _fix_nuts_code(frame, unique_col):
    cols = frame.columns
    for c in cols[cols.str.startswith("codice_nuts")]:
        if (mask := frame[c].isna()).any():
            mapping = (
                frame[[unique_col, c]]
                .dropna()
                .drop_duplicates()
                .set_index(unique_col, verify_integrity=True)
                .squeeze()
            )
            frame.loc[mask, c] = frame.loc[mask, unique_col].map(mapping)


def _read_regioni(path):
    frame = _read_csv(path)
    _fix_nuts_code(frame, "codice_regione")
    assert frame.codice_nuts_1.notna().all()
    assert frame.codice_nuts_2.notna().all()

    return frame


def _read_province(path):
    frame = _read_csv(path)
    _fix_nuts_code(frame, "codice_provincia")
    assert frame.codice_nuts_1.notna().all()
    assert frame.codice_nuts_2.notna().all()

    return frame


def regioni():
    req_api = requests.get(URL_REG, headers=API_HEADERS)
    urls = []
    for i in req_api.json():
        if re_reg.fullmatch(i["name"]):
            urls.append(i["download_url"])
    if len(urls) == 0:
        return None
    regioni = _read_regioni(urls[0])
    for i in urls[1:]:
        regioni = regioni.append(_read_regioni(i), ignore_index=True)

    return regioni


def province():
    req_api = requests.get(URL_PRO, headers=API_HEADERS)
    urls = []
    for i in req_api.json():
        if re_pro.fullmatch(i["name"]):
            urls.append(i["download_url"])
    if len(urls) == 0:
        return None
    province = _read_province(urls[0])
    for i in urls[1:]:
        province = province.append(_read_province(i), ignore_index=True)

    return province
