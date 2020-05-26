"""
functions to get data from DPC GitHub repository at
https://github.com/pcm-dpc/COVID-19
"""

import re

import pandas as pd
import requests

from . import code

URL_API = "https://api.github.com/repos/{owner}/{repo}/contents/{path}"
URL_REG = URL_API.format(owner="pcm-dpc", repo="COVID-19", path="dati-regioni")
URL_PRO = URL_API.format(
    owner="pcm-dpc", repo="COVID-19", path="dati-province"
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
re_pro = re.compile(r"dpc-covid19-ita-province-\d{8}.csv")
re_reg = re.compile(r"dpc-covid19-ita-regioni-\d{8}.csv")

__all__ = ["province", "regioni"]


def _read_csv(p):
    """read daily CSV files, drop redundant data"""

    frame = pd.read_csv(
        p,
        encoding="UTF-8",
        na_values=[""],
        keep_default_na=False,
        converters={"data": pd.to_datetime},
    )

    frame["data"] = frame["data"].dt.tz_localize("Europe/Rome")

    # column "stato" must be "ITA"
    assert (frame.stato == "ITA").all()

    # column "data" must be the same for all rows
    assert (frame.data == frame.iloc[0].data).all()

    # drop redundant data
    frame.drop(columns=DROP_COLS, inplace=True)

    return frame


def _merge_cols(df):
    ser = pd.Series(dtype=object)
    for col_name, col in df.iteritems():
        if col.notna().any():
            ser[col_name] = ",".join(col[col.notna()].tolist())
        else:
            ser[col_name] = col.iloc[0]
    return ser


def _read_regioni(path):
    frame = _read_csv(path)

    cr_max = max(code.denominazione_regione.keys())
    i = frame["codice_regione"] > cr_max
    frame["codice_provincia"] = 0
    frame.loc[i, "codice_provincia"] = frame["codice_regione"]

    frame.loc[i, "codice_regione"] = frame.loc[i, "codice_regione"].apply(
        lambda x: code.province[x].codice_regione
    )

    return frame


def _read_province(path):
    frame = _read_csv(path)

    cr_max = max(code.denominazione_regione.keys())
    i = frame["codice_regione"] > cr_max
    frame.loc[i, "codice_regione"] = frame.loc[i, "codice_provincia"].apply(
        lambda x: code.province[x].codice_regione
    )
    frame.loc[i, "denominazione_regione"] = frame.loc[
        i, "codice_provincia"
    ].apply(
        lambda x: code.denominazione_regione[code.province[x].codice_regione]
    )

    return frame


def regioni():
    req_api = requests.get(URL_REG)
    urls = []
    for i in req_api.json():
        if re_reg.fullmatch(i["name"]):
            urls.append(i["download_url"])
    if len(urls) == 0:
        return None
    regioni = _read_regioni(urls[0])
    for i in urls[1:]:
        regioni = regioni.append(_read_regioni(i), ignore_index=True)

    mi = pd.MultiIndex.from_frame(regioni[INDEX_COLS_REG])
    regioni = regioni.drop(columns=INDEX_COLS_REG).set_index(mi)

    return regioni


def province():
    req_api = requests.get(URL_PRO)
    urls = []
    for i in req_api.json():
        if re_pro.fullmatch(i["name"]):
            urls.append(i["download_url"])
    if len(urls) == 0:
        return None
    province = _read_province(urls[0])
    for i in urls[1:]:
        province = province.append(_read_province(i), ignore_index=True)

    mi = pd.MultiIndex.from_frame(province[INDEX_COLS_PRO])
    province = province.drop(columns=INDEX_COLS_PRO).set_index(mi)

    return province


def _check_totale_casi(reg, pro):
    if len(pro) != len(reg):
        raise ValueError("different number of data points")
    if (abs(pro.index - reg.index.get_level_values("data")) > "1h").any():
        raise ValueError("data points at different times")
    # we know that first province data point is empty
    assert pro.values[0] == 0
    if (pro.values[1:] != reg.values[1:]).any():
        raise ValueError("inconsistent data")


def _check_reg_pro(regioni, province):
    _TRENTINO = 4
    for key, group in province.groupby(by="codice_regione"):
        if key == _TRENTINO:
            continue
        pro = group["totale_casi"].sum(level="data")
        reg = regioni["totale_casi"].xs(key, level="codice_regione")
        _check_totale_casi(reg, pro)
    for key, group in regioni.groupby(by="codice_provincia"):
        if key == 0:
            continue
        reg = group["totale_casi"]
        pro = province["totale_casi"].xs(key=key, level="codice_provincia")
        _check_totale_casi(reg, pro)

    return


def _check_invariant(inv, msg):
    if inv.any():
        raise ValueError(
            "Invariant {} error at {}".format(msg, inv.index[inv].to_list())
        )


def validate(regioni, province):
    """validate data against some invariants"""

    # province data is consistent with regioni data
    _check_reg_pro(regioni, province)

    # check different data invariants

    # ricoverati_con_sintomi + terapia_intensiva == totale_ospedalizzati
    i1 = (
        regioni.ricoverati_con_sintomi + regioni.terapia_intensiva
        != regioni.totale_ospedalizzati
    )
    _check_invariant(i1, "totale_ospedalizzati")

    # totale_ospedalizzati + isolamento_domiciliare == totale_positivi
    i2 = (
        regioni.totale_ospedalizzati + regioni.isolamento_domiciliare
        != regioni.totale_positivi
    )
    _check_invariant(i2, "totale_positivi")

    # totale_positivi + dimessi_guariti + deceduti == totale_casi
    i4 = (
        regioni.totale_positivi + regioni.dimessi_guariti + regioni.deceduti
        != regioni.totale_casi
    )
    _check_invariant(i4, "totale_casi")

    # levels to unstack
    tlevels = regioni.index.names[1:]

    # variazione_totale_positivi = Δ totale_positivi
    i3 = (
        regioni.totale_positivi.unstack(tlevels).diff().iloc[1:]
        != regioni.variazione_totale_positivi.unstack(tlevels).iloc[1:]
    ).stack(tlevels)
    _check_invariant(i3, "Δ totale_positivi")

    # nuovi_positivi =  Δ totale_casi
    i5 = (
        regioni.totale_casi.unstack(tlevels).diff().iloc[1:]
        != regioni.nuovi_positivi.unstack(tlevels).iloc[1:]
    ).stack(tlevels)
    _check_invariant(i5, "Δ totale_casi")
