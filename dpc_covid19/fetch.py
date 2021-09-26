"""
functions to get data from DPC GitHub repository at
https://github.com/pcm-dpc/COVID-19
"""

import pandas as pd

# GitHub raw.githubusercontent.com
_URL_TEMPLATE = (
    "https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{path}".format
)
URL_REG = _URL_TEMPLATE(
    owner="pcm-dpc",
    repo="COVID-19",
    branch="master",
    path="dati-regioni/dpc-covid19-ita-regioni.csv",
)
URL_PRO = _URL_TEMPLATE(
    owner="pcm-dpc",
    repo="COVID-19",
    branch="master",
    path="dati-province/dpc-covid19-ita-province.csv",
)


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


def regioni():
    frame = _read_csv(URL_REG)
    _fix_nuts_code(frame, "codice_regione")
    return frame


def province():
    frame = _read_csv(URL_PRO)
    _fix_nuts_code(frame, "codice_provincia")
    return frame
