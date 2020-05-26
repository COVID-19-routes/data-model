import pytest
from pandas.testing import assert_series_equal
from dpc_covid19 import code, fetch


def test_NA(province):
    # check that 'NA' (Napoli) is not misunderstud for 'not available'
    assert_series_equal(
        province.sigla_provincia == "NA",
        province.denominazione_provincia == "Napoli",
        check_names=False,
    )


def test_code(province, regioni):
    assert (
        regioni.index.get_level_values("codice_regione")
        .isin(code.denominazione_regione.keys())
        .all()
    ), "spurious codice_regione in fetch.regioni"
    assert (
        province.index.get_level_values("codice_provincia")
        .isin(code.denominazione_provincia.keys())
        .all()
    ), "spurious codice_provincia in fetch.province"
    assert province.codice_regione.isin(
        code.denominazione_regione.keys()
    ).all(), "spurious codice_regione in fetch.province"


def test_trbz(regioni):

    with pytest.raises(KeyError):
        regioni.xs(key=0, level="codice_provincia").xs(
            key=4, level="codice_regione"
        )


def test_validate(regioni, province):
    fetch.validate(regioni, province)
