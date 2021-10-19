import pytest
from pandas.testing import assert_series_equal

from dpc_covid19 import fetch


@pytest.fixture()
def province():
    return fetch.province()


@pytest.fixture()
def regioni():
    return fetch.regioni()


def test_categorical_dtype_regioni(regioni):
    for c in fetch._CATEGORICAL_REG:
        assert hasattr(regioni[c], "cat")


def test_categorical_dtype_province(province):
    for c in fetch._CATEGORICAL_PRO:
        assert hasattr(province[c], "cat")


def test_const_regioni(regioni):
    assert (regioni.stato == "ITA").all()


def test_const_province(province):
    assert (province.stato == "ITA").all()


def test_NA(province):
    # check that 'NA' (Napoli) is not misunderstud for 'not available'
    assert_series_equal(
        province.sigla_provincia == "NA",
        province.denominazione_provincia == "Napoli",
        check_names=False,
    )
