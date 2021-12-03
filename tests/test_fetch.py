import pytest
from pandas.testing import assert_series_equal
from pytest_lazyfixture import lazy_fixture

from dpc_covid19 import fetch


@pytest.fixture(scope="module")
def province():
    return fetch.province()


@pytest.fixture(scope="module")
def regioni():
    return fetch.regioni()


@pytest.mark.parametrize(
    "dataframe, EXPECTED_CATEGORICAL",
    [
        (lazy_fixture("regioni"), fetch._CATEGORICAL_REG),
        (lazy_fixture("province"), fetch._CATEGORICAL_PRO),
    ],
)
def test_categorical_dtype(dataframe, EXPECTED_CATEGORICAL):
    for c in EXPECTED_CATEGORICAL:
        assert hasattr(dataframe[c], "cat")


@pytest.mark.parametrize(
    "dataframe", [lazy_fixture("regioni"), lazy_fixture("province")]
)
def test_const_stato(dataframe):
    assert (dataframe.stato == "ITA").all()


def test_NA(province):
    # check that 'NA' (Napoli) is not misunderstud for 'not available'
    assert_series_equal(
        province.sigla_provincia == "NA",
        province.denominazione_provincia == "Napoli",
        check_names=False,
    )
