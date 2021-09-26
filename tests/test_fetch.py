import pytest
from pandas.testing import assert_series_equal

from dpc_covid19 import fetch


def test_NA(province):
    # check that 'NA' (Napoli) is not misunderstud for 'not available'
    assert_series_equal(
        province.sigla_provincia == "NA",
        province.denominazione_provincia == "Napoli",
        check_names=False,
    )
