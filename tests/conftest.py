import pytest

from dpc_covid19 import fetch


@pytest.fixture(scope="module")
def rp_fetch():
    r = fetch.regioni()
    p = fetch.province()
    return r, p
