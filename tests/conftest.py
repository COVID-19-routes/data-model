import pytest

from dpc_covid19 import fetch


@pytest.fixture(scope="module")
def province():
    return fetch.province()


@pytest.fixture(scope="module")
def regioni():
    return fetch.regioni()
