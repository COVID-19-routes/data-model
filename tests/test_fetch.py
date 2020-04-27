from dpc_covid19 import fetch


def test_fetch_and_validate():
    r, p = fetch.regioni(), fetch.province()
    fetch.validate(r, p)
