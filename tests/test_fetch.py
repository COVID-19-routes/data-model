from dpc_covid19 import fetch


def test_fetch_and_validate():
    r, p = fetch.regioni(), fetch.province()
    fetch.validate(r, p)

    # check that 'NA' is not misunderstud for 'not available'
    (
        (p.sigla_provincia == "NA") == (p.denominazione_provincia == "Napoli")
    ).all()
