import pytest

from dpc_covid19 import fetch


def test_NA(rp_fetch):
    # check that 'NA' (Napoli) is not misunderstud for 'not available'
    _, p = rp_fetch
    (
        (p.sigla_provincia == "NA") == (p.denominazione_provincia == "Napoli")
    ).all()


def test_trbz(rp_fetch):
    r, _ = rp_fetch

    with pytest.raises(KeyError):
        r.xs(key=0, level="codice_provincia").xs(key=4, level="codice_regione")


def test_validate(rp_fetch):
    r, p = rp_fetch

    fetch.validate(r, p)
