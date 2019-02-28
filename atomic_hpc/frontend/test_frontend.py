import pytest

from atomic_hpc.frontend import run_config, retrieve_config


def test_run_config_help():
    with pytest.raises(SystemExit) as out:
        run_config.main(['-h'])
        assert out.value.code == 0


def test_retrieve_config_help():
    with pytest.raises(SystemExit) as out:
        retrieve_config.main(['-h'])
        assert out.value.code == 0
