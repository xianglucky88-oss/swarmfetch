import pytest

def test_dedupe_is_open_gap():
    from src.swarmfetch import dedupe
    with pytest.raises(NotImplementedError):
        dedupe.process({"id": "x"})

def test_frontier_is_open_gap():
    from src.swarmfetch import frontier
    with pytest.raises(NotImplementedError):
        frontier.process({"id": "x"})
