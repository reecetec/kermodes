import pytest


@pytest.fixture(autouse=True)
def lake_beside_the_store(monkeypatch):
    """Tests keep their lake beside their store: a shell set up for the cluster
    (KERMODES_LAKE, AWS_*) must not send a test's files to the cluster's bucket."""
    monkeypatch.delenv("KERMODES_LAKE", raising=False)
    for name in [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_ENDPOINT",
        "AWS_ALLOW_HTTP",
    ]:
        monkeypatch.delenv(name, raising=False)
