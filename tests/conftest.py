"""
Shared test fixtures.

The autouse network guard exists because authentication now issues its own
HTTP requests (``robinhood_sync.session``) instead of going through
``rh.login()``. A test that mocks only ``rh`` would otherwise POST real
credentials at ``api.robinhood.com`` — which is exactly the kind of
accidental login traffic that got this account rate-limited in production.
Tests that need HTTP must patch ``robinhood_sync.session.SESSION`` explicitly.
"""

import pytest
from robin_stocks.robinhood.globals import SESSION


@pytest.fixture(autouse=True)
def block_real_network(monkeypatch):
    """Make any un-mocked HTTP call through robin_stocks' session fail loudly."""

    def _blocked(*args, **kwargs):
        raise RuntimeError(
            "Real network access is blocked in tests. Patch "
            "`robinhood_sync.session.SESSION` (or the relevant request helper) "
            f"instead. Attempted: {args[:1]}"
        )

    for method in ("get", "post", "put", "delete", "request"):
        monkeypatch.setattr(SESSION, method, _blocked, raising=False)
