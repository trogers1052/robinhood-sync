"""
Tests for the robin_stocks._validate_sherrif_id replacement.

The patched function is hard to integration-test because it calls real
Robinhood endpoints. We test it by stubbing `request_get` / `request_post`
and verifying the function:
  - never raises TypeError on a None/empty response (the upstream crash)
  - returns cleanly once a "validated" response arrives
  - raises clean RuntimeError when Robinhood demands SMS/email codes
  - raises TimeoutError when no validation arrives within the window
  - applies rate-limit backoff (sleep > 0) on empty responses
"""

from unittest.mock import patch

import pytest

from robinhood_sync import auth_patch


@pytest.fixture(autouse=True)
def _no_real_sleep(monkeypatch):
    """time.sleep would slow each test by 5+ seconds; stub it out."""
    monkeypatch.setattr(auth_patch.time, "sleep", lambda _s: None)


@pytest.fixture(autouse=True)
def _fast_timeout(monkeypatch):
    """Tighten the overall timeout so timeout tests don't run for 5 minutes."""
    monkeypatch.setattr(auth_patch, "OVERALL_TIMEOUT_SEC", 2.0)


@pytest.fixture
def stub_post():
    """Default to a healthy pathfinder response so we can reach stage 2."""
    with patch.object(auth_patch, "request_post") as m:
        m.return_value = {"id": "machine-123"}
        yield m


@pytest.fixture
def stub_get():
    with patch.object(auth_patch, "request_get") as m:
        yield m


# ---------------------------------------------------------------------------
# install() is idempotent
# ---------------------------------------------------------------------------


def test_install_is_idempotent():
    auth_patch.install()
    auth_patch.install()
    import robin_stocks.robinhood.authentication as rs_auth
    assert rs_auth._validate_sherrif_id is auth_patch.patched_validate_sherrif_id


# ---------------------------------------------------------------------------
# stage 1 — discovery
# ---------------------------------------------------------------------------


def test_pathfinder_returns_no_id_raises(stub_post, stub_get):
    stub_post.return_value = {}  # no id key
    with pytest.raises(TimeoutError, match="no machine_id"):
        auth_patch.patched_validate_sherrif_id("dev-tok", "wf-1")


def test_sms_challenge_raises_clean(stub_post, stub_get):
    stub_get.return_value = {
        "context": {"sheriff_challenge": {"type": "sms", "id": "c-1", "status": "issued"}}
    }
    with pytest.raises(RuntimeError, match="sms verification"):
        auth_patch.patched_validate_sherrif_id("dev-tok", "wf-1")


def test_email_challenge_raises_clean(stub_post, stub_get):
    stub_get.return_value = {
        "context": {"sheriff_challenge": {"type": "email", "id": "c-2", "status": "issued"}}
    }
    with pytest.raises(RuntimeError, match="email verification"):
        auth_patch.patched_validate_sherrif_id("dev-tok", "wf-1")


def test_already_validated_returns_early(stub_post, stub_get):
    stub_get.return_value = {
        "context": {"sheriff_challenge": {"type": "prompt", "id": None, "status": "validated"}}
    }
    # status=validated short-circuits before we'd otherwise fail on missing id
    assert auth_patch.patched_validate_sherrif_id("dev-tok", "wf-1") is None


def test_empty_inquiries_response_does_not_crash(stub_post, stub_get):
    """The original crash: response is None → subscript blows up. We must not."""
    stub_get.return_value = None  # simulate 429
    with pytest.raises(TimeoutError, match="never received a usable prompt"):
        # No TypeError, just a clean timeout.
        auth_patch.patched_validate_sherrif_id("dev-tok", "wf-1")


def test_empty_response_triggers_rate_limit_backoff(stub_post, stub_get, monkeypatch):
    """Subsequent empty responses must increase the sleep interval."""
    stub_get.return_value = None
    sleeps: list[float] = []
    monkeypatch.setattr(auth_patch.time, "sleep", lambda s: sleeps.append(s))
    # Allow many iterations within timeout
    monkeypatch.setattr(auth_patch, "OVERALL_TIMEOUT_SEC", 0.1)
    with pytest.raises(TimeoutError):
        auth_patch.patched_validate_sherrif_id("dev-tok", "wf-1")
    # First sleep is the initial interval; later sleeps escalate to RATE_LIMIT_BACKOFF_MIN.
    assert any(s >= auth_patch.RATE_LIMIT_BACKOFF_MIN_SEC for s in sleeps), \
        f"never escalated backoff; saw {sleeps}"


# ---------------------------------------------------------------------------
# stage 2 — prompt polling
# ---------------------------------------------------------------------------


def test_prompt_validated_proceeds_to_stage3_then_returns(stub_post, stub_get):
    # Stage 1: prompt challenge with an id
    # Stage 2: validated immediately
    # Stage 3: workflow approved on first try
    stub_get.side_effect = [
        # stage 1
        {"context": {"sheriff_challenge": {"type": "prompt", "id": "c-9", "status": "issued"}}},
        # stage 2
        {"challenge_status": "validated"},
    ]
    stub_post.side_effect = [
        {"id": "machine-X"},                                       # pathfinder
        {"type_context": {"result": "workflow_status_approved"}},   # finalize
    ]
    assert auth_patch.patched_validate_sherrif_id("dev-tok", "wf-1") is None


def test_prompt_polling_tolerates_empty_then_validates(stub_post, stub_get):
    stub_get.side_effect = [
        # stage 1: usable prompt
        {"context": {"sheriff_challenge": {"type": "prompt", "id": "c-9", "status": "issued"}}},
        # stage 2: 429, 429, then validated
        None,
        None,
        {"challenge_status": "validated"},
    ]
    stub_post.side_effect = [
        {"id": "machine-X"},
        {"type_context": {"result": "workflow_status_approved"}},
    ]
    auth_patch.patched_validate_sherrif_id("dev-tok", "wf-1")


def test_prompt_never_validated_raises_timeout(stub_post, stub_get):
    """Persistent 429s on stage 2 must raise TimeoutError, not loop forever."""
    calls = {"n": 0}
    prompt_response = {
        "context": {"sheriff_challenge": {"type": "prompt", "id": "c-9", "status": "issued"}}
    }

    def _g(*args, **kwargs):
        calls["n"] += 1
        # First call: give a usable prompt challenge so we advance to stage 2.
        # All subsequent calls: simulate persistent 429 (None) until timeout.
        return prompt_response if calls["n"] == 1 else None

    stub_get.side_effect = _g
    with pytest.raises(TimeoutError, match="device approval not validated"):
        auth_patch.patched_validate_sherrif_id("dev-tok", "wf-1")


# ---------------------------------------------------------------------------
# stage 3 — finalization
# ---------------------------------------------------------------------------


def test_finalization_retries_then_raises(stub_post, stub_get):
    stub_get.side_effect = [
        {"context": {"sheriff_challenge": {"type": "prompt", "id": "c-9", "status": "issued"}}},
        {"challenge_status": "validated"},
    ]
    # pathfinder, then 5 finalization attempts all unapproved
    stub_post.side_effect = [{"id": "m"}] + [{"type_context": {"result": "still_pending"}}] * 5
    with pytest.raises(TimeoutError, match="workflow did not reach workflow_status_approved"):
        auth_patch.patched_validate_sherrif_id("dev-tok", "wf-1")


def test_finalization_eventual_success(stub_post, stub_get):
    stub_get.side_effect = [
        {"context": {"sheriff_challenge": {"type": "prompt", "id": "c-9", "status": "issued"}}},
        {"challenge_status": "validated"},
    ]
    stub_post.side_effect = [
        {"id": "m"},
        {"type_context": {"result": "still_pending"}},
        {"type_context": {"result": "workflow_status_approved"}},
    ]
    # No raise.
    auth_patch.patched_validate_sherrif_id("dev-tok", "wf-1")
