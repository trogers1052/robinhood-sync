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


# ---------------------------------------------------------------------------
# interactive code challenges (prime_session only — never on the Pi)
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clear_code_provider():
    """The provider is module-global; a leak would un-headless the Pi path."""
    yield
    auth_patch.set_code_provider(None)


def _sms_challenge(status="issued"):
    return {
        "context": {
            "sheriff_challenge": {
                "type": "sms",
                "id": "challenge-1",
                "status": status,
            }
        }
    }


def test_sms_challenge_raises_without_a_code_provider(stub_post, stub_get):
    """Headless default: no provider means halt, not block on input()."""
    stub_get.return_value = _sms_challenge()
    with pytest.raises(RuntimeError, match="requires interactive input"):
        auth_patch.patched_validate_sherrif_id("dev-tok", "wf-1")


def test_sms_challenge_answered_by_code_provider(stub_post, stub_get):
    stub_get.return_value = _sms_challenge()
    stub_post.side_effect = [
        {"id": "machine-123"},                                  # pathfinder
        {"status": "validated"},                                # challenge respond
        {"type_context": {"result": "workflow_status_approved"}},  # finalize
    ]
    auth_patch.set_code_provider(lambda challenge_type: "123456")

    assert auth_patch.patched_validate_sherrif_id("dev-tok", "wf-1") is None

    respond_call = stub_post.call_args_list[1]
    assert respond_call.kwargs["payload"] == {"response": "123456"}
    assert "challenge-1" in respond_call.kwargs["url"]


def test_code_provider_receives_the_challenge_type(stub_post, stub_get):
    stub_get.return_value = _sms_challenge()
    stub_post.side_effect = [
        {"id": "machine-123"},
        {"status": "validated"},
        {"type_context": {"result": "workflow_status_approved"}},
    ]
    seen = []
    auth_patch.set_code_provider(lambda challenge_type: seen.append(challenge_type) or "1")
    auth_patch.patched_validate_sherrif_id("dev-tok", "wf-1")
    assert seen == ["sms"]


def test_rejected_code_falls_through_to_the_headless_error(stub_post, stub_get):
    stub_get.return_value = _sms_challenge()
    stub_post.side_effect = [
        {"id": "machine-123"},
        {"status": "rejected"},
    ]
    auth_patch.set_code_provider(lambda challenge_type: "000000")
    with pytest.raises(RuntimeError, match="requires interactive input"):
        auth_patch.patched_validate_sherrif_id("dev-tok", "wf-1")


def test_empty_code_is_treated_as_no_answer(stub_post, stub_get):
    stub_get.return_value = _sms_challenge()
    auth_patch.set_code_provider(lambda challenge_type: "")
    with pytest.raises(RuntimeError, match="requires interactive input"):
        auth_patch.patched_validate_sherrif_id("dev-tok", "wf-1")


def test_aborted_code_entry_is_treated_as_no_answer(stub_post, stub_get):
    stub_get.return_value = _sms_challenge()

    def _abort(_challenge_type):
        raise KeyboardInterrupt

    auth_patch.set_code_provider(_abort)
    with pytest.raises(RuntimeError, match="requires interactive input"):
        auth_patch.patched_validate_sherrif_id("dev-tok", "wf-1")


def test_email_challenge_uses_the_same_path(stub_post, stub_get):
    challenge = _sms_challenge()
    challenge["context"]["sheriff_challenge"]["type"] = "email"
    stub_get.return_value = challenge
    stub_post.side_effect = [
        {"id": "machine-123"},
        {"status": "validated"},
        {"type_context": {"result": "workflow_status_approved"}},
    ]
    auth_patch.set_code_provider(lambda challenge_type: "654321")
    assert auth_patch.patched_validate_sherrif_id("dev-tok", "wf-1") is None
