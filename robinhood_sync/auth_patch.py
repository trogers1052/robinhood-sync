"""
Monkey-patch for robin_stocks.robinhood.authentication._validate_sherrif_id.

The upstream implementation has two critical bugs that put this service
into a 1,000-attempt rate-limit spiral on Robinhood:

  1. The push-approval polling loop subscripts the response dict
     unconditionally:
         prompt_challenge_status["challenge_status"]
     When Robinhood returns HTTP 429 (no JSON body), `request_get`
     yields None, and the subscript raises
     `TypeError: 'NoneType' object is not subscriptable`. That exception
     propagates up to login(), which reports a generic "Login failed.
     Check credentials and try again." — completely hiding the real cause.
  2. The inner `while True` polls `get_prompts_status` every 5 seconds
     with no rate-limit awareness, no `status_code` check, and no exit
     condition other than success. Persistent 429s make this loop
     effectively spin and re-issue device challenges through every retry.

This module installs a drop-in replacement that:
  - Treats any falsy / non-dict response as "transient, back off" rather
    than crashing
  - Uses minute-scale backoff (30s → 2 min) instead of second-scale once
    the response stream goes empty, which is how Robinhood signals 429
  - Has a single overall timeout (5 minutes) covering all polling stages,
    so we cannot infinite-loop
  - Raises a clean `RuntimeError` if Robinhood demands SMS/email codes
    (we are headless and cannot prompt) — the wrapper in
    `robinhood_client.py` classifies that as DEVICE_CHALLENGE → halt
  - Logs each significant transition (kept print()-based, matching the
    upstream style, so existing stdout-capture-based classification keeps
    working)

The patch is installed by importing this module. `robinhood_client`
imports it at module load time, so any code path that goes through
`rh.login(...)` automatically benefits.
"""

import logging
import time

import robin_stocks.robinhood.authentication as _rs_auth
from robin_stocks.robinhood.helper import request_get, request_post

logger = logging.getLogger(__name__)


# Tuning constants. Conservative defaults — easy to over-correct here
# and end up with a different flavour of broken.
OVERALL_TIMEOUT_SEC = 300.0          # 5 minutes total polling budget
INITIAL_POLL_INTERVAL_SEC = 5.0      # match upstream when things are normal
RATE_LIMIT_BACKOFF_MIN_SEC = 30.0    # first backoff step when we see a 429-shaped failure
RATE_LIMIT_BACKOFF_MAX_SEC = 120.0   # never sleep longer than this between polls
FINALIZATION_RETRY_ATTEMPTS = 5


def _extract_machine_id(machine_data):
    """The id field in pathfinder POST response. Defensive against shape drift."""
    if not isinstance(machine_data, dict):
        return None
    return machine_data.get("id")


def patched_validate_sherrif_id(device_token: str, workflow_id: str):
    """
    Drop-in replacement for `_validate_sherrif_id`.

    Same signature, same overall semantics (raises TimeoutError on failure,
    returns None on success), but tolerates 429s and empty responses without
    crashing or spinning.
    """
    print("Starting verification process (auth_patch)...")

    pathfinder_url = "https://api.robinhood.com/pathfinder/user_machine/"
    machine_payload = {
        "device_id": device_token,
        "flow": "suv",
        "input": {"workflow_id": workflow_id},
    }
    machine_data = request_post(
        url=pathfinder_url, payload=machine_payload, json=True
    )
    machine_id = _extract_machine_id(machine_data)
    if not machine_id:
        raise TimeoutError(
            f"auth_patch: pathfinder POST returned no machine_id "
            f"(response={machine_data!r})"
        )

    inquiries_url = (
        f"https://api.robinhood.com/pathfinder/inquiries/{machine_id}/user_view/"
    )
    overall_start = time.monotonic()

    def _remaining() -> float:
        return OVERALL_TIMEOUT_SEC - (time.monotonic() - overall_start)

    # ------------------------------------------------------------------
    # Stage 1: discover the challenge (type + id)
    # ------------------------------------------------------------------
    challenge_id = None
    challenge_type = None
    poll_interval = INITIAL_POLL_INTERVAL_SEC

    while _remaining() > 0:
        time.sleep(poll_interval)
        inquiries_response = request_get(inquiries_url)

        if not isinstance(inquiries_response, dict) or not inquiries_response:
            # Empty / None / non-dict → almost certainly a 429 or 5xx.
            # Back off in 30s–2min steps. Upstream sleeps 5s and crashes;
            # we sleep proportionally to the rate-limit window.
            poll_interval = min(
                max(poll_interval * 2, RATE_LIMIT_BACKOFF_MIN_SEC),
                RATE_LIMIT_BACKOFF_MAX_SEC,
            )
            print(
                f"auth_patch: inquiries empty/None — backing off {poll_interval:.0f}s"
            )
            continue

        # Reset the backoff once we're getting real responses again.
        poll_interval = INITIAL_POLL_INTERVAL_SEC

        challenge = (inquiries_response.get("context") or {}).get(
            "sheriff_challenge"
        ) or {}
        challenge_type = challenge.get("type")
        challenge_id = challenge.get("id")
        challenge_status = challenge.get("status")

        if challenge_type == "prompt" and challenge_id:
            break  # proceed to stage 2

        if challenge_type in ("sms", "email"):
            # Headless service cannot prompt for codes. Raise so our wrapper
            # can classify and halt cleanly.
            raise RuntimeError(
                f"auth_patch: Robinhood demanded {challenge_type} verification — "
                f"requires interactive input the Pi cannot provide. "
                f"Prime the pickle from a non-headless environment."
            )

        if challenge_status == "validated":
            # Already approved on a prior poll — nothing more to do here.
            return

    if not (challenge_type == "prompt" and challenge_id):
        raise TimeoutError(
            "auth_patch: never received a usable prompt challenge within "
            f"{OVERALL_TIMEOUT_SEC:.0f}s"
        )

    # ------------------------------------------------------------------
    # Stage 2: poll the push-prompt endpoint until approved
    # ------------------------------------------------------------------
    print(
        f"auth_patch: check Robinhood app for device approval. "
        f"Polling up to {_remaining():.0f}s..."
    )
    prompt_url = (
        f"https://api.robinhood.com/push/{challenge_id}/get_prompts_status/"
    )
    poll_interval = INITIAL_POLL_INTERVAL_SEC
    approved = False

    while _remaining() > 0:
        time.sleep(poll_interval)
        prompt_response = request_get(url=prompt_url)

        if not isinstance(prompt_response, dict) or not prompt_response:
            poll_interval = min(
                max(poll_interval * 2, RATE_LIMIT_BACKOFF_MIN_SEC),
                RATE_LIMIT_BACKOFF_MAX_SEC,
            )
            print(
                f"auth_patch: get_prompts_status empty/None — "
                f"backing off {poll_interval:.0f}s"
            )
            continue

        poll_interval = INITIAL_POLL_INTERVAL_SEC
        # Use .get to survive any future shape changes that drop the key.
        if prompt_response.get("challenge_status") == "validated":
            print("auth_patch: device approval validated.")
            approved = True
            break

    if not approved:
        raise TimeoutError(
            "auth_patch: device approval not validated within "
            f"{OVERALL_TIMEOUT_SEC:.0f}s"
        )

    # ------------------------------------------------------------------
    # Stage 3: finalize the workflow on Robinhood's side
    # ------------------------------------------------------------------
    final_payload = {"sequence": 0, "user_input": {"status": "continue"}}
    retries_left = FINALIZATION_RETRY_ATTEMPTS

    while retries_left > 0:
        try:
            inquiries_response = request_post(
                url=inquiries_url, payload=final_payload, json=True
            )
        except Exception as e:  # network blip — retry a few times
            print(f"auth_patch: finalization POST raised: {e}")
            inquiries_response = None

        if isinstance(inquiries_response, dict):
            type_context = inquiries_response.get("type_context") or {}
            if type_context.get("result") == "workflow_status_approved":
                print("auth_patch: workflow approved.")
                return

        retries_left -= 1
        if retries_left <= 0:
            break
        time.sleep(INITIAL_POLL_INTERVAL_SEC)

    raise TimeoutError(
        "auth_patch: workflow did not reach workflow_status_approved after "
        f"{FINALIZATION_RETRY_ATTEMPTS} attempts"
    )


def install() -> None:
    """Replace robin_stocks's broken polling function with our resilient one."""
    if getattr(_rs_auth, "_validate_sherrif_id", None) is patched_validate_sherrif_id:
        return  # already installed
    _rs_auth._validate_sherrif_id = patched_validate_sherrif_id
    logger.info(
        "Installed robin_stocks auth patch: _validate_sherrif_id replaced "
        "with 429-aware implementation"
    )


# Install immediately on import — robinhood_client imports this module
# at its own top level, so the patch is in place before login() ever runs.
install()
