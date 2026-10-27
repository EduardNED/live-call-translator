from dataclasses import dataclass
from datetime import date
from pathlib import Path
import math
import tomllib

# Outgoing (owner → remote) pairs; the incoming route runs each one reversed.
LANGUAGE_PAIRS = {("en", "fr"), ("en", "ru"), ("ru", "fr")}

# The owner-approved extra packet (kb/requirements.md) is shared by all remaining tasks,
# so no single call may be configured above it.
PACKET_CEILING_SECONDS = 600
CALL_KEYS = {"setup_seconds", "conversation_seconds", "drain_seconds",
             "packet_remaining_seconds", "packet_verified_on"}


@dataclass(frozen=True)
class Config:
    project: str
    location: str
    model: str
    source_language: str
    target_language: str
    input_device: str
    output_device: str
    max_seconds: int
    credit: dict
    bridge: dict | None = None
    call: dict | None = None
    audio_latency: str = "high"
    continuous: dict | None = None

    @classmethod
    def load(cls, path):
        with Path(path).open("rb") as handle:
            config = cls(**tomllib.load(handle))
        config.validate()
        return config

    def validate(self):
        if not self.project or "REPLACE" in self.project:
            raise ValueError("Set the existing eligible Google Cloud project")
        if self.location != "global" or self.model != "gemini-3.5-live-translate-preview":
            raise ValueError("Only the approved global translation model is supported")
        pair = (self.source_language, self.target_language)
        if pair not in LANGUAGE_PAIRS and pair[::-1] not in LANGUAGE_PAIRS:
            raise ValueError("Supported pairs: en→fr, en→ru, ru→fr")
        if type(self.max_seconds) is not int or not 1 <= self.max_seconds <= 120:
            raise ValueError("max_seconds must be an integer from 1 to 120")
        if self.audio_latency not in ("low", "high"):
            raise ValueError("audio_latency must be low or high")
        if self.call is not None:
            self.validate_call()
        if self.continuous is not None:
            if self.call is not None:
                raise ValueError("Remove the historical finite [call] table for continuous mode")
            from .call_budget import validate_continuous
            validate_continuous(self.continuous, self.credit)
        for value in (self.input_device, self.output_device):
            if not isinstance(value, str) or not value or "REPLACE" in value:
                raise ValueError("Select explicit physical devices")
        today = date.today()
        credit = self.credit
        owner_confirmed = credit.get("owner_confirmed", False)
        if type(owner_confirmed) is not bool:
            raise ValueError("credit.owner_confirmed must be a boolean")
        # Standing owner confirmation avoids repeated cloud checks; keep its original date.
        verified_on = date.fromisoformat(credit["verified_on"])
        remaining = float(credit.get("remaining_usd", 0))
        if (credit.get("eligible") is not True
                or verified_on > today
                or (not owner_confirmed and verified_on != today)
                or date.fromisoformat(credit["expires_on"]) <= today
                or not math.isfinite(remaining) or remaining < 5):
            raise ValueError("Eligible credit verification or standing owner confirmation (at least $5) required")

    def validate_call(self):
        """Finite call window. max_seconds keeps bounding the short diagnostic run without it."""
        call = self.call
        if not isinstance(call, dict) or set(call) != CALL_KEYS:
            raise ValueError(f"[call] needs exactly: {', '.join(sorted(CALL_KEYS))}")
        limits = {"setup_seconds": 60, "conversation_seconds": PACKET_CEILING_SECONDS,
                  "drain_seconds": 60, "packet_remaining_seconds": PACKET_CEILING_SECONDS}
        for key, ceiling in limits.items():
            value = call[key]
            if type(value) is not int or not 1 <= value <= ceiling:
                raise ValueError(f"call.{key} must be an integer from 1 to {ceiling}")
        total = call["setup_seconds"] + call["conversation_seconds"] + call["drain_seconds"]
        if total > call["packet_remaining_seconds"]:
            raise ValueError("Call setup + conversation + drain exceeds the remaining API packet")
        if date.fromisoformat(call["packet_verified_on"]) != date.today():
            raise ValueError("Recompute the remaining API packet from the logs today")
