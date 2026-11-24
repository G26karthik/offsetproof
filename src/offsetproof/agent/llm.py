"""Provider-neutral LLM adapter. Stdlib only; failures return None."""

from __future__ import annotations

import json
import os
import urllib.request


class StubAdapter:
    """Deterministic test double: returns canned text, records prompts."""

    def __init__(self, replies: list[str]) -> None:
        self.replies = list(replies)
        self.prompts: list[str] = []

    def complete(self, prompt: str) -> str | None:
        self.prompts.append(prompt)
        if not self.replies:
            return None
        return self.replies.pop(0)


class HttpAdapter:
    """Minimal chat-completions client (OpenAI-compatible endpoint)."""

    def __init__(self) -> None:
        self.api_key = os.environ.get("LLM_API_KEY", "")
        self.model = os.environ.get("LLM_MODEL", "")
        self.endpoint = os.environ.get(
            "LLM_ENDPOINT", "https://api.openai.com/v1/chat/completions"
        )

    def configured(self) -> bool:
        return bool(self.api_key and self.model)

    def complete(self, prompt: str) -> str | None:
        if not self.configured():
            return None
        body = json.dumps(
            {"model": self.model,
             "messages": [{"role": "user", "content": prompt}],
             "temperature": 0}
        ).encode()
        req = urllib.request.Request(
            self.endpoint,
            data=body,
            headers={"Content-Type": "application/json",
                     "Authorization": f"Bearer {self.api_key}"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
            return str(payload["choices"][0]["message"]["content"])
        except Exception:  # noqa: BLE001 - any transport failure abstains
            return None
