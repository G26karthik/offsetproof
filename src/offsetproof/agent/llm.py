"""Provider-neutral LLM adapter. Stdlib only; failures return None."""

from __future__ import annotations

import json
import os
import urllib.request
from typing import Any


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
    """Provider-neutral chat client. Keys stay the user's: reads
    OPENAI_API_KEY (OpenAI-compatible endpoint) or ANTHROPIC_API_KEY
    (Anthropic messages endpoint). The key value is never logged."""

    @staticmethod
    def _load_env_file() -> None:
        """Load KEY=VALUE lines from a user-owned .env into this process.

        Lookup order: $OFFSETPROOF_ENV_FILE, <repo>/.env,
        <repo>/../.env (the workspace root, where the owner keeps theirs).
        Values are never printed or logged anywhere in this codebase.
        """
        from pathlib import Path

        here = Path(__file__).resolve()
        candidates = []
        explicit = os.environ.get("OFFSETPROOF_ENV_FILE", "")
        if explicit:
            candidates.append(Path(explicit))
        candidates.append(here.parent.parent.parent.parent / ".env")
        candidates.append(here.parent.parent.parent.parent.parent / ".env")
        for path in candidates:
            try:
                text = path.read_text(encoding="utf-8")
            except OSError:
                continue
            for line in text.splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip().strip("\"'")
                if key and value and key not in os.environ:
                    os.environ[key] = value
            return

    def __init__(self) -> None:
        self._load_env_file()
        openai_key = os.environ.get("OPENAI_API_KEY", "")
        anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if openai_key:
            self.provider = "openai"
            self.api_key = openai_key
        else:
            self.provider = "anthropic"
            self.api_key = anthropic_key
        self.model = os.environ.get("LLM_MODEL", "")
        self.endpoint = os.environ.get("LLM_ENDPOINT", "")

    def configured(self) -> bool:
        return bool(self.api_key and self.model)

    @staticmethod
    def parse_openai(payload: dict[str, Any]) -> str:
        return str(payload["choices"][0]["message"]["content"])

    @staticmethod
    def parse_anthropic(payload: dict[str, Any]) -> str:
        blocks = payload.get("content", [])
        texts = [b.get("text", "") for b in blocks if isinstance(b, dict)]
        combined = "".join(texts)
        if not combined:
            raise ValueError("empty anthropic content")
        return combined

    def _request(self, prompt: str) -> tuple[str, bytes, dict[str, str], str]:
        if self.provider == "anthropic":
            url = self.endpoint or "https://api.anthropic.com/v1/messages"
            body = json.dumps({"model": self.model, "max_tokens": 2000,
                               "messages": [{"role": "user",
                                             "content": prompt}]}).encode()
            headers = {"Content-Type": "application/json",
                       "x-api-key": self.api_key,
                       "anthropic-version": "2023-06-01"}
            parse = "anthropic"
        else:
            url = self.endpoint or "https://api.openai.com/v1/chat/completions"
            body = json.dumps(
                {"model": self.model,
                 "messages": [{"role": "user", "content": prompt}],
                 "temperature": 0}).encode()
            headers = {"Content-Type": "application/json",
                       "Authorization": f"Bearer {self.api_key}"}
            parse = "openai"
        return url, body, headers, parse

    def complete(self, prompt: str) -> str | None:
        if not self.configured():
            return None
        url, body, headers, parse = self._request(prompt)
        req = urllib.request.Request(url, data=body, headers=headers,
                                     method="POST")
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
            if parse == "anthropic":
                return self.parse_anthropic(payload)
            return self.parse_openai(payload)
        except Exception:  # noqa: BLE001 - any transport failure abstains
            return None
