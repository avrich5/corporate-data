import json
import logging
import asyncio
import hashlib
from typing import Optional
from dataclasses import dataclass

from anthropic import AsyncAnthropic, APIError as AnthropicAPIError
from openai import AsyncOpenAI, APIError as OpenAIAPIError

import config

logger = logging.getLogger(__name__)


class LLMError(Exception):
    """Custom exception unifying underlying provider API errors."""
    pass


@dataclass
class LLMResponse:
    provider: str
    model: str
    content: str
    prompt_tokens: int
    completion_tokens: int
    input_hash: str


class BaseClient:
    def _compute_hash(self, system: str, prompt: str) -> str:
        s = f"{system}\n\n{prompt}"
        return hashlib.sha256(s.encode("utf-8")).hexdigest()

    async def _retry_with_backoff(self, coro_func, *args, **kwargs):
        delay = config.LLM_RETRY_BASE_DELAY
        for attempt in range(config.LLM_RETRY_MAX):
            try:
                return await coro_func(*args, **kwargs)
            except Exception as e:
                # Catching general Exception to allow unifying Anthropic/OpenAI
                is_last_attempt = attempt == config.LLM_RETRY_MAX - 1
                if is_last_attempt:
                    raise LLMError(f"API failed after {config.LLM_RETRY_MAX} attempts: {e}") from e
                
                logger.warning(f"API attempt {attempt + 1} failed: {e}. Retrying in {delay}s...")
                await asyncio.sleep(delay)
                delay *= 2


class AnthropicClient(BaseClient):
    def __init__(self, api_key: str, model: str = config.ANTHROPIC_MODEL):
        self.client = AsyncAnthropic(api_key=api_key)
        self.model = model

    async def complete(
        self,
        prompt: str,
        system: str,
        max_tokens: int = 4096,
        temperature: float = 0.0,
        json_mode: bool = False,  # ignored for Anthropic (no native json_mode)
    ) -> LLMResponse:
        input_hash = self._compute_hash(system, prompt)

        async def _call():
            resp = await self.client.messages.create(
                model=self.model,
                max_tokens=max_tokens,
                system=system,
                messages=[{"role": "user", "content": prompt}],
                temperature=temperature,
            )
            if resp.stop_reason != "end_turn":
                logger.warning(
                    f"Anthropic stop_reason={resp.stop_reason!r} — "
                    "response may be truncated"
                )
            content = resp.content[0].text
            return LLMResponse(
                provider="anthropic",
                model=self.model,
                content=content,
                prompt_tokens=resp.usage.input_tokens,
                completion_tokens=resp.usage.output_tokens,
                input_hash=input_hash
            )
            
        return await self._retry_with_backoff(_call)


class OpenAIClient(BaseClient):
    def __init__(self, api_key: str, model: str = config.OPENAI_MODEL):
        self.client = AsyncOpenAI(api_key=api_key)
        self.model = model

    async def complete(
        self,
        prompt: str,
        system: str,
        max_tokens: int = 1024,
        temperature: float = 0.0,
        json_mode: bool = False,
    ) -> LLMResponse:
        input_hash = self._compute_hash(system, prompt)

        async def _call():
            kwargs: dict = dict(
                model=self.model,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": prompt},
                ],
                max_tokens=max_tokens,
                temperature=temperature,
            )
            if json_mode:
                kwargs["response_format"] = {"type": "json_object"}
            resp = await self.client.chat.completions.create(**kwargs)
            content = resp.choices[0].message.content
            # Safely grab usage if present
            usage = resp.usage
            prompt_tokens = usage.prompt_tokens if usage else 0
            completion_tokens = usage.completion_tokens if usage else 0
            
            return LLMResponse(
                provider="openai",
                model=self.model,
                content=content,
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                input_hash=input_hash
            )

        return await self._retry_with_backoff(_call)
