"""
LLM provider polling and model management for Ollama and LMStudio.

Import with:
    from supportal.llm_providers import (
        fetch_ollama_models, fetch_openai_compat_models,
        fetch_ollama_model_info, poll_ollama_ps,
        poll_lmstudio_model_info, lmstudio_ensure_model_loaded,
    )
"""

import time

import requests

def fetch_ollama_models(base_url: str) -> list[str]:
    """Fetch available model names from a running Ollama instance."""
    try:
        resp = requests.get(f"{base_url.rstrip('/')}/api/tags", timeout=10, verify=False)
        resp.raise_for_status()
        return sorted(m["name"] for m in resp.json().get("models", []))
    except Exception as exc:
        raise RuntimeError(f"Could not reach Ollama at {base_url}: {exc}") from exc

def fetch_openai_compat_models(base_url: str, api_key: str = "") -> list[str]:
    """Fetch available model IDs from an OpenAI-compatible endpoint (LMStudio, OpenAI, etc.)."""
    try:
        headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        url = base_url.rstrip("/")
        if not url.endswith("/v1"):
            url += "/v1"
        resp = requests.get(f"{url}/models", headers=headers, timeout=10, verify=False)
        resp.raise_for_status()
        return sorted(m["id"] for m in resp.json().get("data", []))
    except Exception as exc:
        raise RuntimeError(f"Could not fetch models from {base_url}: {exc}") from exc

# Model name fragments that indicate thinking/reasoning capability.
# Used as fallback when the provider doesn't report capabilities explicitly.
_THINKING_MODEL_PATTERNS = ["qwen3", "qwq", "deepseek-r1", "deepseek-r2", "o1-", "o3-"]


def _model_has_thinking_by_name(model: str) -> bool:
    name = model.lower()
    return any(p in name for p in _THINKING_MODEL_PATTERNS)

def fetch_ollama_model_info(base_url: str, model: str) -> dict:
    """Return /api/show details for a model.

    Returned dict keys:
      num_ctx   – int or None (native context window)
      thinking  – bool (model supports thinking/reasoning mode)
      caps      – list[str] (raw capabilities from Ollama, e.g. ['completion','thinking'])
    """
    try:
        resp = requests.post(
            f"{base_url.rstrip('/')}/api/show",
            json={"name": model},
            timeout=15,
            verify=False,
        )
        resp.raise_for_status()
        data = resp.json()
        # num_ctx: model_info (newer Ollama) or parameters string (older)
        info   = data.get("model_info", {})
        params = data.get("parameters", "")
        num_ctx = (
            info.get("llama.context_length")
            or info.get("context_length")
            or _parse_num_ctx_from_params(params)
        )
        # capabilities: Ollama >= 0.6 returns a list e.g. ["completion", "thinking", "tools"]
        caps     = data.get("capabilities", [])
        thinking = "thinking" in caps or _model_has_thinking_by_name(model)
        return {
            "num_ctx":  int(num_ctx) if num_ctx else None,
            "thinking": thinking,
            "caps":     caps,
        }
    except Exception as exc:
        return {
            "num_ctx":  None,
            "thinking": _model_has_thinking_by_name(model),  # best-effort fallback
            "caps":     [],
            "error":    str(exc),
        }


def _parse_num_ctx_from_params(params: str) -> int | None:
    """Parse num_ctx from Ollama's parameters string (e.g. 'num_ctx 131072\\n...')."""
    for line in (params or "").splitlines():
        parts = line.strip().split()
        if len(parts) == 2 and parts[0] == "num_ctx":
            try:
                return int(parts[1])
            except ValueError:
                pass
    return None

def poll_ollama_ps(base_url: str) -> dict:
    """Return Ollama /api/ps payload (models currently loaded in memory)."""
    try:
        resp = requests.get(f"{base_url.rstrip('/')}/api/ps", timeout=3, verify=False)
        return resp.json()
    except Exception:
        return {}

def poll_lmstudio_model_info(base_url: str) -> dict:
    """Query LMStudio's /api/v1/models endpoint, falls back to /v1/models.

    LMStudio native format (returned by /api/v1/models):
      {"models": [{
        "type": "llm"|"embedding",
        "key": "<model-key>",         # identifier to use in API calls
        "loaded_instances": [...],    # non-empty when loaded
        "max_context_length": N,
        ...
      }]}

    Returns a dict with keys:
      models          : list of model info dicts (normalised)
      n_parallel      : parallel request count (None if not found)
      context_length  : context length of first loaded model (None if unknown)
      api_version     : "v1" | None
    """
    base = base_url.rstrip("/")
    result: dict = {"models": [], "n_parallel": None, "context_length": None, "api_version": None}

    # LMStudio native API — richer data, different schema from OpenAI compat
    try:
        resp = requests.get(f"{base}/api/v1/models", timeout=4, verify=False)
        if resp.ok:
            body = resp.json()
            # Native format uses "models" key; OpenAI compat uses "data"
            if isinstance(body, list):
                models = body
            else:
                models = body.get("models") or body.get("data") or []
            result["models"]      = models
            result["api_version"] = "v1"
            for m in models:
                instances = m.get("loaded_instances") or []
                is_loaded = bool(instances)
                if is_loaded:
                    # n_parallel from instance config
                    cfg = instances[0].get("config", {}) if instances else {}
                    if result["n_parallel"] is None and cfg.get("parallel") is not None:
                        result["n_parallel"] = int(cfg["parallel"])
                    # context length from instance config or model-level field
                    if result["context_length"] is None:
                        ctx = cfg.get("context_length") or m.get("max_context_length")
                        if ctx:
                            result["context_length"] = int(ctx)
            return result
    except Exception:
        pass

    # Fallback: standard OpenAI-compat /v1/models
    try:
        resp = requests.get(f"{base}/v1/models", timeout=4, verify=False)
        if resp.ok:
            result["models"]      = resp.json().get("data", [])
            result["api_version"] = "v1"
    except Exception:
        pass

    return result


def lmstudio_load_model(base_url: str, model_id: str) -> bool:
    """Ask LMStudio to load a specific model via POST /api/v1/models/load.

    The endpoint may stream SSE events until loading completes — this can take
    well over 10 seconds for large models.  We use stream=True and read only
    the first non-empty line so we can validate the response code and check for
    the fake-200 "Unexpected endpoint" body, then let the caller poll for
    completion via GET /api/v1/models.

    Returns True when the load request was accepted (even if still in progress).
    """
    base = base_url.rstrip("/")
    try:
        with requests.post(
            f"{base}/api/v1/models/load",
            json={"model": model_id},
            stream=True,
            timeout=(8, 15),   # (connect, first-chunk) — not total
            verify=False,
        ) as resp:
            if not resp.ok:
                body = resp.text or ""
                print(f"[LMStudio] load POST {resp.status_code}: {body[:200]}")
                return False
            # Read just the first SSE line to detect fake-200 rejection
            first_line = ""
            for chunk in resp.iter_lines(decode_unicode=True):
                if chunk:
                    first_line = chunk
                    break
            if "unexpected endpoint" in first_line.lower() or "unexpected method" in first_line.lower():
                print("[LMStudio] load endpoint not supported — load manually in LMStudio UI")
                return False
            print(f"[LMStudio] load accepted, SSE started: {first_line[:120]!r}")
            return True
    except requests.exceptions.ReadTimeout:
        # Server accepted but is streaming — treat as success, caller polls
        print("[LMStudio] load request streaming (ReadTimeout on first chunk — load in progress)")
        return True
    except Exception as exc:
        print(f"[LMStudio] load request exception: {exc}")
        return False


def lmstudio_ensure_model_loaded(
    base_url: str,
    desired_model: str,
    timeout_s: int = 30,
    model_type: str | None = None,
) -> str:
    """
    Ensure an appropriate model is loaded in LMStudio and return its ID.

    model_type — "embeddings" to require an embedding model, "llm" to require
                 a text-gen/VLM model, or None to accept any loaded model.
                 Uses the model type field from the LMStudio API rather than
                 fragile name matching, so it works regardless of how the user
                 named their model in the app.

    desired_model is used only as the load request target when nothing
    appropriate is loaded; it is NOT used for matching.

    Returns the actual model id to pass to the API, or "" if nothing loaded.
    """
    # LMStudio native API uses "embedding" (singular) and "llm" as type values.
    # loaded_instances is a non-empty list when the model is loaded.
    _EMB_TYPES = {"embedding", "embeddings"}
    _LLM_TYPES = {"llm", "vlm"}

    def _mid(m: dict) -> str:
        # Native API uses "key"; OpenAI-compat uses "id"
        return m.get("key") or m.get("id") or m.get("identifier") or ""

    def _is_loaded(m: dict) -> bool:
        # Native API: loaded_instances is non-empty when loaded
        # OpenAI-compat: state == "loaded"
        instances = m.get("loaded_instances")
        if instances is not None:
            return bool(instances)
        return m.get("state") == "loaded"

    def _type_ok(m: dict) -> bool:
        if model_type == "embeddings":
            return m.get("type") in _EMB_TYPES
        if model_type == "llm":
            return m.get("type") in _LLM_TYPES
        return True  # no type constraint

    info   = poll_lmstudio_model_info(base_url)
    models = info.get("models", [])
    loaded = [m for m in models if _is_loaded(m)]

    print(
        f"[LMStudio] ensure model_type={model_type!r}  "
        f"total={len(models)}  loaded={[(_mid(m), m.get('type')) for m in loaded]}"
    )

    # Return the first loaded model of the right type — no name matching.
    for m in loaded:
        if _type_ok(m):
            mid = _mid(m)
            print(f"[LMStudio] Found loaded {model_type or 'model'}: {mid!r}")
            return mid

    # Nothing of the right type loaded — request a load.
    # Prefer desired_model; fall back to first available of the right type.
    target = desired_model
    if not target:
        candidates = [m for m in models if _type_ok(m)]
        target = _mid(candidates[0]) if candidates else ""
    if not target:
        print(f"[LMStudio] No candidates of type {model_type!r} in model list — cannot load")
        return ""

    print(f"[LMStudio] Requesting load of '{target}' (type={model_type!r})…")
    ok = lmstudio_load_model(base_url, target)
    if not ok:
        # Load endpoint not supported or rejected — no point polling
        print(f"[LMStudio] Load not supported — load '{target}' manually in the LMStudio UI")
        return ""

    deadline = time.time() + timeout_s
    while time.time() < deadline:
        time.sleep(2)
        info = poll_lmstudio_model_info(base_url)
        type_matches = [
            (_mid(m), _is_loaded(m))
            for m in info.get("models", [])
            if _type_ok(m)
        ]
        print(f"[LMStudio] poll  type_matches={type_matches}")
        for m in info.get("models", []):
            if _is_loaded(m) and _type_ok(m):
                mid = _mid(m)
                print(f"[LMStudio] Model now loaded: {mid!r}")
                return mid
    print(f"[LMStudio] Timed out after {timeout_s}s — still no loaded {model_type or 'model'}")
    return ""

