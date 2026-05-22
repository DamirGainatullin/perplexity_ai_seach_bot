from __future__ import annotations

import os
from pathlib import Path

from google_auth_oauthlib.flow import InstalledAppFlow


DEFAULT_GMAIL_SCOPES = ("https://www.googleapis.com/auth/gmail.readonly",)


def _split_csv(raw: str | None) -> tuple[str, ...]:
    if not raw:
        return ()
    return tuple(part.strip() for part in raw.split(",") if part.strip())


def load_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().lstrip("=")
    return values


def main() -> None:
    root = Path(__file__).resolve().parent
    env = {**load_env_file(root / ".env"), **os.environ}

    client_secret_file = Path(env.get("GMAIL_CLIENT_SECRET_FILE", root / "credentials.json"))
    token_file = Path(env.get("GMAIL_TOKEN_FILE", root / "gmail_token.json"))
    scopes = _split_csv(env.get("GMAIL_SCOPES")) or DEFAULT_GMAIL_SCOPES

    if not client_secret_file.exists():
        raise RuntimeError(
            f"Client secret file not found: {client_secret_file}. "
            "Set GMAIL_CLIENT_SECRET_FILE in .env or env vars."
        )

    flow = InstalledAppFlow.from_client_secrets_file(str(client_secret_file), list(scopes))
    try:
        creds = flow.run_local_server(
            host="localhost",
            port=0,
            open_browser=True,
            access_type="offline",
            prompt="consent",
        )
    except Exception:
        # Fallback for environments where local callback flow is unavailable.
        flow = InstalledAppFlow.from_client_secrets_file(str(client_secret_file), list(scopes))
        flow.redirect_uri = "http://localhost"
        auth_url, _state = flow.authorization_url(
            access_type="offline",
            include_granted_scopes="true",
            prompt="consent",
        )
        print("Open URL in browser, approve access, then paste code:")
        print(auth_url)
        code = input("Authorization code: ").strip()
        if not code:
            raise RuntimeError("Authorization code is empty.")
        flow.fetch_token(code=code)
        creds = flow.credentials

    token_file.write_text(creds.to_json(), encoding="utf-8")
    print(f"Token saved to: {token_file}")


if __name__ == "__main__":
    main()
