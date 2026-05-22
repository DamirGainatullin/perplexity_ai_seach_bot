from __future__ import annotations

import base64
import html
import os
import re
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

try:
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google.oauth2.service_account import Credentials as ServiceAccountCredentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build
except ImportError as exc:  # pragma: no cover - dependency availability varies by environment
    Request = None
    Credentials = None
    ServiceAccountCredentials = None
    InstalledAppFlow = None
    build = None
    _GOOGLE_IMPORT_ERROR = exc
else:
    _GOOGLE_IMPORT_ERROR = None

DEFAULT_GMAIL_SCOPES = ("https://www.googleapis.com/auth/gmail.readonly",)
DEFAULT_LABEL_IDS = ("INBOX",)
UNMATCHED_PROMPT_BUCKET = "__unmatched__"


@dataclass(frozen=True)
class GmailSearchConfig:
    client_secret_file: Path
    token_file: Path
    service_account_file: Path | None = None
    delegated_user: str | None = None
    allow_interactive_auth: bool = False
    user_id: str = "me"
    scopes: tuple[str, ...] = DEFAULT_GMAIL_SCOPES
    label_ids: tuple[str, ...] = DEFAULT_LABEL_IDS
    from_filters: tuple[str, ...] = ()
    subject_filters: tuple[str, ...] = ()
    max_results: int = 20
    days_back: int = 7


@dataclass(frozen=True)
class GmailMessage:
    message_id: str
    thread_id: str
    sender: str
    subject: str
    snippet: str
    body_text: str
    internal_date: datetime | None
    matched_prompts: tuple[str, ...] = ()


# Parse comma-separated values from env.
def _split_csv(raw: str | None) -> tuple[str, ...]:
    if not raw:
        return ()
    return tuple(part.strip() for part in raw.split(",") if part.strip())


# Parse integer with fallback.
def _safe_int(raw: str | None, default: int) -> int:
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


# Parse boolean values from env with fallback.
def _safe_bool(raw: str | None, default: bool) -> bool:
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


# Split text into normalized tokens for keyword matching.
def _tokenize(text: str) -> set[str]:
    return {token for token in re.split(r"[^\w]+", text.lower(), flags=re.UNICODE) if len(token) >= 3}


# Load .env-like file with KEY=VALUE lines.
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


# Build prompt keyword map from prompt filenames and first line.
def prompt_topics_from_directory(prompts_dir: Path) -> dict[str, set[str]]:
    """
    Build minimal topic keywords from prompt file names and first line content.
    """
    topics: dict[str, set[str]] = {}
    for prompt_path in sorted(prompts_dir.glob("*.txt")):
        prompt_name = prompt_path.stem.lower()
        topic_tokens = set(_tokenize(prompt_name))
        try:
            first_line = prompt_path.read_text(encoding="utf-8").splitlines()[0]
        except (UnicodeDecodeError, IndexError):
            first_line = ""
        topic_tokens.update(_tokenize(first_line))
        if topic_tokens:
            topics[prompt_name] = topic_tokens
    return topics


class GmailConnector:
    """
    Gmail connector skeleton:
    - supports non-interactive auth for servers
    - reads messages via Gmail API
    - matches candidate emails to prompt topics
    """

    def __init__(self, config: GmailSearchConfig):
        self.config = config
        self._service = None

    # Create connector settings from .env and process environment.
    @classmethod
    def from_env(cls, base_dir: Path | None = None, env_file: str = ".env") -> "GmailConnector":
        root = base_dir or Path(__file__).resolve().parent
        env = {**load_env_file(root / env_file), **os.environ}

        client_secret_file = Path(env.get("GMAIL_CLIENT_SECRET_FILE", root / "credentials.json"))
        token_file = Path(env.get("GMAIL_TOKEN_FILE", root / "gmail_token.json"))
        service_account_path = env.get("GMAIL_SERVICE_ACCOUNT_FILE")
        service_account_file = Path(service_account_path) if service_account_path else None
        delegated_user = env.get("GMAIL_DELEGATED_USER") or None
        scopes = _split_csv(env.get("GMAIL_SCOPES")) or DEFAULT_GMAIL_SCOPES
        label_ids = _split_csv(env.get("GMAIL_LABEL_IDS")) or DEFAULT_LABEL_IDS
        from_filters = (
            _split_csv(env.get("GMAIL_FROM_FILTER"))
            or _split_csv(env.get("PERPLEXITY_FROM_FILTER"))
            or _split_csv(env.get("PERPLEXITY_MAIL_FROM"))
        )
        subject_filters = _split_csv(env.get("GMAIL_SUBJECT_FILTER")) or _split_csv(
            env.get("PERPLEXITY_SUBJECT_FILTER")
        )

        config = GmailSearchConfig(
            client_secret_file=client_secret_file,
            token_file=token_file,
            service_account_file=service_account_file,
            delegated_user=delegated_user,
            allow_interactive_auth=_safe_bool(env.get("GMAIL_ALLOW_INTERACTIVE_AUTH"), False),
            user_id=env.get("GMAIL_USER_ID", "me"),
            scopes=tuple(scopes),
            label_ids=tuple(label_ids),
            from_filters=tuple(from_filters),
            subject_filters=tuple(subject_filters),
            max_results=max(1, _safe_int(env.get("GMAIL_MAX_RESULTS"), 20)),
            days_back=max(1, _safe_int(env.get("GMAIL_DAYS_BACK"), 7)),
        )
        return cls(config=config)

    # Authorize and cache Gmail API client.
    def ensure_service(self):
        if self._service is not None:
            return self._service

        if _GOOGLE_IMPORT_ERROR is not None:
            raise RuntimeError(
                "Google Gmail dependencies are missing. "
                "Install: google-api-python-client google-auth-httplib2 google-auth-oauthlib"
            ) from _GOOGLE_IMPORT_ERROR

        creds = self._load_service_account_credentials()
        if creds is None:
            creds = self._load_authorized_user_credentials()

        if creds is None:
            if self.config.allow_interactive_auth:
                creds = self._run_interactive_auth()
                self.config.token_file.write_text(creds.to_json(), encoding="utf-8")
            else:
                raise RuntimeError(
                    "No non-interactive Gmail auth available. "
                    "Use GMAIL_SERVICE_ACCOUNT_FILE + GMAIL_DELEGATED_USER (Google Workspace) "
                    "or provide a ready GMAIL_TOKEN_FILE. "
                    "If needed, enable one-time console login with GMAIL_ALLOW_INTERACTIVE_AUTH=true."
                )

        self._service = build("gmail", "v1", credentials=creds, cache_discovery=False)
        return self._service

    # Try service account credentials (Google Workspace with delegation).
    def _load_service_account_credentials(self):
        service_account_file = self.config.service_account_file
        if not service_account_file:
            return None
        if not service_account_file.exists():
            raise RuntimeError(f"Service account file not found: {service_account_file}")
        if not self.config.delegated_user:
            raise RuntimeError(
                "GMAIL_DELEGATED_USER is required with GMAIL_SERVICE_ACCOUNT_FILE."
            )
        creds = ServiceAccountCredentials.from_service_account_file(
            str(service_account_file), scopes=list(self.config.scopes)
        )
        return creds.with_subject(self.config.delegated_user)

    # Try oauth authorized user token file and refresh if needed.
    def _load_authorized_user_credentials(self):
        if not self.config.token_file.exists():
            return None
        creds = Credentials.from_authorized_user_file(str(self.config.token_file), self.config.scopes)
        if creds and creds.valid:
            return creds
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            self.config.token_file.write_text(creds.to_json(), encoding="utf-8")
            return creds
        return None

    # Run interactive oauth flow and return credentials.
    def _run_interactive_auth(self):
        if not self.config.client_secret_file.exists():
            raise RuntimeError(
                f"Gmail client secrets file not found: {self.config.client_secret_file}. "
                "Set GMAIL_CLIENT_SECRET_FILE in .env."
            )
        flow = InstalledAppFlow.from_client_secrets_file(
            str(self.config.client_secret_file), list(self.config.scopes)
        )
        return self._run_console_oauth(flow)

    # Headless-friendly OAuth flow for Linux servers without GUI/browser.
    def _run_console_oauth(self, flow: InstalledAppFlow):
        flow.redirect_uri = "http://localhost"
        auth_url, _state = flow.authorization_url(
            access_type="offline",
            include_granted_scopes="true",
            prompt="consent",
        )
        print("\nOpen this URL in any browser and approve access:")
        print(auth_url)
        print("\nPaste the authorization code from Google here.")
        code = input("Authorization code: ").strip()
        if not code:
            raise RuntimeError("Authorization code was empty.")
        flow.fetch_token(code=code)
        return flow.credentials

    # Compose Gmail search query using sender/subject/time filters.
    def build_query(self, extra_subject_terms: set[str] | None = None) -> str:
        query_parts: list[str] = []

        if self.config.days_back > 0:
            query_parts.append(f"newer_than:{self.config.days_back}d")

        for sender in self.config.from_filters:
            query_parts.append(f"from:{sender}")

        subject_terms = list(self.config.subject_filters)
        if extra_subject_terms:
            subject_terms.extend(sorted(extra_subject_terms))
        if subject_terms:
            subject_query = " OR ".join(f'subject:"{term}"' for term in subject_terms if term)
            if subject_query:
                query_parts.append(f"({subject_query})")

        return " ".join(query_parts).strip()

    # Fetch messages from Gmail and parse them into GmailMessage objects.
    def fetch_messages(
        self,
        prompt_topics: Mapping[str, set[str]] | None = None,
        max_results: int | None = None,
    ) -> list[GmailMessage]:
        service = self.ensure_service()

        extra_terms: set[str] = set()
        if prompt_topics:
            for prompt_name, tokens in prompt_topics.items():
                extra_terms.add(prompt_name)
                extra_terms.update(token for token in tokens if len(token) >= 4)

        query = self.build_query(extra_subject_terms=extra_terms)
        messages_resource = (
            service.users()
            .messages()
            .list(
                userId=self.config.user_id,
                labelIds=list(self.config.label_ids),
                q=query or None,
                maxResults=max_results or self.config.max_results,
            )
            .execute()
        )

        result: list[GmailMessage] = []
        for raw_meta in messages_resource.get("messages", []):
            raw = (
                service.users()
                .messages()
                .get(userId=self.config.user_id, id=raw_meta["id"], format="full")
                .execute()
            )
            message = self._parse_message(raw)
            if prompt_topics:
                matched = self.match_message_to_prompts(message, prompt_topics)
                message = replace(message, matched_prompts=matched)
            result.append(message)
        return result

    # Group fetched messages by prompt name.
    def group_by_prompt(
        self,
        messages: list[GmailMessage],
        prompt_topics: Mapping[str, set[str]],
        include_unmatched: bool = True,
    ) -> dict[str, list[GmailMessage]]:
        grouped: dict[str, list[GmailMessage]] = {prompt_name: [] for prompt_name in prompt_topics.keys()}
        if include_unmatched:
            grouped[UNMATCHED_PROMPT_BUCKET] = []

        for message in messages:
            matched = message.matched_prompts or self.match_message_to_prompts(message, prompt_topics)
            if not matched:
                if include_unmatched:
                    grouped[UNMATCHED_PROMPT_BUCKET].append(message)
                continue
            for prompt_name in matched:
                grouped.setdefault(prompt_name, []).append(message)
        return grouped

    # Match a single message against prompt topic keywords.
    def match_message_to_prompts(
        self,
        message: GmailMessage,
        prompt_topics: Mapping[str, set[str]],
    ) -> tuple[str, ...]:
        haystack = "\n".join([message.sender, message.subject, message.snippet, message.body_text]).lower()
        matched: list[str] = []
        for prompt_name, tokens in prompt_topics.items():
            if prompt_name in haystack:
                matched.append(prompt_name)
                continue
            if any(token in haystack for token in tokens):
                matched.append(prompt_name)
        return tuple(matched)

    # Convert raw Gmail API message into a normalized model.
    def _parse_message(self, raw: dict) -> GmailMessage:
        payload = raw.get("payload", {})
        headers = payload.get("headers", []) or []
        header_map = {str(item.get("name", "")).lower(): str(item.get("value", "")) for item in headers}
        sender = header_map.get("from", "")
        subject = header_map.get("subject", "")
        snippet = str(raw.get("snippet", ""))
        body_text = self._extract_body_text(payload)

        internal_date = None
        internal_date_raw = raw.get("internalDate")
        if internal_date_raw:
            try:
                timestamp = int(str(internal_date_raw)) / 1000.0
                internal_date = datetime.fromtimestamp(timestamp, tz=timezone.utc)
            except ValueError:
                internal_date = None

        return GmailMessage(
            message_id=str(raw.get("id", "")),
            thread_id=str(raw.get("threadId", "")),
            sender=sender,
            subject=subject,
            snippet=snippet,
            body_text=body_text,
            internal_date=internal_date,
        )

    # Extract best-effort message body text (plain preferred, html fallback).
    def _extract_body_text(self, payload: dict) -> str:
        plain_parts: list[str] = []
        html_parts: list[str] = []

        def visit(part: dict) -> None:
            mime_type = str(part.get("mimeType", "")).lower()
            body = part.get("body", {}) or {}
            data = body.get("data")
            if data:
                decoded = self._decode_part_data(data)
                if mime_type.startswith("text/plain"):
                    plain_parts.append(decoded)
                elif mime_type.startswith("text/html"):
                    html_parts.append(decoded)

            for sub in part.get("parts", []) or []:
                visit(sub)

        visit(payload)

        if plain_parts:
            return "\n".join(part.strip() for part in plain_parts if part.strip())
        if html_parts:
            html_text = "\n".join(part for part in html_parts if part.strip())
            return self._strip_html(html_text)
        return ""

    @staticmethod
    # Decode base64url-encoded message part data.
    def _decode_part_data(data: str) -> str:
        padded = data + "=" * (-len(data) % 4)
        try:
            decoded = base64.urlsafe_b64decode(padded.encode("ascii"))
            return decoded.decode("utf-8", errors="replace")
        except Exception:
            return ""

    @staticmethod
    # Strip HTML tags and normalize whitespace.
    def _strip_html(raw_html: str) -> str:
        without_tags = re.sub(r"<[^>]+>", " ", raw_html)
        normalized = re.sub(r"\s+", " ", html.unescape(without_tags)).strip()
        return normalized


if __name__ == "__main__":
    """
    Minimal smoke-check:
    - configure .env auth settings
    - run and print candidate messages grouped by prompts
    """
    connector = GmailConnector.from_env()
    prompts_dir = Path(__file__).resolve().parent / "prompts"
    topics = prompt_topics_from_directory(prompts_dir)
    messages = connector.fetch_messages(prompt_topics=topics)
    grouped = connector.group_by_prompt(messages, topics)

    print(f"Found messages: {len(messages)}")
    for prompt_name, items in grouped.items():
        print(f"- {prompt_name}: {len(items)}")
