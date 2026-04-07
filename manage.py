import asyncio
import json
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Iterable
from urllib import error, parse, request


BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"
PROMPTS_DIR = BASE_DIR / "prompts"
DB_PATH = BASE_DIR / "weekly_bot.sqlite3"
PERPLEXITY_URL = "https://api.perplexity.ai/chat/completions"
TELEGRAM_API_URL = "https://api.telegram.org/bot{token}/{method}"
MAX_TELEGRAM_MESSAGE_LENGTH = 4000
WEEKDAY_NAMES = {
    0: "понедельник",
    1: "вторник",
    2: "среда",
    3: "четверг",
    4: "пятница",
    5: "суббота",
    6: "воскресенье",
}


def load_env(path: Path) -> dict[str, str]:
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


def read_prompt(path: Path) -> str:
    for encoding in ("utf-8", "utf-8-sig", "cp1251", "cp866"):
        try:
            return path.read_text(encoding=encoding).strip()
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError("prompt", b"", 0, 1, f"Unable to decode {path}")


def split_message(text: str, limit: int = MAX_TELEGRAM_MESSAGE_LENGTH) -> list[str]:
    if len(text) <= limit:
        return [text]

    chunks: list[str] = []
    current = ""
    for block in text.split("\n\n"):
        candidate = block if not current else f"{current}\n\n{block}"
        if len(candidate) <= limit:
            current = candidate
            continue

        if current:
            chunks.append(current)
            current = ""

        while len(block) > limit:
            split_at = block.rfind("\n", 0, limit)
            if split_at <= 0:
                split_at = limit
            chunks.append(block[:split_at].strip())
            block = block[split_at:].strip()
        current = block

    if current:
        chunks.append(current)
    return chunks


def format_schedule(day: int, run_time: time) -> str:
    return f"{WEEKDAY_NAMES[day]} в {run_time.strftime('%H:%M')}"


@dataclass(slots=True)
class Settings:
    bot_token: str
    perplexity_api_token: str
    perplexity_model: str
    schedule_day: int
    schedule_time: time
    timezone: timezone

    @classmethod
    def from_env(cls) -> "Settings":
        env = load_env(ENV_PATH)
        bot_token = env.get("BOT_TOKEN", "")
        perplexity_api_token = env.get("PERPLEXITY_API_TOKEN", "")
        if not bot_token or not perplexity_api_token:
            raise RuntimeError("BOT_TOKEN и PERPLEXITY_API_TOKEN должны быть заданы в .env")

        model = env.get("PERPLEXITY_MODEL", "sonar-pro")
        schedule_day = int(env.get("WEEKLY_SEND_DAY", "0"))
        schedule_time = datetime.strptime(env.get("WEEKLY_SEND_TIME", "09:00"), "%H:%M").time()
        timezone_name = env.get("TZ", "Europe/Moscow")
        timezone_value = resolve_timezone(timezone_name)
        return cls(
            bot_token=bot_token,
            perplexity_api_token=perplexity_api_token,
            perplexity_model=model,
            schedule_day=schedule_day,
            schedule_time=schedule_time,
            timezone=timezone_value,
        )


def resolve_timezone(timezone_name: str) -> timezone:
    normalized = timezone_name.strip()
    if normalized in {"Europe/Moscow", "MSK", "UTC+3", "+03:00", "+03"}:
        return timezone(timedelta(hours=3))
    if normalized in {"UTC", "Etc/UTC", "+00:00", "+00"}:
        return timezone.utc

    sign = 1
    raw = normalized
    if raw.startswith("UTC"):
        raw = raw[3:]
    if raw.startswith("+"):
        raw = raw[1:]
    elif raw.startswith("-"):
        sign = -1
        raw = raw[1:]

    if ":" in raw:
        hours_text, minutes_text = raw.split(":", 1)
    elif raw.isdigit():
        hours_text, minutes_text = raw, "0"
    else:
        return timezone.utc

    try:
        hours = int(hours_text)
        minutes = int(minutes_text)
    except ValueError:
        return timezone.utc

    offset = timedelta(hours=hours, minutes=minutes) * sign
    return timezone(offset)


class ChatRepository:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS chats (
                    chat_id INTEGER PRIMARY KEY,
                    created_at TEXT NOT NULL
                )
                """
            )

    def add_chat(self, chat_id: int) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO chats (chat_id, created_at)
                VALUES (?, ?)
                ON CONFLICT(chat_id) DO NOTHING
                """,
                (chat_id, datetime.utcnow().isoformat(timespec="seconds")),
            )

    def get_all_chat_ids(self) -> list[int]:
        with self._connect() as connection:
            rows = connection.execute("SELECT chat_id FROM chats ORDER BY created_at ASC").fetchall()
        return [int(row["chat_id"]) for row in rows]


class TelegramBotAPI:
    def __init__(self, token: str) -> None:
        self.token = token

    def _request(self, method: str, payload: dict | None = None) -> dict:
        url = TELEGRAM_API_URL.format(token=self.token, method=method)
        body = None
        headers = {}
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"

        http_request = request.Request(url, data=body, headers=headers, method="POST" if payload else "GET")
        try:
            with request.urlopen(http_request, timeout=60) as response:
                data = json.loads(response.read().decode("utf-8"))
        except error.HTTPError as exc:
            details = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Telegram API error {exc.code}: {details}") from exc
        except error.URLError as exc:
            raise RuntimeError(f"Telegram network error: {exc}") from exc

        if not data.get("ok"):
            raise RuntimeError(f"Telegram API returned error: {data}")
        return data["result"]

    async def get_updates(self, offset: int | None = None, timeout: int = 30) -> list[dict]:
        params = {"timeout": timeout}
        if offset is not None:
            params["offset"] = offset
        query = parse.urlencode(params)
        url = TELEGRAM_API_URL.format(token=self.token, method=f"getUpdates?{query}")
        http_request = request.Request(url, method="GET")

        def _perform() -> list[dict]:
            try:
                with request.urlopen(http_request, timeout=timeout + 10) as response:
                    data = json.loads(response.read().decode("utf-8"))
            except error.HTTPError as exc:
                details = exc.read().decode("utf-8", errors="replace")
                raise RuntimeError(f"Telegram polling error {exc.code}: {details}") from exc
            except error.URLError as exc:
                raise RuntimeError(f"Telegram polling network error: {exc}") from exc
            if not data.get("ok"):
                raise RuntimeError(f"Telegram polling returned error: {data}")
            return data["result"]

        return await asyncio.to_thread(_perform)

    async def send_text(self, chat_id: int, text: str) -> None:
        for chunk in split_message(text):
            await asyncio.to_thread(
                self._request,
                "sendMessage",
                {"chat_id": chat_id, "text": chunk, "disable_web_page_preview": True},
            )


class PerplexityClient:
    def __init__(self, api_token: str, model: str) -> None:
        self.api_token = api_token
        self.model = model

    async def ask(self, prompt: str) -> str:
        payload = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": "Ты готовишь аккуратную еженедельную новостную сводку на русском языке.",
                },
                {"role": "user", "content": prompt},
            ],
        }

        def _perform() -> str:
            http_request = request.Request(
                PERPLEXITY_URL,
                data=json.dumps(payload).encode("utf-8"),
                headers={
                    "Authorization": f"Bearer {self.api_token}",
                    "Content-Type": "application/json",
                },
                method="POST",
            )
            try:
                with request.urlopen(http_request, timeout=180) as response:
                    data = json.loads(response.read().decode("utf-8"))
            except error.HTTPError as exc:
                details = exc.read().decode("utf-8", errors="replace")
                raise RuntimeError(f"Perplexity API error {exc.code}: {details}") from exc
            except error.URLError as exc:
                raise RuntimeError(f"Perplexity network error: {exc}") from exc

            try:
                return data["choices"][0]["message"]["content"].strip()
            except (KeyError, IndexError, TypeError) as exc:
                raise RuntimeError(f"Unexpected Perplexity response: {data}") from exc

        return await asyncio.to_thread(_perform)


class WeeklyDigestService:
    def __init__(self, prompts_dir: Path, perplexity_client: PerplexityClient) -> None:
        self.prompts_dir = prompts_dir
        self.perplexity_client = perplexity_client
        self._generation_lock = asyncio.Lock()

    def _iter_prompt_files(self) -> Iterable[Path]:
        return sorted(self.prompts_dir.glob("*.txt"))

    async def generate_digest(self) -> str:
        prompt_files = list(self._iter_prompt_files())
        if not prompt_files:
            raise RuntimeError("В папке prompts не найдено ни одного .txt файла")

        async with self._generation_lock:
            sections: list[str] = []
            for prompt_file in prompt_files:
                prompt_text = read_prompt(prompt_file)
                answer = await self.perplexity_client.ask(prompt_text)
                sections.append(f"{prompt_file.stem.upper()}\n\n{answer}")

        timestamp = datetime.now().strftime("%d.%m.%Y %H:%M")
        return f"Еженедельная сводка\nСформировано: {timestamp}\n\n" + "\n\n".join(sections)


class WeeklyBotApp:
    def __init__(
        self,
        settings: Settings,
        repository: ChatRepository,
        telegram_api: TelegramBotAPI,
        digest_service: WeeklyDigestService,
    ) -> None:
        self.settings = settings
        self.repository = repository
        self.telegram_api = telegram_api
        self.digest_service = digest_service
        self._update_offset: int | None = None

    async def run(self) -> None:
        await asyncio.gather(
            self._poll_updates_forever(),
            self._weekly_scheduler_forever(),
        )

    async def _poll_updates_forever(self) -> None:
        while True:
            try:
                updates = await self.telegram_api.get_updates(offset=self._update_offset, timeout=30)
                for update in updates:
                    self._update_offset = update["update_id"] + 1
                    await self._handle_update(update)
            except Exception as exc:
                print(f"[polling] {exc}", file=sys.stderr)
                await asyncio.sleep(5)

    async def _handle_update(self, update: dict) -> None:
        message = update.get("message") or update.get("edited_message")
        if not message:
            return

        chat = message.get("chat") or {}
        chat_id = chat.get("id")
        text = (message.get("text") or "").strip()
        if not chat_id or not text:
            return

        command = text.split()[0].split("@")[0].lower()
        if command == "/start":
            await self._handle_start(chat_id)
        elif command == "/weekly":
            await self._handle_weekly(chat_id)

    async def _handle_start(self, chat_id: int) -> None:
        self.repository.add_chat(chat_id)
        schedule_text = format_schedule(self.settings.schedule_day, self.settings.schedule_time)
        await self.telegram_api.send_text(
            chat_id,
            "Чат зарегистрирован для еженедельной рассылки.\n"
            f"Автоматическая отправка: {schedule_text}.\n"
            "Команда /weekly запускает сборку сводки сразу.",
        )

    async def _handle_weekly(self, chat_id: int) -> None:
        await self.telegram_api.send_text(chat_id, "Собираю свежую еженедельную сводку, это может занять пару минут.")
        await self._send_digest_to_chat(chat_id)

    async def _send_digest_to_chat(self, chat_id: int) -> None:
        try:
            digest = await self.digest_service.generate_digest()
            await self.telegram_api.send_text(chat_id, digest)
        except Exception as exc:
            await self.telegram_api.send_text(chat_id, f"Не удалось получить сводку: {exc}")

    def _next_run_at(self) -> datetime:
        now = datetime.now(self.settings.timezone)
        target = datetime.combine(now.date(), self.settings.schedule_time, tzinfo=self.settings.timezone)
        days_ahead = (self.settings.schedule_day - now.weekday()) % 7
        if days_ahead == 0 and target <= now:
            days_ahead = 7
        return target + timedelta(days=days_ahead)

    async def _weekly_scheduler_forever(self) -> None:
        while True:
            next_run = self._next_run_at()
            delay = max((next_run - datetime.now(self.settings.timezone)).total_seconds(), 1)
            print(f"[scheduler] Next run at {next_run.isoformat()}")
            await asyncio.sleep(delay)
            await self._broadcast_digest()

    async def _broadcast_digest(self) -> None:
        chat_ids = self.repository.get_all_chat_ids()
        if not chat_ids:
            print("[scheduler] No registered chats yet")
            return

        try:
            digest = await self.digest_service.generate_digest()
        except Exception as exc:
            print(f"[scheduler] Failed to generate digest: {exc}", file=sys.stderr)
            return

        for chat_id in chat_ids:
            try:
                await self.telegram_api.send_text(chat_id, digest)
            except Exception as exc:
                print(f"[scheduler] Failed to send digest to {chat_id}: {exc}", file=sys.stderr)


async def main() -> None:
    settings = Settings.from_env()
    repository = ChatRepository(DB_PATH)
    telegram_api = TelegramBotAPI(settings.bot_token)
    perplexity_client = PerplexityClient(settings.perplexity_api_token, settings.perplexity_model)
    digest_service = WeeklyDigestService(PROMPTS_DIR, perplexity_client)
    app = WeeklyBotApp(settings, repository, telegram_api, digest_service)
    await app.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Bot stopped")
