"""
themes.py
---------
Global Intel Bot visual theme + Telegram custom emoji integration.

The bot uses the published Telegram custom emoji pack:
    GlobalIntelNews

This module contains no Telegram/network initialization at import time.
The custom emoji IDs are loaded by bot.py at startup.
"""
from __future__ import annotations
import html
from pathlib import Path
from typing import Any, Final

BASE_DIR: Final[Path] = Path(__file__).resolve().parent
THEME_ASSETS_DIR: Final[Path] = BASE_DIR / "assets" / "themes"
CUSTOM_EMOJI_PACK: Final[str] = "GlobalIntelNews"

THEME_EMOJIS: Final[dict[str, str]] = {
    "urgent": "🚨", "monitoring": "📡", "search": "🔎", "analysis": "🧠",
    "done": "✅", "world": "🌍", "economy": "📈", "defense": "🛡️",
    "official": "🏛️", "warning": "⚠️",
}

THEMES: Final[dict[str, str]] = {
    "urgent": "🚨 عاجل", "monitoring": "📡 جاري الرصد", "search": "🔎 جاري البحث",
    "analysis": "🧠 جاري التحليل", "done": "✅ اكتمل", "warning": "⚠️ تحذير",
    "world": "🌍 العالم", "economy": "📈 الاقتصاد والأسواق",
    "defense": "🛡 الدفاع والأمن", "official": "🏛 البيانات الرسمية",
}

THEME_FILES: Final[dict[str, str]] = {
    "urgent": "urgent.webm", "monitoring": "monitoring.webm", "search": "search.webm",
    "analysis": "analysis.webm", "done": "done.webm", "warning": "warning.webm",
    "world": "world.webm", "economy": "economy.webm", "defense": "defense.webm",
    "official": "official.webm",
}

def theme_label(theme: str) -> str:
    return THEMES.get(theme, theme)

def theme_emoji(theme: str) -> str:
    return THEME_EMOJIS.get(theme, "🌐")

def theme_path(theme: str) -> Path:
    filename = THEME_FILES.get(theme)
    if not filename:
        raise KeyError(f"Unknown theme: {theme}")
    return THEME_ASSETS_DIR / filename

def theme_exists(theme: str) -> bool:
    return theme_path(theme).is_file()

def available_themes() -> list[str]:
    return [theme for theme in THEMES if theme_exists(theme)]

STATUS_THEMES: Final[dict[str, str]] = {
    "urgent": "urgent", "monitoring": "monitoring", "search": "search",
    "analysis": "analysis", "done": "done", "warning": "warning",
    "world": "world", "economy": "economy", "defense": "defense", "official": "official",
}

def status_theme(status: str) -> str:
    return STATUS_THEMES.get(status, "monitoring")

TOPIC_THEMES: Final[dict[str, str]] = {
    "economy_energy": "economy", "official": "official", "urgent": "urgent",
    "middle_east": "world", "world": "world", "security": "defense",
}

def topic_theme(topic: str) -> str:
    return TOPIC_THEMES.get(topic, "world")

def _sticker_attr(sticker: Any, name: str, default: Any = None) -> Any:
    if isinstance(sticker, dict):
        return sticker.get(name, default)
    return getattr(sticker, name, default)

def _emoji_matches(actual: str, expected: str) -> bool:
    return str(actual or "").replace("\\ufe0f", "") == str(expected or "").replace("\\ufe0f", "")

async def load_custom_emoji_ids(bot: Any, pack_name: str = CUSTOM_EMOJI_PACK) -> dict[str, str]:
    sticker_set = await bot.get_sticker_set(pack_name)
    stickers = _sticker_attr(sticker_set, "stickers", []) or []
    result: dict[str, str] = {}
    for sticker in stickers:
        emoji = _sticker_attr(sticker, "emoji", "") or ""
        custom_id = _sticker_attr(sticker, "custom_emoji_id", None)
        if not custom_id:
            continue
        for theme, fallback in THEME_EMOJIS.items():
            if _emoji_matches(emoji, fallback):
                result[theme] = str(custom_id)
                break
    return result


async def load_custom_emoji_sticker_ids(bot: Any, pack_name: str = CUSTOM_EMOJI_PACK) -> dict[str, str]:
    """Return Telegram file_ids for the published video custom-emoji stickers."""
    sticker_set = await bot.get_sticker_set(pack_name)
    stickers = _sticker_attr(sticker_set, "stickers", []) or []
    result: dict[str, str] = {}
    for sticker in stickers:
        emoji = _sticker_attr(sticker, "emoji", "") or ""
        file_id = _sticker_attr(sticker, "file_id", None)
        if not file_id:
            continue
        for theme, fallback in THEME_EMOJIS.items():
            if _emoji_matches(emoji, fallback):
                result[theme] = str(file_id)
                break
    return result


def custom_emoji_html(custom_emoji_id: str | None, fallback_emoji: str) -> str:
    fallback = str(fallback_emoji or "🌐")
    if not custom_emoji_id:
        return fallback
    return (f'<tg-emoji emoji-id="{html.escape(str(custom_emoji_id), quote=True)}">'
            f'{html.escape(fallback)}</tg-emoji>')

def theme_file(theme: str) -> str:
    return str(theme_path(theme))

def theme_bytes(theme: str) -> bytes:
    return theme_path(theme).read_bytes()

__all__ = [
    "CUSTOM_EMOJI_PACK", "THEMES", "THEME_FILES", "THEME_EMOJIS", "THEME_ASSETS_DIR",
    "STATUS_THEMES", "TOPIC_THEMES", "theme_label", "theme_emoji", "theme_path",
    "theme_exists", "available_themes", "status_theme", "topic_theme",
    "load_custom_emoji_ids", "load_custom_emoji_sticker_ids", "custom_emoji_html", "theme_file", "theme_bytes",
]
