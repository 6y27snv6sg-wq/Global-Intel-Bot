from pathlib import Path
import zipfile, base64

zip_path = Path("/mnt/data/news_bot_sticker_pack_prototype.zip")
out_path = Path("/mnt/data/themes_fixed.py")

selected = {
    "urgent": "01_urgent.webm",
    "monitoring": "02_monitoring.webm",
    "search": "03_search.webm",
    "analysis": "04_analysis.webm",
    "done": "05_complete.webm",
    "warning": "11_warning.webm",
    "world": "06_world.webm",
    "economy": "08_economy.webm",
    "defense": "09_security.webm",
    "official": "10_official.webm",
}

with zipfile.ZipFile(zip_path) as z:
    encoded = {k: base64.b64encode(z.read(v)).decode("ascii") for k, v in selected.items()}

parts = ["""# themes.py — النسخة النهائية المستقلة
from pathlib import Path
from typing import Final
import base64
import tempfile

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

STATUS_THEMES: Final[dict[str, str]] = {
    "urgent": "urgent", "monitoring": "monitoring", "search": "search",
    "analysis": "analysis", "done": "done", "warning": "warning",
    "world": "world", "economy": "economy", "defense": "defense", "official": "official",
}

TOPIC_THEMES: Final[dict[str, str]] = {
    "economy_energy": "economy", "official": "official", "urgent": "urgent",
    "middle_east": "world", "world": "world", "security": "defense",
}

_THEME_DATA: Final[dict[str, str]] = {
"""]
for k, v in encoded.items():
    parts.append(f'    "{k}": "{v}",\n')
parts.append("""}

_CACHE_DIR = Path(tempfile.gettempdir()) / "global_intel_bot_themes"
_CACHE_DIR.mkdir(parents=True, exist_ok=True)

def theme_label(theme: str) -> str:
    return THEMES.get(theme, theme)

def theme_path(theme: str) -> Path:
    filename = THEME_FILES.get(theme)
    if not filename:
        raise KeyError(f"الثيم غير معروف: {theme}")
    return _CACHE_DIR / filename

def theme_exists(theme: str) -> bool:
    return theme in _THEME_DATA

def available_themes() -> list[str]:
    return list(THEMES.keys())

def _ensure_theme_file(theme: str) -> Path | None:
    data = _THEME_DATA.get(theme)
    if data is None:
        return None
    path = theme_path(theme)
    if not path.exists() or path.stat().st_size == 0:
        path.write_bytes(base64.b64decode(data))
    return path

def theme_file(theme: str) -> Path | None:
    return _ensure_theme_file(theme)

def status_theme(status: str) -> str:
    return STATUS_THEMES.get(status, "monitoring")

def topic_theme(topic: str) -> str:
    return TOPIC_THEMES.get(topic, "world")

def theme_bytes(theme: str) -> bytes | None:
    data = _THEME_DATA.get(theme)
    return base64.b64decode(data) if data is not None else None

__all__ = [
    "THEMES", "THEME_FILES", "STATUS_THEMES", "TOPIC_THEMES",
    "theme_label", "theme_path", "theme_exists", "available_themes",
    "theme_file", "status_theme", "topic_theme", "theme_bytes",
]
""")

content = "".join(parts)
compile(content, str(out_path), "exec")
out_path.write_text(content, encoding="utf-8")

ns = {}
exec(compile(content, str(out_path), "exec"), ns)
sizes = {k: ns["theme_file"](k).stat().st_size for k in selected}
print("تم إنشاء النسخة النهائية.")
print(f"الحجم: {out_path.stat().st_size:,} bytes")
print("Syntax: OK")
print("theme_file: OK")
print("10/10 themes:", all(v > 0 for v in sizes.values()))
