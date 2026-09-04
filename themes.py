
'''"""
themes.py
---------
إدارة الهوية البصرية والثيمات المتحركة لبوت الأخبار.

الملف مستقل عن bot.py و news_engine.py.
يمكن تعديل أسماء/ملفات الثيمات هنا دون تغيير منطق البوت.

ملاحظة:
- ملفات الوسائط نفسها توضع لاحقًا بجانب المشروع أو في مجلد assets/ حسب طريقة
  رفعها إلى Telegram.
- هذا الملف لا يرسل أي شيء إلى Telegram بمفرده؛ bot.py هو المسؤول عن الإرسال.
"""

from pathlib import Path
from typing import Final


# ============================================================
# المسارات
# ============================================================

BASE_DIR: Final[Path] = Path(__file__).resolve().parent
THEME_ASSETS_DIR: Final[Path] = BASE_DIR / "assets" / "themes"


# ============================================================
# أسماء الثيمات
# ============================================================

THEMES: Final[dict[str, str]] = {
    "urgent": "🚨 عاجل",
    "monitoring": "📡 جاري الرصد",
    "search": "🔎 جاري البحث",
    "analysis": "🧠 جاري التحليل",
    "done": "✅ اكتمل",
    "warning": "⚠️ تحذير",
    "world": "🌍 العالم",
    "economy": "📈 الاقتصاد والأسواق",
    "defense": "🛡 الدفاع والأمن",
    "official": "🏛 البيانات الرسمية",
}


# ============================================================
# أسماء ملفات الوسائط
# ============================================================

THEME_FILES: Final[dict[str, str]] = {
    "urgent": "urgent.webm",
    "monitoring": "monitoring.webm",
    "search": "search.webm",
    "analysis": "analysis.webm",
    "done": "done.webm",
    "warning": "warning.webm",
    "world": "world.webm",
    "economy": "economy.webm",
    "defense": "defense.webm",
    "official": "official.webm",
}


# ============================================================
# أدوات الوصول
# ============================================================

def theme_label(theme: str) -> str:
    """إرجاع اسم الثيم الظاهر للمستخدم."""
    return THEMES.get(theme, theme)


def theme_path(theme: str) -> Path:
    """إرجاع المسار المتوقع لملف الثيم المتحرك."""
    filename = THEME_FILES.get(theme)
    if not filename:
        raise KeyError(f"الثيم غير معروف: {theme}")
    return THEME_ASSETS_DIR / filename


def theme_exists(theme: str) -> bool:
    """التحقق من وجود ملف الثيم محليًا."""
    return theme_path(theme).is_file()


def available_themes() -> list[str]:
    """إرجاع الثيمات التي لها ملفات وسائط موجودة فعليًا."""
    return [
        theme
        for theme in THEMES
        if theme_exists(theme)
    ]


# ============================================================
# حالات البوت → الثيم المناسب
# ============================================================

STATUS_THEMES: Final[dict[str, str]] = {
    "urgent": "urgent",
    "monitoring": "monitoring",
    "search": "search",
    "analysis": "analysis",
    "done": "done",
    "warning": "warning",
    "world": "world",
    "economy": "economy",
    "defense": "defense",
    "official": "official",
}


def status_theme(status: str) -> str:
    """تحويل حالة داخلية إلى اسم الثيم."""
    return STATUS_THEMES.get(status, "monitoring")


# ============================================================
# تصنيف الأقسام الرئيسية
# ============================================================

TOPIC_THEMES: Final[dict[str, str]] = {
    "economy_energy": "economy",
    "official": "official",
    "urgent": "urgent",
    "middle_east": "world",
    "world": "world",
    "security": "defense",
}


def topic_theme(topic: str) -> str:
    """إرجاع الثيم المرتبط بالقسم."""
    return TOPIC_THEMES.get(topic, "world")


__all__ = [
    "THEMES",
    "THEME_FILES",
    "THEME_ASSETS_DIR",
    "STATUS_THEMES",
    "TOPIC_THEMES",
    "theme_label",
    "theme_path",
    "theme_exists",
    "available_themes",
    "status_theme",
    "topic_theme",
]
'''

path = Path("/mnt/data/themes.py")
path.write_text(themes_py, encoding="utf-8")

# فحص نحوي
compile(themes_py, str(path), "exec")

print(f"تم إنشاء themes.py بنجاح: {len(themes_py.splitlines())} سطر")
print("الفحص النحوي: OK")
