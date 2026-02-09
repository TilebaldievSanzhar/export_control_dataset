"""
Шаблоны для определения необходимости лицензии.
Заполняется вручную на основе анализа документов.
"""

import re
from typing import Optional

# Паттерны, указывающие что лицензия НЕ нужна
LICENSE_NOT_REQUIRED_PATTERNS = [
    r"не\s+требуется\s+получение\s+лицензии",
    r"не\s+требуется\s+получение\s+разрешения",
    r"не\s+числится\s+в\s+лицензируемых\s+товарах",
    r"не\s+возражает\s+против\s+ввоза",
    r"не\s+подлежит\s+лицензированию",
    r"не\s+подлежит\s+экспортному\s+контролю",
    r"лицензия\s+не\s+требуется",
    r"разрешение\s+не\s+требуется",
    r"не\s+включен[оа]?\s+в\s+(?:контрольные?\s+)?списки?",
    r"не\s+относится\s+к\s+контролируемым",
]

# Паттерны, указывающие что лицензия НУЖНА
LICENSE_REQUIRED_PATTERNS = [
    r"согласовывает\s+выдачу",
    r"положительное\s+экспертное\s+заключение",
    r"выдано.*заключение.*разрешительный\s+документ",
    r"требуется\s+(?:получение\s+)?лицензи[яию]",
    r"подлежит\s+лицензированию",
    r"подлежит\s+экспортному\s+контролю",
    r"включен[оа]?\s+в\s+(?:контрольные?\s+)?списки?",
    r"относится\s+к\s+контролируемым",
    r"необходимо\s+(?:получить\s+)?разрешение",
    r"выдать\s+лицензию",
]

# Приоритет источников (от высшего к низшему)
SOURCE_PRIORITY = [
    "license_text",   # Заключения имеют высший приоритет
    "permit_text",    # Затем разъяснения/экспертные заключения
]


def _check_patterns(text: str, patterns: list[str]) -> bool:
    """Check if any pattern matches the text."""
    if not isinstance(text, str) or not text:
        return False
    text_lower = text.lower()
    for pattern in patterns:
        if re.search(pattern, text_lower, re.IGNORECASE | re.UNICODE):
            return True
    return False


def determine_license_need(
    permit_text: Optional[str],
    license_text: Optional[str]
) -> Optional[bool]:
    """
    Определяет необходимость лицензии на основе текстов документов.

    Args:
        permit_text: Текст из документов permit/
        license_text: Текст из документов license/

    Returns:
        True - лицензия нужна
        False - лицензия не нужна
        None - не удалось определить
    """
    # Проверяем источники в порядке приоритета
    for source_name in SOURCE_PRIORITY:
        text = license_text if source_name == "license_text" else permit_text

        if not isinstance(text, str) or not text:
            continue

        # Проверяем паттерны "лицензия нужна"
        if _check_patterns(text, LICENSE_REQUIRED_PATTERNS):
            return True

        # Проверяем паттерны "лицензия не нужна"
        if _check_patterns(text, LICENSE_NOT_REQUIRED_PATTERNS):
            return False

    # Не удалось определить
    return None
