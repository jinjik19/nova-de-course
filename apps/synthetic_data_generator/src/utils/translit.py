TRANSLIT_MAP = {
    'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'yo',
    'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm',
    'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u',
    'ф': 'f', 'х': 'h', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'sch', 'ъ': '',
    'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya'
}


def transliterate(text: str) -> str:
    """Transliterates Cyrillic text to Latin."""
    result = []

    for char in text.lower():
        if char in TRANSLIT_MAP:
            result.append(TRANSLIT_MAP[char])

        elif 'a' <= char <= 'z' or '0' <= char <= '9':
            result.append(char)

        elif char == ' ':
            result.append('.')

    return "".join(result)
