import re

def regex_checks(url):
    # Exclude URLs with dates
    if (re.search(r"(?:\d{1,2}[-/]\d{1,2}[-/]\d{2,4})") or
        re.search(r"?:\d{4}[-/]\d{1,2}[-/]\d{1,2})")    or
        re.search(r"(?:\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s\d{1,2},\s\d{4})")
        ):
        return False
    else:
        return True