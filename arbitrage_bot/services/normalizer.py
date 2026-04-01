import re


class NormalizerService:
    def normalize_text(self, text):
        if not text:
            return ""
        text = str(text).lower()
        text = re.sub(r'[^a-z0-9\s]', '', text)
        return " ".join(text.split())


    def extract_entities(self, text):
        text = str(text).lower()

        # extract dates from text
        date_pattern = r'(?:january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{1,2}(?:,\s*\d{4})?'
        dates = re.findall(date_pattern, text)

        # remove matched date substrings before extracting standalone numbers
        text_without_dates = re.sub(date_pattern, '', text)
        numbers = re.findall(r'\d+(?:\.\d+)?', text_without_dates)

        return {
            "dates": dates,
            "numbers": numbers
        }


    def normalize_outcome_label(self, value):
        normalized = str(value or "").strip().lower()
        if normalized in {"yes", "y"}:
            return "yes"
        if normalized in {"no", "n"}:
            return "no"
        if normalized in {"draw", "tie"}:
            return "draw"
        return normalized