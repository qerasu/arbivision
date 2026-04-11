import re


class NormalizerService:
    month_aliases = {
        "jan": "january",
        "january": "january",
        "feb": "february",
        "february": "february",
        "mar": "march",
        "march": "march",
        "apr": "april",
        "april": "april",
        "may": "may",
        "jun": "june",
        "june": "june",
        "jul": "july",
        "july": "july",
        "aug": "august",
        "august": "august",
        "sep": "september",
        "sept": "september",
        "september": "september",
        "oct": "october",
        "october": "october",
        "nov": "november",
        "november": "november",
        "dec": "december",
        "december": "december",
    }
    month_pattern = r"(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t|tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"


    def normalize_text(self, text):
        if not text:
            return ""
        text = str(text).lower()
        text = re.sub(r'[^a-z0-9\s]', '', text)
        return " ".join(text.split())


    def _canonical_month(self, value):
        normalized = str(value or "").strip().lower().rstrip(".")
        return self.month_aliases.get(normalized, normalized)


    def _extract_date_matches(self, text):
        matches = []
        occupied_spans = []

        def is_available(start, end):
            return all(end <= left or start >= right for left, right in occupied_spans)

        def add_match(match, canonical_value):
            start, end = match.span()
            if not is_available(start, end):
                return

            occupied_spans.append((start, end))
            matches.append(
                {
                    "span": (start, end),
                    "value": canonical_value,
                }
            )

        full_date_pattern = re.compile(
            rf"\b({self.month_pattern})\.?\s+(\d{{1,2}})(?:st|nd|rd|th)?(?:,\s*(\d{{4}}))?\b"
        )
        month_year_pattern = re.compile(
            rf"\b({self.month_pattern})\.?\s+(\d{{4}})\b"
        )
        month_only_pattern = re.compile(
            rf"\b({self.month_pattern})\b\.?"
        )

        for match in full_date_pattern.finditer(text):
            month = self._canonical_month(match.group(1))
            day = str(int(match.group(2)))
            year = match.group(3)
            canonical = f"{month} {day}"
            if year:
                canonical = f"{canonical} {year}"
            add_match(match, canonical)

        for match in month_year_pattern.finditer(text):
            month = self._canonical_month(match.group(1))
            year = match.group(2)
            add_match(match, f"{month} {year}")

        for match in month_only_pattern.finditer(text):
            month = self._canonical_month(match.group(1))
            add_match(match, month)

        return sorted(matches, key=lambda item: item["span"][0])


    def extract_entities(self, text):
        text = str(text or "").lower()
        date_matches = self._extract_date_matches(text)
        dates = [match["value"] for match in date_matches]

        text_chars = list(text)
        for match in date_matches:
            start, end = match["span"]
            for index in range(start, end):
                text_chars[index] = " "

        text_without_dates = "".join(text_chars)
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