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
        dates = re.findall(
            r'(?:january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{1,2}(?:,\s*\d{4})?',
            text
        )
        
        # extract numbers and thresholds
        numbers = re.findall(r'\d+(?:\.\d+)?', text)

        return {
            "dates": dates,
            "numbers": numbers
        }
