import hashlib
from arbitrage_bot.services.normalizer import NormalizerService
from arbitrage_bot.models.orm import MarketPair


class MatcherService:
    def __init__(self, db_session):
        self.db = db_session
        self.normalizer = NormalizerService()
        self.stop_words = {
            "the", "a", "an", "on", "in", "at", "for", "to", "of", "and", "or",
            "will", "be", "is", "are", "was", "were", "with", "by"
        }


    def _calculate_hash(self, id_a, id_b):
        sorted_ids = sorted([str(id_a), str(id_b)])
        return hashlib.sha256(f"{sorted_ids[0]}_{sorted_ids[1]}".encode()).hexdigest()


    def match_candidates(self, poly_market, pf_market):
        poly_norm = self.normalizer.normalize_text(poly_market.title)
        pf_norm = self.normalizer.normalize_text(pf_market.title)

        poly_entities = self.normalizer.extract_entities(poly_market.title)
        pf_entities = self.normalizer.extract_entities(pf_market.title)

        # strict date heuristic
        if poly_entities["dates"] and pf_entities["dates"]:
            if set(poly_entities["dates"]) != set(pf_entities["dates"]):
                return None

        if poly_entities["numbers"] and pf_entities["numbers"]:
            if set(poly_entities["numbers"]) != set(pf_entities["numbers"]):
                return None

        # jaccard index for basic text comparison 
        poly_words = self._tokenize(poly_norm)
        pf_words = self._tokenize(pf_norm)

        if not poly_words or not pf_words:
            return None

        intersection = len(poly_words.intersection(pf_words))
        union = len(poly_words.union(pf_words))
        score = intersection / union

        status = "candidate"
        if score >= 0.85:
            status = "auto_approved"
        elif score >= 0.65:
            status = "manual_review"
        else:
            return None

        pair_hash = self._calculate_hash(poly_market.id, pf_market.id)

        pair = MarketPair(
            market_id_a=poly_market.id,
            market_id_b=pf_market.id,
            pair_hash=pair_hash,
            status=status,
            match_score=score,
            match_reason_json={"poly_title": poly_market.title, "pf_title": pf_market.title}
        )

        return pair


    def _tokenize(self, normalized_text):
        return {
            word for word in normalized_text.split()
            if len(word) > 1 and word not in self.stop_words
        }
