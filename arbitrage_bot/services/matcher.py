import hashlib
from arbitrage_bot.services.normalizer import NormalizerService
from arbitrage_bot.models.orm import MarketPair


class MatcherService:


    def __init__(self, db_session=None):
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
        outcome_mapping = self._build_outcome_mapping(poly_market, pf_market)

        status = "candidate"
        if score >= 0.85 and poly_words == pf_words and outcome_mapping:
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
            match_reason_json={"poly_title": poly_market.title, "pf_title": pf_market.title},
            outcome_mapping_json=outcome_mapping,
        )

        return pair


    def _normalize_outcome_label(self, value):
        normalized = str(value or "").strip().lower()
        if normalized in {"yes", "y"}:
            return "yes"
        if normalized in {"no", "n"}:
            return "no"
        return normalized


    def _extract_binary_outcomes(self, market):
        outcomes = getattr(market, "outcomes_json", None) or []
        mapping = {}

        for outcome in outcomes:
            if not isinstance(outcome, dict):
                continue

            key = self._normalize_outcome_label(
                outcome.get("slug") or outcome.get("label")
            )
            if key not in {"yes", "no"} or key in mapping:
                continue

            mapping[key] = {
                "id": str(outcome.get("id", key)),
                "label": str(outcome.get("label") or key),
                "slug": key,
            }

        if set(mapping.keys()) != {"yes", "no"}:
            return None

        return mapping


    def _build_outcome_mapping(self, poly_market, pf_market):
        poly_outcomes = self._extract_binary_outcomes(poly_market)
        pf_outcomes = self._extract_binary_outcomes(pf_market)
        if not poly_outcomes or not pf_outcomes:
            return None

        return {
            "market_a": {
                "yes": poly_outcomes["yes"]["id"],
                "no": poly_outcomes["no"]["id"],
            },
            "market_b": {
                "yes": pf_outcomes["yes"]["id"],
                "no": pf_outcomes["no"]["id"],
            },
            "is_inverted": False,
            "confidence": "high",
        }


    def _tokenize(self, normalized_text):
        return {
            word for word in normalized_text.split()
            if len(word) > 1 and word not in self.stop_words
        }


    def build_market_signature(self, market):
        normalized_title = self.normalizer.normalize_text(market.title)
        return {
            "market": market,
            "tokens": self._tokenize(normalized_title),
        }


    def build_candidate_index(self, markets):
        index = {}

        for market in markets:
            signature = self.build_market_signature(market)
            for token in signature["tokens"]:
                index.setdefault(token, []).append(signature)

        return index