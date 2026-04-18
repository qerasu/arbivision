import hashlib
import re
from arbitrage_bot.services.normalizer import NormalizerService
from arbitrage_bot.models.orm import MarketPair


class MatcherService:
    def __init__(self):
        self.normalizer = NormalizerService()
        self.max_ranked_candidates = 25
        self.stop_words = {
            "the", "a", "an", "on", "in", "at", "for", "to", "of", "and", "or",
            "will", "be", "is", "are", "was", "were", "with", "by"
        }
        self.entity_stop_words = {
            "vs", "versus", "v", "win", "wins", "beat", "beats", "defeat", "defeats",
            "make", "makes", "made", "qualify", "qualifies", "reach", "reaches",
            "end", "draw", "over", "under", "yes", "no", "game", "match", "series",
            "final", "finals", "playoff", "playoffs", "conference", "eastern",
            "western", "league", "cup", "trophy", "championship", "champions",
            "season", "round", "today", "tomorrow"
        }
        self.semantic_qualifiers = {
            # ordinals
            "first", "second", "third", "fourth", "fifth",
            "sixth", "seventh", "eighth", "ninth", "tenth",
            "1st", "2nd", "3rd", "4th", "5th",
            # rankings
            "largest", "biggest", "smallest", "highest", "lowest",
            "top", "bottom", "most", "least", "best", "worst",
            # gender
            "men", "women", "mens", "womens", "male", "female",
            "boys", "girls",
            # competition structure
            "team", "individual", "singles", "doubles",
            "junior", "senior", "amateur", "professional",
            "open", "pro",
        }
        self.generic_context_words = {
            "which", "what", "who",
            "company", "companies",
            "best", "top",
            "model", "models",
            "end", "ending",
            "have", "has", "had",
            "win", "wins", "winner", "winners",
            "price", "prices",
            "market", "markets",
            "ai",
        }
        self.generic_outcome_labels = {
            "up", "down", "over", "under",
            "higher", "lower", "above", "below",
            "increase", "decrease", "rise", "fall",
            "bull", "bear", "long", "short",
            "more", "less", "positive", "negative",
        }


    def _calculate_hash(self, id_a, id_b):
        sorted_ids = sorted([str(id_a), str(id_b)])
        return hashlib.sha256(f"{sorted_ids[0]}_{sorted_ids[1]}".encode()).hexdigest()


    def match_candidates(self, poly_market, pf_market, poly_signature=None, pf_signature=None):
        decision = self.explain_match(
            poly_market,
            pf_market,
            poly_signature=poly_signature,
            pf_signature=pf_signature,
        )
        
        if not decision["matched"]:
            return None

        pair_hash = self._calculate_hash(poly_market.id, pf_market.id)

        return MarketPair(
            market_id_a=poly_market.id,
            market_id_b=pf_market.id,
            pair_hash=pair_hash,
            status="auto_approved",
            match_score=decision["score"],
            match_reason_json=decision["reason"],
            outcome_mapping_json=decision["outcome_mapping"],
        )


    def explain_match(self, poly_market, pf_market, poly_signature=None, pf_signature=None):
        poly_signature = poly_signature or self.build_market_signature(poly_market)
        pf_signature = pf_signature or self.build_market_signature(pf_market)

        if (
            poly_signature["event_granularity"] != pf_signature["event_granularity"]
            and (
                poly_signature["event_granularity"] != "default"
                or pf_signature["event_granularity"] != "default"
            )
        ):
            return self._build_rejection(
                poly_market,
                pf_market,
                poly_signature,
                pf_signature,
                "event_granularity_mismatch",
            )

        if poly_signature["variant"] != pf_signature["variant"]:
            return self._build_rejection(
                poly_market,
                pf_market,
                poly_signature,
                pf_signature,
                "market_variant_mismatch",
            )

        if (
            poly_signature["scope"] != pf_signature["scope"]
            and (
                poly_signature["scope"] != "default"
                or pf_signature["scope"] != "default"
            )
        ):
            return self._build_rejection(
                poly_market,
                pf_market,
                poly_signature,
                pf_signature,
                "market_scope_mismatch",
            )

        if poly_signature["comparison_type"] != pf_signature["comparison_type"]:
            comparison_types = {
                poly_signature["comparison_type"],
                pf_signature["comparison_type"],
            }
            if "unknown" not in comparison_types:
                return self._build_rejection(
                    poly_market,
                    pf_market,
                    poly_signature,
                    pf_signature,
                    "comparison_mismatch",
                )

        direct_condition_match = self._has_direct_condition_match(poly_market, pf_market)
        if direct_condition_match:
            outcome_mapping = self._build_outcome_mapping(
                poly_market,
                pf_market,
                allow_fallback_order=True,
            )
            if outcome_mapping and self._has_compatible_direct_condition_context(
                poly_signature,
                pf_signature,
            ) and self._has_compatible_market_context(
                poly_signature,
                pf_signature,
            ):
                return {
                    "matched": True,
                    "score": 1.0,
                    "outcome_mapping": outcome_mapping,
                    "reason": {
                        "poly_title": poly_market.title,
                        "pf_title": pf_market.title,
                        "strategy": "condition_id",
                        "reject_reason": None,
                    },
                }
            if outcome_mapping:
                return self._build_rejection(
                    poly_market,
                    pf_market,
                    poly_signature,
                    pf_signature,
                    "direct_condition_context_mismatch",
                    outcome_mapping=outcome_mapping,
                )

        if not self._are_market_shapes_compatible(poly_signature, pf_signature):
            return self._build_rejection(
                poly_market,
                pf_market,
                poly_signature,
                pf_signature,
                "market_shape_mismatch",
            )

        if not self._has_compatible_matchup_participants(poly_signature, pf_signature):
            return self._build_rejection(
                poly_market,
                pf_market,
                poly_signature,
                pf_signature,
                "participant_mismatch",
            )

        # strict date heuristic
        if poly_signature["entities"]["dates"] and pf_signature["entities"]["dates"]:
            if set(poly_signature["entities"]["dates"]) != set(pf_signature["entities"]["dates"]):
                return self._build_rejection(
                    poly_market,
                    pf_market,
                    poly_signature,
                    pf_signature,
                    "date_mismatch",
                )

        if poly_signature["entities"]["numbers"] and pf_signature["entities"]["numbers"]:
            if set(poly_signature["entities"]["numbers"]) != set(pf_signature["entities"]["numbers"]):
                return self._build_rejection(
                    poly_market,
                    pf_market,
                    poly_signature,
                    pf_signature,
                    "number_mismatch",
                )

        if not self._has_compatible_market_context(poly_signature, pf_signature):
            return self._build_rejection(
                poly_market,
                pf_market,
                poly_signature,
                pf_signature,
                "market_context_mismatch",
            )

        # jaccard index for basic text comparison
        poly_words = poly_signature["title_tokens"]
        pf_words = pf_signature["title_tokens"]

        if not poly_words or not pf_words:
            return self._build_rejection(
                poly_market,
                pf_market,
                poly_signature,
                pf_signature,
                "empty_title_tokens",
            )

        intersection = len(poly_words.intersection(pf_words))
        union = len(poly_words.union(pf_words))
        title_score = intersection / union
        participant_score = self._calculate_participant_score(
            poly_signature["participants"],
            pf_signature["participants"],
        )
        outcome_mapping = self._build_outcome_mapping(poly_market, pf_market)
        score = max(
            title_score,
            (title_score * 0.45) + (participant_score * 0.55),
            participant_score,
        )

        if not self._should_auto_approve(
            poly_words,
            pf_words,
            poly_signature,
            pf_signature,
            outcome_mapping,
            score,
            participant_score,
        ):
            return self._build_rejection(
                poly_market,
                pf_market,
                poly_signature,
                pf_signature,
                "insufficient_confidence" if outcome_mapping else "missing_outcome_mapping",
                title_score=title_score,
                participant_score=participant_score,
                outcome_mapping=outcome_mapping,
            )

        return {
            "matched": True,
            "score": score,
            "outcome_mapping": outcome_mapping,
            "reason": {
                "poly_title": poly_market.title,
                "pf_title": pf_market.title,
                "title_score": round(title_score, 4),
                "participant_score": round(participant_score, 4),
                "poly_kind": poly_signature["kind"],
                "pf_kind": pf_signature["kind"],
                "reject_reason": None,
            },
        }


    def _should_auto_approve(self, poly_words, pf_words, poly_signature, pf_signature, outcome_mapping, score, participant_score):
        if not outcome_mapping:
            return False

        if self._has_meaningful_title_difference(poly_words, pf_words):
            return False

        if score >= 0.85 and poly_words == pf_words:
            return True

        if (
            participant_score >= 0.8
            and poly_signature["kind"] == "matchup"
            and pf_signature["kind"] == "matchup"
        ):
            return True

        # non-matchup: require minimum title overlap alongside participant score
        intersection = len(poly_words & pf_words)
        union = len(poly_words | pf_words)
        title_jaccard = intersection / union if union else 0.0

        if (
            participant_score >= 0.95
            and poly_signature["kind"] == pf_signature["kind"]
            and title_jaccard >= 0.5
        ):
            if poly_signature["kind"] == "matchup":
                return True

            return self._has_compatible_non_matchup_context(
                poly_signature,
                pf_signature,
            )

        return False


    def _has_compatible_non_matchup_context(self, poly_signature, pf_signature):
        poly_context_tokens = self._meaningful_non_participant_title_tokens(poly_signature)
        pf_context_tokens = self._meaningful_non_participant_title_tokens(pf_signature)

        if not poly_context_tokens or not pf_context_tokens:
            return False

        intersection = len(poly_context_tokens & pf_context_tokens)
        union = len(poly_context_tokens | pf_context_tokens)
        if union == 0:
            return False

        if (intersection / union) < 0.8:
            return False

        return not (
            poly_context_tokens - pf_context_tokens
            or pf_context_tokens - poly_context_tokens
        )


    def _non_participant_title_tokens(self, signature):
        participant_tokens = {
            token
            for participant in signature["participants"]
            for token in participant["tokens"]
        }
        return signature["title_tokens"] - participant_tokens


    def _meaningful_non_participant_title_tokens(self, signature):
        return {
            token
            for token in self._non_participant_title_tokens(signature)
            if token not in self.stop_words
            if token not in self.generic_context_words
        }


    def _meaningful_context_tokens(self, signature):
        participant_tokens = {
            token
            for participant in signature["participants"]
            for token in participant["tokens"]
        }
        return {
            token
            for token in self._context_tokens(signature)
            if token not in participant_tokens
            and token not in self.stop_words
            and token not in self.generic_context_words
        }


    def _has_meaningful_title_difference(self, poly_words, pf_words):
        diff = poly_words.symmetric_difference(pf_words)
        if not diff:
            return False

        for word in diff:
            if word in self.semantic_qualifiers:
                return True

            # catch compound ordinals like 'thirdlargest' after normalization
            for qualifier in self.semantic_qualifiers:
                if qualifier in word and word != qualifier:
                    return True

        return False


    def _build_rejection(
        self,
        poly_market,
        pf_market,
        poly_signature,
        pf_signature,
        reject_reason,
        title_score=0.0,
        participant_score=0.0,
        outcome_mapping=None,
    ):
        return {
            "matched": False,
            "score": max(title_score, participant_score),
            "outcome_mapping": outcome_mapping,
            "reason": {
                "poly_title": poly_market.title,
                "pf_title": pf_market.title,
                "title_score": round(title_score, 4),
                "participant_score": round(participant_score, 4),
                "poly_kind": poly_signature["kind"],
                "pf_kind": pf_signature["kind"],
                "reject_reason": reject_reason,
            },
        }


    def _extract_condition_ids(self, market):
        raw_payload = getattr(market, "raw_payload_json", None) or {}
        condition_ids = []

        direct_condition_id = raw_payload.get("conditionId")
        if direct_condition_id:
            condition_ids.append(str(direct_condition_id))

        linked_condition_ids = raw_payload.get("polymarketConditionIds") or []
        if isinstance(linked_condition_ids, list):
            condition_ids.extend(
                str(condition_id)
                for condition_id in linked_condition_ids
                if condition_id
            )

        seen = set()
        unique_ids = []
        for condition_id in condition_ids:
            if condition_id in seen:
                continue
            seen.add(condition_id)
            unique_ids.append(condition_id)

        return unique_ids


    def _has_direct_condition_match(self, poly_market, pf_market):
        poly_condition_ids = set(self._extract_condition_ids(poly_market))
        pf_condition_ids = set(self._extract_condition_ids(pf_market))
        return bool(poly_condition_ids and pf_condition_ids and poly_condition_ids.intersection(pf_condition_ids))


    def _normalize_outcome_label(self, value):
        return self.normalizer.normalize_outcome_label(value)


    def _extract_ordered_outcomes(self, market):
        outcomes = getattr(market, "outcomes_json", None) or []
        ordered_outcomes = []

        for index, outcome in enumerate(outcomes):
            if not isinstance(outcome, dict):
                continue

            label = str(outcome.get("label") or outcome.get("name") or "").strip()
            outcome_id = outcome.get("id")
            if not label or outcome_id is None:
                continue

            ordered_outcomes.append(
                {
                    "id": str(outcome_id),
                    "label": label,
                    "normalized_label": self._normalize_outcome_label(label),
                    "index": index,
                }
            )

        return ordered_outcomes


    def _extract_binary_outcomes(self, market):
        outcomes = self._extract_ordered_outcomes(market)
        mapping = {}

        for outcome in outcomes:
            key = self._normalize_outcome_label(outcome.get("normalized_label"))
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


    def _extract_named_outcomes(self, market):
        named_outcomes = []

        for outcome in self._extract_ordered_outcomes(market):
            if outcome["normalized_label"] in {"yes", "no", "draw"}:
                continue
            named_outcomes.append(outcome)

        return named_outcomes


    def _extract_matchup_participants_from_title(self, title):
        lowered = str(title or "").lower()
        parts = re.split(r"\bvs\.?\b|\bversus\b|\bv\.?\b", lowered)
        if len(parts) != 2:
            return []

        participants = []
        for part in parts:
            cleaned = str(part).strip(" -:,.?!")
            if not cleaned:
                continue
            participants.append(cleaned)

        return participants if len(participants) == 2 else []


    def _extract_subject_participant_from_title(self, title):
        normalized = self.normalizer.normalize_text(title)
        patterns = [
            r"^will\s+(.+?)\s+(?:win|beat|make|reach|qualify|finish|be|have)\b",
            r"^can\s+(.+?)\s+(?:win|beat|make|reach|qualify|finish|be|have)\b",
        ]

        for pattern in patterns:
            match = re.match(pattern, normalized)
            if match:
                subject = match.group(1).strip()
                if subject:
                    return subject

        return None


    def _extract_subject_participant_from_market_context(self, market):
        raw_payload = getattr(market, "raw_payload_json", None) or {}
        for field_name in ("groupItemTitle",):
            value = raw_payload.get(field_name)
            if not self._is_meaningful_context_subject(value):
                continue
            return str(value).strip()

        return None


    def _is_meaningful_context_subject(self, value):
        tokens = self._entity_tokens(value)
        if not tokens:
            return False

        structural_tokens = {
            "draw", "half", "halftime", "handicap", "line", "market",
            "moneyline", "overtime", "period", "quarter", "result",
            "results", "spread", "tie", "total", "totals", "winner",
        }
        return tokens.isdisjoint(structural_tokens)


    def _extract_binary_head_to_head_participants(self, title):
        normalized = self.normalizer.normalize_text(title)
        patterns = [
            r"^(?:will|can)\s+(.+?)\s+(?:beat|defeat|defeats)\s+(.+?)(?:\bon\b|\bin\b|\bby\b|$)",
            r"^(?:will|can)\s+(.+?)\s+win\s+(?:against|over)\s+(.+?)(?:\bon\b|\bin\b|\bby\b|$)",
        ]

        for pattern in patterns:
            match = re.match(pattern, normalized)
            if not match:
                continue

            subject = match.group(1).strip()
            opponent = match.group(2).strip()
            if subject and opponent:
                return [subject, opponent]

        return []


    def _entity_tokens(self, value):
        normalized = self.normalizer.normalize_text(value)
        return {
            token for token in normalized.split()
            if len(token) > 1
            and token not in self.stop_words
            and token not in self.entity_stop_words
        }


    def _canonical_participants(self, values):
        canonical = []
        seen = set()

        for value in values:
            tokens = self._entity_tokens(value)
            if not tokens:
                continue

            key = tuple(sorted(tokens))
            if key in seen:
                continue

            seen.add(key)
            canonical.append(
                {
                    "label": str(value).strip(),
                    "tokens": tokens,
                }
            )

        return canonical


    def _extract_participants(self, market):
        title = getattr(market, "title", "") or ""
        participants = self._extract_matchup_participants_from_title(title)

        if not participants:
            participants = self._extract_binary_head_to_head_participants(title)

        if not participants:
            subject = self._extract_subject_participant_from_title(title)
            if subject:
                participants.append(subject)

        if not participants:
            subject = self._extract_subject_participant_from_market_context(market)
            if subject:
                participants.append(subject)

        named_outcomes = self._extract_named_outcomes(market)
        meaningful = [
            o for o in named_outcomes
            if o["normalized_label"] not in self.generic_outcome_labels
        ]
        if len(meaningful) == 2:
            participants.extend(outcome["label"] for outcome in meaningful)

        return self._canonical_participants(participants)


    def _detect_market_kind(self, market, participants):
        title = str(getattr(market, "title", "") or "")
        if self._extract_matchup_participants_from_title(title):
            return "matchup"

        if self._extract_binary_head_to_head_participants(title):
            return "matchup"

        if len(participants) == 2 and len(self._extract_named_outcomes(market)) == 2:
            return "matchup"

        if self._extract_subject_participant_from_title(title):
            return "proposition"

        return "generic"


    def _detect_market_variant(self, market):
        haystack = self._market_context_haystack(market)

        if any(token in haystack for token in ("spread", "handicap", "run line", "puck line")):
            return "spread"

        if any(token in haystack for token in ("total", "totals", "o u", "over under")):
            return "total"

        return "moneyline"


    def _market_context_haystack(self, market):
        raw_payload = getattr(market, "raw_payload_json", None) or {}
        parts = [
            getattr(market, "title", "") or "",
            getattr(market, "slug", "") or "",
            getattr(market, "category", "") or "",
            getattr(market, "description", "") or "",
            raw_payload.get("title") or "",
            raw_payload.get("name") or "",
            raw_payload.get("groupItemTitle") or "",
            raw_payload.get("question") or "",
            raw_payload.get("description") or "",
            raw_payload.get("category") or "",
            raw_payload.get("subcategory") or "",
        ]
        return self.normalizer.normalize_text(" ".join(str(part) for part in parts if part))


    def _detect_market_scope(self, market):
        haystack = self._market_context_haystack(market)

        if any(
            phrase in haystack
            for phrase in (
                "halftime",
                "half time",
                "first half",
                "1st half",
            )
        ):
            return "halftime"

        if any(
            phrase in haystack
            for phrase in (
                "second half",
                "2nd half",
            )
        ):
            return "second_half"

        return "default"


    def _detect_event_granularity(self, market):
        haystack = self._market_context_haystack(market)

        if any(
            phrase in haystack
            for phrase in (
                "who will win series",
                "win series",
                "wins series",
                "best of 7 series",
                "best of seven series",
                "first round series",
                "series between",
            )
        ):
            return "series"

        if any(
            phrase in haystack
            for phrase in (
                "map 1",
                "map 2",
                "map 3",
                "map 4",
                "map 5",
            )
        ):
            return "map"

        if any(
            phrase in haystack
            for phrase in (
                "set 1",
                "set 2",
                "set 3",
                "set 4",
                "set 5",
            )
        ):
            return "set"

        if any(
            phrase in haystack
            for phrase in (
                "scheduled for",
                "upcoming nba game",
                "upcoming game",
                "final score",
                "if the rockets win",
                "if the lakers win",
                "if the game is postponed",
                "if the game is canceled",
                "game will resolve to",
            )
        ):
            return "game"

        return "default"


    def _detect_comparison_type(self, market):
        raw_payload = getattr(market, "raw_payload_json", None) or {}
        raw_parts = [
            getattr(market, "title", "") or "",
            getattr(market, "slug", "") or "",
            raw_payload.get("title") or "",
            raw_payload.get("groupItemTitle") or "",
            raw_payload.get("question") or "",
            raw_payload.get("description") or "",
        ]
        raw_haystack = " ".join(str(part) for part in raw_parts if part).lower()
        haystack = self.normalizer.normalize_text(raw_haystack)

        if not re.search(r"\d", raw_haystack):
            return "unknown"

        if any(symbol in raw_haystack for symbol in ("≥", ">=")):
            return "gte"

        if any(symbol in raw_haystack for symbol in ("≤", "<=")):
            return "lte"

        if ">" in raw_haystack:
            return "gt"

        if "<" in raw_haystack:
            return "lt"

        if any(
            phrase in haystack
            for phrase in (
                "at least",
                "or more",
                "not less than",
                "no less than",
                "minimum",
                "min ",
            )
        ):
            return "gte"

        if any(
            phrase in haystack
            for phrase in (
                "at most",
                "or less",
                "not more than",
                "no more than",
                "maximum",
                "max ",
            )
        ):
            return "lte"

        if any(
            phrase in haystack
            for phrase in (
                "more than",
                "greater than",
                "higher than",
                "above ",
                "over ",
                "exceed",
                "exceeds",
            )
        ):
            return "gt"

        if any(
            phrase in haystack
            for phrase in (
                "less than",
                "lower than",
                "below ",
                "under ",
                "fewer than",
            )
        ):
            return "lt"

        if any(
            phrase in haystack
            for phrase in (
                "exactly",
                "equal to",
                "equals ",
                "equal ",
            )
        ):
            return "exact"

        if re.search(r"\b(?:increase|decrease|rise|fall|inflation)\b.*\bby\s+\d", haystack):
            return "exact"

        return "unknown"


    def _participant_similarity(self, left, right):
        if not left["tokens"] or not right["tokens"]:
            return 0.0

        intersection = len(left["tokens"].intersection(right["tokens"]))
        union = len(left["tokens"].union(right["tokens"]))
        if union == 0:
            return 0.0

        if left["tokens"].issubset(right["tokens"]) or right["tokens"].issubset(left["tokens"]):
            return max(intersection / union, 0.8)

        return intersection / union


    def _calculate_participant_score(self, poly_participants, pf_participants):
        if not poly_participants or not pf_participants:
            return 0.0

        if len(poly_participants) == 2 and len(pf_participants) == 2:
            direct_score = (
                self._participant_similarity(poly_participants[0], pf_participants[0])
                + self._participant_similarity(poly_participants[1], pf_participants[1])
            ) / 2
            inverse_score = (
                self._participant_similarity(poly_participants[0], pf_participants[1])
                + self._participant_similarity(poly_participants[1], pf_participants[0])
            ) / 2
            return max(direct_score, inverse_score)

        return max(
            self._participant_similarity(poly_participant, pf_participant)
            for poly_participant in poly_participants
            for pf_participant in pf_participants
        )


    def _best_matchup_participant_alignment(self, poly_participants, pf_participants):
        if len(poly_participants) != 2 or len(pf_participants) != 2:
            return None

        direct_scores = (
            self._participant_similarity(poly_participants[0], pf_participants[0]),
            self._participant_similarity(poly_participants[1], pf_participants[1]),
        )
        inverse_scores = (
            self._participant_similarity(poly_participants[0], pf_participants[1]),
            self._participant_similarity(poly_participants[1], pf_participants[0]),
        )

        direct_min = min(direct_scores)
        inverse_min = min(inverse_scores)
        if direct_min >= inverse_min:
            return direct_scores
        return inverse_scores


    def _has_compatible_matchup_participants(self, poly_signature, pf_signature):
        if poly_signature["kind"] != "matchup" or pf_signature["kind"] != "matchup":
            return True

        alignment_scores = self._best_matchup_participant_alignment(
            poly_signature["participants"],
            pf_signature["participants"],
        )
        if alignment_scores is None:
            return True

        return min(alignment_scores) >= 0.5


    def _context_tokens(self, signature):
        return set((signature.get("context_haystack") or "").split())


    def _participant_matches_signature_context(self, participant, signature):
        participant_tokens = participant["tokens"]
        context_tokens = self._context_tokens(signature)
        if not participant_tokens or not context_tokens:
            return False

        overlap = len(participant_tokens.intersection(context_tokens))
        return (overlap / len(participant_tokens)) >= 0.8


    def _has_compatible_direct_condition_context(self, poly_signature, pf_signature):
        if not self._are_market_shapes_compatible(poly_signature, pf_signature):
            return False

        if not self._has_compatible_matchup_participants(poly_signature, pf_signature):
            return False

        poly_participants = poly_signature["participants"]
        pf_participants = pf_signature["participants"]

        if poly_participants and pf_participants:
            return self._calculate_participant_score(
                poly_participants,
                pf_participants,
            ) >= 0.8

        if poly_participants:
            return all(
                self._participant_matches_signature_context(participant, pf_signature)
                for participant in poly_participants
            )

        if pf_participants:
            return all(
                self._participant_matches_signature_context(participant, poly_signature)
                for participant in pf_participants
            )

        return True


    def _has_compatible_market_context(self, poly_signature, pf_signature):
        if "matchup" in {poly_signature["kind"], pf_signature["kind"]}:
            return True

        poly_context_tokens = self._meaningful_context_tokens(poly_signature)
        pf_context_tokens = self._meaningful_context_tokens(pf_signature)

        if not poly_context_tokens or not pf_context_tokens:
            return True

        if poly_context_tokens == pf_context_tokens:
            return True

        intersection = poly_context_tokens & pf_context_tokens
        if not intersection:
            return False

        overlap_floor = min(len(poly_context_tokens), len(pf_context_tokens))
        if overlap_floor == 0:
            return True

        if (len(intersection) / overlap_floor) < 0.8:
            return False

        return not (
            poly_context_tokens - pf_context_tokens
            or pf_context_tokens - poly_context_tokens
        )


    def _are_market_shapes_compatible(self, poly_signature, pf_signature):
        if poly_signature["kind"] == pf_signature["kind"]:
            return True

        if "matchup" in {poly_signature["kind"], pf_signature["kind"]}:
            return False

        poly_count = len(poly_signature["participants"])
        pf_count = len(pf_signature["participants"])
        if poly_count and pf_count and poly_count != pf_count:
            return False

        return True


    def _pick_best_label_match(self, source_outcome, target_outcomes, used_indexes):
        best_index = None
        best_score = 0.0
        source_tokens = self._entity_tokens(source_outcome["label"])

        for index, target_outcome in enumerate(target_outcomes):
            if index in used_indexes:
                continue

            target_tokens = self._entity_tokens(target_outcome["label"])
            if not source_tokens or not target_tokens:
                continue

            intersection = len(source_tokens.intersection(target_tokens))
            union = len(source_tokens.union(target_tokens))
            if union == 0:
                continue

            score = intersection / union
            if source_tokens.issubset(target_tokens) or target_tokens.issubset(source_tokens):
                score = max(score, 0.8)

            if score > best_score:
                best_score = score
                best_index = index

        if best_index is None or best_score < 0.5:
            return None

        used_indexes.add(best_index)
        return target_outcomes[best_index]


    def _build_two_outcome_mapping(self, poly_outcomes, pf_outcomes, force_order=False):
        if len(poly_outcomes) != 2 or len(pf_outcomes) != 2:
            return None

        poly_by_label = {
            outcome["normalized_label"]: outcome
            for outcome in poly_outcomes
        }
        pf_by_label = {
            outcome["normalized_label"]: outcome
            for outcome in pf_outcomes
        }

        if set(poly_by_label.keys()) == set(pf_by_label.keys()):
            yes_poly = poly_by_label[pf_outcomes[0]["normalized_label"]]
            no_poly = poly_by_label[pf_outcomes[1]["normalized_label"]]
            yes_pf = pf_outcomes[0]
            no_pf = pf_outcomes[1]
        elif force_order:
            yes_poly, no_poly = poly_outcomes
            yes_pf, no_pf = pf_outcomes
        else:
            used_indexes = set()
            matched_poly_outcomes = []

            for pf_outcome in pf_outcomes:
                poly_outcome = self._pick_best_label_match(
                    pf_outcome,
                    poly_outcomes,
                    used_indexes,
                )
                if poly_outcome is None:
                    return None
                matched_poly_outcomes.append(poly_outcome)

            yes_poly, no_poly = matched_poly_outcomes
            yes_pf, no_pf = pf_outcomes

        return {
            "market_a": {
                "yes": yes_poly["id"],
                "no": no_poly["id"],
                "yes_label": yes_poly["label"],
                "no_label": no_poly["label"],
            },
            "market_b": {
                "yes": yes_pf["id"],
                "no": no_pf["id"],
                "yes_label": yes_pf["label"],
                "no_label": no_pf["label"],
            },
            "is_inverted": False,
            "confidence": "high" if not force_order else "medium",
        }


    def _build_binary_named_outcome_mapping(self, binary_market, named_market, binary_is_market_a):
        binary_outcomes = self._extract_binary_outcomes(binary_market)
        named_outcomes = self._extract_named_outcomes(named_market)
        head_to_head_participants = self._extract_binary_head_to_head_participants(
            getattr(binary_market, "title", ""),
        )

        if not binary_outcomes or len(named_outcomes) != 2 or len(head_to_head_participants) != 2:
            return None

        used_indexes = set()
        yes_named_outcome = self._pick_best_label_match(
            {"label": head_to_head_participants[0]},
            named_outcomes,
            used_indexes,
        )
        if yes_named_outcome is None:
            return None

        no_named_outcome = self._pick_best_label_match(
            {"label": head_to_head_participants[1]},
            named_outcomes,
            used_indexes,
        )
        if no_named_outcome is None:
            return None

        binary_payload = {
            "yes": binary_outcomes["yes"]["id"],
            "no": binary_outcomes["no"]["id"],
            "yes_label": binary_outcomes["yes"]["label"],
            "no_label": binary_outcomes["no"]["label"],
        }
        named_payload = {
            "yes": yes_named_outcome["id"],
            "no": no_named_outcome["id"],
            "yes_label": yes_named_outcome["label"],
            "no_label": no_named_outcome["label"],
        }

        return {
            "market_a": binary_payload if binary_is_market_a else named_payload,
            "market_b": named_payload if binary_is_market_a else binary_payload,
            "is_inverted": False,
            "confidence": "medium",
        }


    def _build_outcome_mapping(self, poly_market, pf_market, allow_fallback_order=False):
        poly_outcomes = self._extract_binary_outcomes(poly_market)
        pf_outcomes = self._extract_binary_outcomes(pf_market)
        if poly_outcomes and pf_outcomes:
            return {
                "market_a": {
                    "yes": poly_outcomes["yes"]["id"],
                    "no": poly_outcomes["no"]["id"],
                    "yes_label": poly_outcomes["yes"]["label"],
                    "no_label": poly_outcomes["no"]["label"],
                },
                "market_b": {
                    "yes": pf_outcomes["yes"]["id"],
                    "no": pf_outcomes["no"]["id"],
                    "yes_label": pf_outcomes["yes"]["label"],
                    "no_label": pf_outcomes["no"]["label"],
                },
                "is_inverted": False,
                "confidence": "high",
            }

        if poly_outcomes:
            mapping = self._build_binary_named_outcome_mapping(poly_market, pf_market, binary_is_market_a=True)
            if mapping:
                return mapping

        if pf_outcomes:
            mapping = self._build_binary_named_outcome_mapping(
                pf_market,
                poly_market,
                binary_is_market_a=False,
            )
            if mapping:
                return mapping

        return self._build_two_outcome_mapping(
            self._extract_ordered_outcomes(poly_market),
            self._extract_ordered_outcomes(pf_market),
            force_order=allow_fallback_order,
        )


    def _tokenize(self, normalized_text):
        return {
            word for word in normalized_text.split()
            if len(word) > 1 and word not in self.stop_words
        }


    def _category_tokens(self, market):
        category = getattr(market, "category", "") or ""
        normalized = self.normalizer.normalize_text(category)
        return self._tokenize(normalized)


    def _overlap_score(self, left_values, right_values):
        left_set = set(left_values or [])
        right_set = set(right_values or [])
        if not left_set or not right_set:
            return 0.0

        intersection = len(left_set.intersection(right_set))
        union = len(left_set.union(right_set))
        if union == 0:
            return 0.0

        return intersection / union


    def candidate_rank_score(self, poly_signature, pf_signature, shared_token_count):
        score = float(shared_token_count)
        score += self._overlap_score(
            poly_signature["category_tokens"],
            pf_signature["category_tokens"],
        ) * 3.0
        score += self._overlap_score(
            poly_signature["entities"]["dates"],
            pf_signature["entities"]["dates"],
        ) * 4.0
        score += self._overlap_score(
            poly_signature["entities"]["numbers"],
            pf_signature["entities"]["numbers"],
        ) * 2.0
        score += self._calculate_participant_score(
            poly_signature["participants"],
            pf_signature["participants"],
        ) * 5.0
        if poly_signature["kind"] == pf_signature["kind"]:
            score += 1.0
        return score


    def build_market_signature(self, market):
        normalized_title = self.normalizer.normalize_text(market.title)
        participants = self._extract_participants(market)
        title_tokens = self._tokenize(normalized_title)
        context_haystack = self._market_context_haystack(market)
        return {
            "market": market,
            "title_tokens": title_tokens,
            "tokens": title_tokens.union(
                {
                    token
                    for participant in participants
                    for token in participant["tokens"]
                }
            ),
            "context_haystack": context_haystack,
            "category_tokens": self._category_tokens(market),
            "condition_ids": self._extract_condition_ids(market),
            "entities": self.normalizer.extract_entities(context_haystack),
            "participants": participants,
            "kind": self._detect_market_kind(market, participants),
            "variant": self._detect_market_variant(market),
            "scope": self._detect_market_scope(market),
            "event_granularity": self._detect_event_granularity(market),
            "comparison_type": self._detect_comparison_type(market),
        }


    def build_candidate_index(self, markets):
        index = {
            "tokens": {},
            "condition_ids": {},
        }

        for market in markets:
            signature = self.build_market_signature(market)
            for token in signature["tokens"]:
                index["tokens"].setdefault(token, []).append(signature)

            for condition_id in signature["condition_ids"]:
                index["condition_ids"].setdefault(condition_id, []).append(signature)

        return index