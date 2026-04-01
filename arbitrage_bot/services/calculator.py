from arbitrage_bot.core.config import settings


class ArbitrageCalculator:
    def __init__(self):
        self.fee_poly = settings.FEE_POLYMARKET_BPS / 10000.0
        self.fee_pf = settings.FEE_PREDICT_FUN_BPS / 10000.0


    def calculate_opportunity(self, poly_asks, pf_asks):
        if not poly_asks or not pf_asks:
            return None

        poly_levels = list(poly_asks)
        pf_levels = list(pf_asks)

        shares = 0.0
        capital = 0.0
        cost_poly = 0.0
        cost_pf = 0.0

        poly_idx = 0
        pf_idx = 0

        # bypassing the order books on both exchanges until the sum of prices < 1
        while poly_idx < len(poly_levels) and pf_idx < len(pf_levels):
            p_price, p_size = poly_levels[poly_idx]
            f_price, f_size = pf_levels[pf_idx]

            if p_price < 0 or f_price < 0 or p_size <= 0 or f_size <= 0:
                return None

            sum_price = p_price + f_price
            if sum_price >= 1.0:
                break

            take_size = min(p_size, f_size)

            shares += take_size
            cost_poly += take_size * p_price
            cost_pf += take_size * f_price
            capital += take_size * sum_price

            poly_levels[poly_idx] = (p_price, p_size - take_size)
            pf_levels[pf_idx] = (f_price, f_size - take_size)

            if poly_levels[poly_idx][1] <= 0:
                poly_idx += 1
            if pf_levels[pf_idx][1] <= 0:
                pf_idx += 1

        if shares == 0:
            return None

        avg_price_poly = cost_poly / shares
        avg_price_pf = cost_pf / shares

        gross_profit = shares * 1.0 - capital

        total_fees = cost_poly * self.fee_poly + cost_pf * self.fee_pf
        net_profit = gross_profit - total_fees

        gross_roi = gross_profit / capital if capital > 0 else 0.0
        net_roi = net_profit / capital if capital > 0 else 0.0

        return {
            "shares": shares,
            "capital_required": capital,
            "avg_price_leg_1": avg_price_poly,
            "avg_price_leg_2": avg_price_pf,
            "gross_profit": gross_profit,
            "net_profit": net_profit,
            "gross_roi": gross_roi,
            "net_roi": net_roi
        }


    def calculate_opportunities(self, direction_books):
        opportunities = []

        for direction, books in (direction_books or {}).items():
            result = self.calculate_opportunity(
                poly_asks=books.get("poly") or [],
                pf_asks=books.get("pf") or [],
            )
            if not result:
                continue

            result["direction"] = direction
            opportunities.append(result)

        return opportunities