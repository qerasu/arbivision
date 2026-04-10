from arbitrage_bot.core.config import settings


class ArbitrageCalculator:
    def __init__(self):
        self.fee_poly = settings.FEE_POLYMARKET_BPS / 10000.0
        self.fee_pf = settings.FEE_PREDICT_FUN_BPS / 10000.0


    def calculate_opportunity(self, poly_asks, pf_asks, max_capital=None, max_polymarket_capital=None, max_predict_fun_capital=None):
        if not poly_asks or not pf_asks:
            return None

        poly_levels = list(poly_asks)
        pf_levels = list(pf_asks)
        best_price_poly = float(poly_levels[0][0])
        best_price_pf = float(pf_levels[0][0])

        shares = 0.0
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

            net_p_price = p_price * (1.0 + self.fee_poly)
            net_f_price = f_price * (1.0 + self.fee_pf)

            if net_p_price + net_f_price >= 1.0:
                break

            take_size = min(p_size, f_size)
            if max_capital is not None:
                remaining_capital = float(max_capital) - (cost_poly + cost_pf)
                if remaining_capital <= 0:
                    break
                per_share_capital = net_p_price + net_f_price
                if per_share_capital <= 0:
                    return None
                take_size = min(take_size, remaining_capital / per_share_capital)

            if max_polymarket_capital is not None:
                remaining_poly_capital = float(max_polymarket_capital) - cost_poly
                if remaining_poly_capital <= 0:
                    break
                if net_p_price <= 0:
                    return None
                take_size = min(take_size, remaining_poly_capital / net_p_price)

            if max_predict_fun_capital is not None:
                remaining_pf_capital = float(max_predict_fun_capital) - cost_pf
                if remaining_pf_capital <= 0:
                    break
                if net_f_price <= 0:
                    return None
                take_size = min(take_size, remaining_pf_capital / net_f_price)

            if take_size <= 0:
                break

            shares += take_size
            cost_poly += take_size * net_p_price
            cost_pf += take_size * net_f_price

            poly_levels[poly_idx] = (p_price, p_size - take_size)
            pf_levels[pf_idx] = (f_price, f_size - take_size)

            if poly_levels[poly_idx][1] <= 0:
                poly_idx += 1
            if pf_levels[pf_idx][1] <= 0:
                pf_idx += 1

        if shares == 0:
            return None

        capital = cost_poly + cost_pf
        net_profit = shares - capital

        if net_profit <= 0:
            return None

        avg_price_poly = cost_poly / shares
        avg_price_pf = cost_pf / shares
        gross_profit = shares - capital
        net_roi = net_profit / capital if capital > 0 else 0.0

        return {
            "shares": shares,
            "capital_required": capital,
            "avg_price_leg_1": avg_price_poly,
            "avg_price_leg_2": avg_price_pf,
            "best_price_leg_1": best_price_poly,
            "best_price_leg_2": best_price_pf,
            "gross_profit": gross_profit,
            "net_profit": net_profit,
            "gross_roi": net_roi,
            "net_roi": net_roi,
        }


    def calculate_opportunities(self, direction_books, max_capital=None, max_polymarket_capital=None, max_predict_fun_capital=None):
        opportunities = []

        for direction, books in (direction_books or {}).items():
            result = self.calculate_opportunity(
                poly_asks=books.get("poly") or [],
                pf_asks=books.get("pf") or [],
                max_capital=max_capital,
                max_polymarket_capital=max_polymarket_capital,
                max_predict_fun_capital=max_predict_fun_capital,
            )
            if not result:
                continue

            result["direction"] = direction
            opportunities.append(result)

        return opportunities