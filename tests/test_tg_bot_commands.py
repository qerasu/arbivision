import unittest
from datetime import datetime
from datetime import timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import patch

from aiogram.exceptions import TelegramBadRequest
from arbitrage_bot.models.orm import UserPreference
from arbitrage_bot.tg_bot.bot import _apply_calc_result_to_opportunity
from arbitrage_bot.tg_bot.bot import _build_bot_commands
from arbitrage_bot.tg_bot.bot import _format_alert_message
from arbitrage_bot.tg_bot.bot import _is_missing_table_error
from arbitrage_bot.tg_bot.bot import _revalidate_alert_opportunity
from arbitrage_bot.tg_bot.bot import _should_skip_alert_for_current_preferences
from arbitrage_bot.tg_bot.handlers import _build_home_keyboard
from arbitrage_bot.tg_bot.handlers import _build_prompt_keyboard
from arbitrage_bot.tg_bot.handlers import _build_settings_keyboard
from arbitrage_bot.tg_bot.handlers import _apply_setting_update
from arbitrage_bot.tg_bot.handlers import _safe_answer_callback
from arbitrage_bot.tg_bot.handlers import _safe_delete_message
from arbitrage_bot.tg_bot.handlers import _safe_edit_text

from arbitrage_bot.tg_bot.handlers import _parse_set_command
from arbitrage_bot.tg_bot.preferences import format_status_text
from sqlalchemy.exc import ProgrammingError


class TelegramBotCommandsTests(unittest.TestCase):
    def test_parse_set_command_for_roi(self):
        field_name, value = _parse_set_command("/set roi 1.5")

        self.assertEqual(field_name, "min_roi_percent")
        self.assertEqual(value, 1.5)


    def test_parse_set_command_for_volume(self):
        field_name, value = _parse_set_command("/set volume 500")

        self.assertEqual(field_name, "max_capital_usd")
        self.assertEqual(value, 500.0)


    def test_parse_set_command_for_min_volume(self):
        field_name, value = _parse_set_command("/set minvolume 50")

        self.assertEqual(field_name, "min_capital_usd")
        self.assertEqual(value, 50.0)


    def test_parse_set_command_for_profit(self):
        field_name, value = _parse_set_command("/set profit 10")

        self.assertEqual(field_name, "min_profit_usd")
        self.assertEqual(value, 10.0)


    def test_parse_set_command_for_expires_off(self):
        field_name, value = _parse_set_command("/set expires off")

        self.assertEqual(field_name, "max_days_to_close")
        self.assertIsNone(value)


    def test_parse_set_command_error_mentions_reset(self):
        with self.assertRaisesRegex(ValueError, "/reset"):
            _parse_set_command("/set")


    def test_build_home_keyboard_contains_pause_and_settings_buttons(self):
        keyboard = _build_home_keyboard()

        self.assertEqual(keyboard.inline_keyboard[0][0].text, "⏸ Pause")
        self.assertEqual(keyboard.inline_keyboard[1][0].text, "Settings")


    def test_build_settings_keyboard_contains_expected_actions(self):
        keyboard = _build_settings_keyboard()

        self.assertEqual(keyboard.inline_keyboard[0][0].text, "→ Min ROI")
        self.assertEqual(keyboard.inline_keyboard[0][1].text, "→ Min volume")
        self.assertEqual(keyboard.inline_keyboard[1][0].text, "→ Max volume")
        self.assertEqual(keyboard.inline_keyboard[1][1].text, "→ Min profit")
        self.assertEqual(keyboard.inline_keyboard[2][0].text, "→ Max market end")


    def test_build_home_keyboard_shows_resume_when_muted(self):
        keyboard = _build_home_keyboard({"muted": True})

        self.assertEqual(keyboard.inline_keyboard[0][0].text, "▶️ Resume")


    def test_build_prompt_keyboard_contains_only_back_button(self):
        keyboard = _build_prompt_keyboard()

        self.assertEqual(len(keyboard.inline_keyboard), 1)
        self.assertEqual(len(keyboard.inline_keyboard[0]), 1)
        self.assertEqual(keyboard.inline_keyboard[0][0].text, "← Back")


    def test_build_bot_commands_contains_start(self):
        commands = _build_bot_commands()

        self.assertEqual(len(commands), 1)
        self.assertEqual(commands[0].command, "start")


    def test_is_missing_table_error_detects_undefined_table(self):
        orig = Exception('relation "alerts" does not exist')
        orig.sqlstate = "42P01"
        exc = ProgrammingError("SELECT * FROM alerts", {}, orig)

        self.assertTrue(_is_missing_table_error(exc))


    def test_is_missing_table_error_ignores_other_programming_errors(self):
        orig = Exception("syntax error")
        orig.sqlstate = "42601"
        exc = ProgrammingError("bad sql", {}, orig)

        self.assertFalse(_is_missing_table_error(exc))


    def test_format_alert_message_uses_compact_card_layout(self):
        opportunity = SimpleNamespace(
            direction="A_no_B_yes",
            net_profit=7.0,
            net_roi=0.14,
            capital_required=43.0,
            shares=50.0,
            avg_price_leg_1=0.36,
            avg_price_leg_2=0.50,
        )
        pair = SimpleNamespace(match_score=1.0)
        market_a = SimpleNamespace(
            platform="polymarket",
            title="Will Manchester United FC win on 2026-03-20?",
            slug="manchester-united-win",
            raw_payload_json={"endDate": "2026-03-28T00:00:00+00:00"},
        )
        market_b = SimpleNamespace(
            platform="predict_fun",
            title="Manchester United FC",
            slug="",
            platform_market_id="pf-123",
            raw_payload_json={"resolveDate": "2026-03-27T00:00:00+00:00"},
        )

        with patch(
            "arbitrage_bot.tg_bot.bot.datetime",
        ) as datetime_mock:
            datetime_mock.now.return_value = datetime(2026, 3, 21, tzinfo=timezone.utc)
            text = _format_alert_message(opportunity, pair, market_a, market_b)

        self.assertIn("Will Manchester United FC win on 2026-03-20?", text)
        self.assertIn("💰 Profit: $7", text)
        self.assertIn("📈 Spread: 14.00%", text)
        self.assertIn("💵 Volume: $43", text)
        self.assertIn("⏳ Ends on: 2026-03-28 (in 7 days)", text)
        self.assertIn("🧾 Buy 50 shares each:", text)
        self.assertIn("• NO on Polymarket @ $0.360 = $18", text)
        self.assertIn("• YES on Predict.Fun @ $0.500 = $25", text)
        self.assertIn("📊 Volumes ratio: 1.39x", text)
        self.assertIn("🔗 Open markets:", text)
        self.assertIn('<a href="https://polymarket.com/market/manchester-united-win?r=qerasuu">Polymarket</a>', text)
        self.assertIn('<a href="https://predict.fun/market/pf-123?ref=077A2">Predict.Fun</a>', text)


    def test_format_alert_message_uses_outcome_labels_from_mapping(self):
        opportunity = SimpleNamespace(
            direction="A_yes_B_no",
            net_profit=7.0,
            net_roi=0.14,
            capital_required=43.0,
            shares=50.0,
            avg_price_leg_1=0.36,
            avg_price_leg_2=0.50,
        )
        pair = SimpleNamespace(
            match_score=1.0,
            outcome_mapping_json={
                "market_a": {"yes_label": "Grizzlies", "no_label": "Hornets"},
                "market_b": {"yes_label": "Grizzlies", "no_label": "Hornets"},
            },
        )
        market_a = SimpleNamespace(
            platform="polymarket",
            title="Grizzlies vs Hornets",
            slug="grizzlies-vs-hornets",
            raw_payload_json={"endDate": "2026-03-28T00:00:00+00:00"},
        )
        market_b = SimpleNamespace(
            platform="predict_fun",
            title="Grizzlies vs. Hornets",
            slug="",
            platform_market_id="pf-123",
            raw_payload_json={"resolveDate": "2026-03-27T00:00:00+00:00"},
        )

        with patch(
            "arbitrage_bot.tg_bot.bot.datetime",
        ) as datetime_mock:
            datetime_mock.now.return_value = datetime(2026, 3, 21, tzinfo=timezone.utc)
            text = _format_alert_message(opportunity, pair, market_a, market_b)

        self.assertIn("• Grizzlies on Polymarket @ $0.360 = $18", text)
        self.assertIn("• Hornets on Predict.Fun @ $0.500 = $25", text)


    def test_format_status_text_contains_status_summary(self):
        text = format_status_text(
            {
                "min_roi_percent": None,
                "min_capital_usd": None,
                "max_capital_usd": None,
                "min_profit_usd": None,
                "max_days_to_close": None,
            }
        )

        self.assertIn("Current bot status.", text)
        self.assertIn("🟢 Status: Active", text)
        self.assertIn("📬 Telegram alerts are enabled.", text)
        self.assertNotIn("Current filters:", text)


    def test_format_status_text_shows_paused_when_muted(self):
        text = format_status_text(
            {
                "min_roi_percent": None,
                "min_capital_usd": None,
                "max_capital_usd": None,
                "min_profit_usd": None,
                "max_days_to_close": None,
                "muted": True,
            }
        )

        self.assertIn("🔴 Status: Paused", text)
        self.assertIn("📭 Telegram alerts are paused.", text)


    def test_should_skip_alert_for_current_preferences_when_updated_volume_filter_blocks(self):
        alert = SimpleNamespace(user_id=10)
        opportunity = SimpleNamespace(net_roi=0.20, capital_required=1382.22)
        market = SimpleNamespace(raw_payload_json={})
        preferences = UserPreference(
            user_id=10,
            min_roi_percent=5.0,
            max_capital_usd=150.0,
            max_days_to_close=5,
            muted=False,
        )

        should_skip = _should_skip_alert_for_current_preferences(
            alert,
            opportunity,
            market,
            market,
            preferences,
        )

        self.assertTrue(should_skip)


    def test_apply_calc_result_to_opportunity_updates_runtime_values(self):
        opportunity = SimpleNamespace(
            price_leg_1=0.0,
            price_leg_2=0.0,
            avg_price_leg_1=0.0,
            avg_price_leg_2=0.0,
            shares=0.0,
            capital_required=0.0,
            gross_profit=0.0,
            net_profit=0.0,
            gross_roi=0.0,
            net_roi=0.0,
            calculation_json=None,
        )
        calc_result = {
            "avg_price_leg_1": 0.41,
            "avg_price_leg_2": 0.52,
            "shares": 12.0,
            "capital_required": 11.16,
            "gross_profit": 0.84,
            "net_profit": 0.84,
            "gross_roi": 0.075,
            "net_roi": 0.075,
            "direction": "A_yes_B_no",
        }

        _apply_calc_result_to_opportunity(opportunity, calc_result)

        self.assertEqual(opportunity.avg_price_leg_1, 0.41)
        self.assertEqual(opportunity.avg_price_leg_2, 0.52)
        self.assertEqual(opportunity.shares, 12.0)
        self.assertEqual(opportunity.capital_required, 11.16)
        self.assertEqual(opportunity.calculation_json, calc_result)


class FakeSessionContext:
    async def __aenter__(self):
        return object()


    async def __aexit__(self, exc_type, exc, tb):
        return False


class TelegramBotSettingsUpdateTests(unittest.IsolatedAsyncioTestCase):
    async def test_apply_setting_update_reuses_prompt_message_when_available(self):
        message = AsyncMock()
        message.chat.id = 123
        message.bot.edit_message_text = AsyncMock()
        message.delete = AsyncMock()

        with patch(
            "arbitrage_bot.tg_bot.handlers.AsyncSessionLocal",
            return_value=FakeSessionContext(),
        ), patch(
            "arbitrage_bot.tg_bot.handlers.get_ui_state",
            new=AsyncMock(
                return_value={
                    "mode": "awaiting_value",
                    "field_name": "min_roi_percent",
                    "prompt_message_id": 77,
                }
            ),
        ), patch(
            "arbitrage_bot.tg_bot.handlers.set_user_preference",
            new=AsyncMock(
                return_value={
                    "min_roi_percent": 1.5,
                    "max_capital_usd": None,
                    "max_days_to_close": None,
                }
            ),
        ), patch(
            "arbitrage_bot.tg_bot.handlers.clear_ui_state",
            new=AsyncMock(),
        ):
            await _apply_setting_update(message, "min_roi_percent", 1.5)

        message.bot.edit_message_text.assert_awaited_once()
        message.answer.assert_not_awaited()
        message.delete.assert_awaited_once()


    async def test_apply_setting_update_falls_back_to_new_message_when_edit_fails(self):
        message = AsyncMock()
        message.chat.id = 123
        message.delete = AsyncMock()
        message.bot.edit_message_text = AsyncMock(
            side_effect=TelegramBadRequest(
                method="editMessageText",
                message="Bad Request: message to edit not found",
            )
        )

        with patch(
            "arbitrage_bot.tg_bot.handlers.AsyncSessionLocal",
            return_value=FakeSessionContext(),
        ), patch(
            "arbitrage_bot.tg_bot.handlers.get_ui_state",
            new=AsyncMock(
                return_value={
                    "mode": "awaiting_value",
                    "field_name": "min_roi_percent",
                    "prompt_message_id": 77,
                }
            ),
        ), patch(
            "arbitrage_bot.tg_bot.handlers.set_user_preference",
            new=AsyncMock(
                return_value={
                    "min_roi_percent": 1.5,
                    "max_capital_usd": None,
                    "max_days_to_close": None,
                }
            ),
        ), patch(
            "arbitrage_bot.tg_bot.handlers.clear_ui_state",
            new=AsyncMock(),
        ):
            await _apply_setting_update(message, "min_roi_percent", 1.5)

        message.bot.edit_message_text.assert_awaited_once()
        message.answer.assert_awaited_once()
        message.delete.assert_awaited_once()


class TelegramBotCallbackTests(unittest.IsolatedAsyncioTestCase):
    async def test_safe_edit_text_ignores_message_not_modified_error(self):
        callback = SimpleNamespace(
            message=SimpleNamespace(
                edit_text=AsyncMock(
                    side_effect=TelegramBadRequest(
                        method="editMessageText",
                        message="Bad Request: message is not modified",
                    )
                )
            )
        )

        await _safe_edit_text(
            callback,
            "same text",
            reply_markup=None,
        )


    async def test_safe_answer_callback_ignores_expired_query(self):
        callback = SimpleNamespace(
            answer=AsyncMock(
                side_effect=TelegramBadRequest(
                    method="answerCallbackQuery",
                    message="Bad Request: query is too old and response timeout expired or query ID is invalid",
                )
            )
        )

        await _safe_answer_callback(callback)


    async def test_safe_answer_callback_raises_for_other_errors(self):
        callback = SimpleNamespace(
            answer=AsyncMock(
                side_effect=TelegramBadRequest(
                    method="answerCallbackQuery",
                    message="Bad Request: chat not found",
                )
            )
        )

        with self.assertRaises(TelegramBadRequest):
            await _safe_answer_callback(callback)


    async def test_safe_delete_message_ignores_bad_request(self):
        message = SimpleNamespace(
            delete=AsyncMock(
                side_effect=TelegramBadRequest(
                    method="deleteMessage",
                    message="Bad Request: message can't be deleted",
                )
            )
        )

        await _safe_delete_message(message)


class TelegramBotRevalidationTests(unittest.IsolatedAsyncioTestCase):
    async def test_revalidate_alert_opportunity_returns_false_when_direction_disappears(self):
        opportunity = SimpleNamespace(direction="A_yes_B_no")
        pair = SimpleNamespace(id=7)
        orderbook_service = SimpleNamespace(
            fetch_orderbooks_for_pairs=AsyncMock(
                return_value=[
                    {
                        "directions": {
                            "A_no_B_yes": {
                                "poly": [(0.40, 10)],
                                "pf": [(0.50, 10)],
                            }
                        }
                    }
                ]
            )
        )
        calculator = SimpleNamespace(
            calculate_opportunities=lambda directions: [
                {
                    "direction": "A_no_B_yes",
                    "avg_price_leg_1": 0.40,
                    "avg_price_leg_2": 0.50,
                    "shares": 10.0,
                    "capital_required": 9.0,
                    "gross_profit": 1.0,
                    "net_profit": 1.0,
                    "gross_roi": 0.11,
                    "net_roi": 0.11,
                }
            ]
        )

        result = await _revalidate_alert_opportunity(
            object(),
            opportunity,
            pair,
            orderbook_service,
            calculator,
        )

        self.assertFalse(result)


    async def test_revalidate_alert_opportunity_refreshes_opportunity_values(self):
        opportunity = SimpleNamespace(
            direction="A_yes_B_no",
            price_leg_1=0.0,
            price_leg_2=0.0,
            avg_price_leg_1=0.0,
            avg_price_leg_2=0.0,
            shares=0.0,
            capital_required=0.0,
            gross_profit=0.0,
            net_profit=0.0,
            gross_roi=0.0,
            net_roi=0.0,
            calculation_json=None,
        )
        pair = SimpleNamespace(id=7)
        orderbook_service = SimpleNamespace(
            fetch_orderbooks_for_pairs=AsyncMock(
                return_value=[
                    {
                        "directions": {
                            "A_yes_B_no": {
                                "poly": [(0.41, 12)],
                                "pf": [(0.52, 12)],
                            }
                        }
                    }
                ]
            )
        )
        calculator = SimpleNamespace(
            calculate_opportunities=lambda directions: [
                {
                    "direction": "A_yes_B_no",
                    "avg_price_leg_1": 0.41,
                    "avg_price_leg_2": 0.52,
                    "shares": 12.0,
                    "capital_required": 11.16,
                    "gross_profit": 0.84,
                    "net_profit": 0.84,
                    "gross_roi": 0.075,
                    "net_roi": 0.075,
                }
            ]
        )

        result = await _revalidate_alert_opportunity(
            object(),
            opportunity,
            pair,
            orderbook_service,
            calculator,
        )

        self.assertTrue(result)
        self.assertEqual(opportunity.avg_price_leg_1, 0.41)
        self.assertEqual(opportunity.avg_price_leg_2, 0.52)
        self.assertEqual(opportunity.shares, 12.0)