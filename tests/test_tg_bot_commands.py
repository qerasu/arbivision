import unittest
from datetime import datetime
from datetime import timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import patch

from aiogram.exceptions import TelegramBadRequest
from arbitrage_bot.tg_bot.bot import _build_bot_commands
from arbitrage_bot.tg_bot.bot import _format_alert_message
from arbitrage_bot.tg_bot.bot import _is_missing_table_error
from arbitrage_bot.tg_bot.handlers import _build_home_keyboard
from arbitrage_bot.tg_bot.handlers import _build_settings_keyboard
from arbitrage_bot.tg_bot.handlers import _build_status_keyboard
from arbitrage_bot.tg_bot.handlers import _apply_setting_update
from arbitrage_bot.tg_bot.handlers import _safe_answer_callback
from arbitrage_bot.tg_bot.handlers import _safe_edit_text
from arbitrage_bot.tg_bot.handlers import cmd_status

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


    def test_parse_set_command_for_expires_off(self):
        field_name, value = _parse_set_command("/set expires off")

        self.assertEqual(field_name, "max_days_to_close")
        self.assertIsNone(value)


    def test_parse_set_command_error_mentions_reset(self):
        with self.assertRaisesRegex(ValueError, "/reset"):
            _parse_set_command("/set")


    def test_build_home_keyboard_contains_status_and_settings_buttons(self):
        keyboard = _build_home_keyboard()

        self.assertEqual(keyboard.inline_keyboard[0][0].text, "Status")
        self.assertEqual(keyboard.inline_keyboard[0][1].text, "Settings")


    def test_build_settings_keyboard_contains_expected_actions(self):
        keyboard = _build_settings_keyboard()

        self.assertEqual(keyboard.inline_keyboard[0][0].text, "→ Min ROI")
        self.assertEqual(keyboard.inline_keyboard[0][1].text, "→ Max volume")
        self.assertEqual(keyboard.inline_keyboard[1][0].text, "→ Max market end")


    def test_build_status_keyboard_contains_only_back_button(self):
        keyboard = _build_status_keyboard()

        self.assertEqual(len(keyboard.inline_keyboard), 1)
        self.assertEqual(len(keyboard.inline_keyboard[0]), 1)
        self.assertEqual(keyboard.inline_keyboard[0][0].text, "← Back")


    def test_build_bot_commands_contains_start(self):
        commands = _build_bot_commands()

        self.assertEqual(len(commands), 3)
        self.assertEqual(commands[0].command, "start")
        self.assertEqual(commands[1].command, "status")
        self.assertEqual(commands[2].command, "settings")


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
        self.assertIn("⏳ Max market end: 2026-03-28 (7 days)", text)
        self.assertIn("🧾 Buy 50 shares each:", text)
        self.assertIn("• NO on Polymarket @ $0.360 = $18", text)
        self.assertIn("• YES on Predict.Fun @ $0.500 = $25", text)
        self.assertIn("📊 Volumes ratio: 1.39x", text)
        self.assertIn("🔗 Open markets:", text)
        self.assertIn('<a href="https://polymarket.com/market/manchester-united-win">Polymarket</a>', text)
        self.assertIn('<a href="https://predict.fun/market/pf-123">Predict.Fun</a>', text)


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
                "max_capital_usd": None,
                "max_days_to_close": None,
            }
        )

        self.assertIn("Current bot status.", text)
        self.assertIn("🟢 Status: Active", text)
        self.assertIn("📬 Telegram alerts are enabled.", text)
        self.assertNotIn("Current filters:", text)


class FakeSessionContext:
    async def __aenter__(self):
        return object()


    async def __aexit__(self, exc_type, exc, tb):
        return False


class TelegramBotStatusCommandTests(unittest.IsolatedAsyncioTestCase):
    async def test_status_clears_pending_ui_state(self):
        message = AsyncMock()
        message.chat.id = 123

        with patch(
            "arbitrage_bot.tg_bot.handlers.AsyncSessionLocal",
            return_value=FakeSessionContext(),
        ), patch(
            "arbitrage_bot.tg_bot.handlers.get_global_preferences",
            new=AsyncMock(
                return_value={
                    "min_roi_percent": None,
                    "max_capital_usd": None,
                    "max_days_to_close": None,
                }
            ),
        ), patch(
            "arbitrage_bot.tg_bot.handlers.clear_ui_state",
            new=AsyncMock(),
        ) as clear_mock:
            await cmd_status(message)

        clear_mock.assert_awaited_once()
        message.answer.assert_awaited_once()


class TelegramBotSettingsUpdateTests(unittest.IsolatedAsyncioTestCase):
    async def test_apply_setting_update_reuses_prompt_message_when_available(self):
        message = AsyncMock()
        message.chat.id = 123
        message.bot.edit_message_text = AsyncMock()

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
            "arbitrage_bot.tg_bot.handlers.set_global_preference",
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


    async def test_apply_setting_update_falls_back_to_new_message_when_edit_fails(self):
        message = AsyncMock()
        message.chat.id = 123
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
            "arbitrage_bot.tg_bot.handlers.set_global_preference",
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