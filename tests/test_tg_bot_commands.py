import unittest
from datetime import datetime
from datetime import timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import patch

from aiogram.exceptions import TelegramBadRequest
from arbitrage_bot.core.config import settings
from arbitrage_bot.tg_bot.bot import _apply_calc_result_to_opportunity
from arbitrage_bot.tg_bot.bot import _build_bot_commands
from arbitrage_bot.tg_bot.bot import _build_market_url
from arbitrage_bot.tg_bot.bot import _format_alert_message
from arbitrage_bot.tg_bot.bot import _is_missing_table_error
from arbitrage_bot.tg_bot.handlers import _format_inactive_chat_text
from arbitrage_bot.tg_bot.handlers import _format_unhandled_message_text
from arbitrage_bot.tg_bot.handlers import _build_home_keyboard
from arbitrage_bot.tg_bot.handlers import _build_prompt_keyboard
from arbitrage_bot.tg_bot.handlers import _build_settings_keyboard
from arbitrage_bot.tg_bot.handlers import _apply_setting_update
from arbitrage_bot.tg_bot.handlers import _format_admin_stats_text
from arbitrage_bot.tg_bot.handlers import _load_admin_stats
from arbitrage_bot.tg_bot.handlers import on_plain_text_setting
from arbitrage_bot.tg_bot.handlers import _safe_answer_callback
from arbitrage_bot.tg_bot.handlers import _safe_delete_message
from arbitrage_bot.tg_bot.handlers import _safe_edit_text
from arbitrage_bot.tg_bot.preferences import format_status_text
from sqlalchemy.exc import ProgrammingError


class TelegramBotCommandsTests(unittest.TestCase):
    def test_build_home_keyboard_contains_pause_and_settings_buttons(self):
        keyboard = _build_home_keyboard()

        self.assertEqual(keyboard.inline_keyboard[0][0].text, "⏸ Pause")
        self.assertEqual(keyboard.inline_keyboard[1][0].text, "Settings")
        self.assertEqual(len(keyboard.inline_keyboard), 2)


    def test_build_settings_keyboard_contains_expected_actions(self):
        keyboard = _build_settings_keyboard()

        self.assertEqual(keyboard.inline_keyboard[0][0].text, "→ Min ROI")
        self.assertEqual(keyboard.inline_keyboard[0][1].text, "→ Min volume")
        self.assertEqual(keyboard.inline_keyboard[1][0].text, "→ Max volume")
        self.assertEqual(keyboard.inline_keyboard[1][1].text, "→ Min profit")
        self.assertEqual(keyboard.inline_keyboard[2][0].text, "→ Polymarket balance")
        self.assertEqual(keyboard.inline_keyboard[2][1].text, "→ Predict.Fun balance")
        self.assertEqual(keyboard.inline_keyboard[3][0].text, "→ Max market end")
        self.assertEqual(keyboard.inline_keyboard[4][0].text, "Reset all")
        self.assertEqual(keyboard.inline_keyboard[4][1].text, "← Back")


    def test_build_settings_keyboard_shows_disable_all_filters_only_for_admin_chat(self):
        original_system_error_chat_ids = settings.TELEGRAM_SYSTEM_ERROR_CHAT_IDS
        settings.TELEGRAM_SYSTEM_ERROR_CHAT_IDS = ["123"]
        try:
            admin_keyboard = _build_settings_keyboard(chat_id=123)
            user_keyboard = _build_settings_keyboard(chat_id=456)
        finally:
            settings.TELEGRAM_SYSTEM_ERROR_CHAT_IDS = original_system_error_chat_ids

        self.assertEqual(admin_keyboard.inline_keyboard[4][0].text, "⛔ Disable all filters")
        self.assertEqual(admin_keyboard.inline_keyboard[5][0].text, "Reset all")
        self.assertEqual(user_keyboard.inline_keyboard[4][0].text, "Reset all")


    def test_build_home_keyboard_shows_resume_when_muted(self):
        keyboard = _build_home_keyboard({"muted": True})

        self.assertEqual(keyboard.inline_keyboard[0][0].text, "▶️ Resume")


    def test_build_home_keyboard_uses_russian_when_language_is_ru(self):
        keyboard = _build_home_keyboard({"muted": False, "language": "ru"})

        self.assertEqual(keyboard.inline_keyboard[0][0].text, "⏸ Пауза")
        self.assertEqual(keyboard.inline_keyboard[1][0].text, "Настройки")


    def test_build_home_keyboard_shows_stats_only_for_admin_chat(self):
        original_system_error_chat_ids = settings.TELEGRAM_SYSTEM_ERROR_CHAT_IDS
        settings.TELEGRAM_SYSTEM_ERROR_CHAT_IDS = ["123"]
        try:
            keyboard = _build_home_keyboard(chat_id=123)
        finally:
            settings.TELEGRAM_SYSTEM_ERROR_CHAT_IDS = original_system_error_chat_ids

        self.assertEqual(keyboard.inline_keyboard[2][0].text, "Stats")


    def test_format_admin_stats_text_contains_expected_sections(self):
        text = _format_admin_stats_text(
            {
                "users": {"total": 3, "active": 2, "paused": 1},
                "alerts": {"sent": 10, "dropped": 4},
                "alert_drop_reasons": [
                    {"reason": "cancelled_after_revalidation", "count": 3},
                ],
                "runtime_alert_drop_reasons": {
                    "cancelled_by_updated_preferences": 2,
                    "send_failed": 1,
                },
                "runtime_opportunity_filter_reasons": {
                    "min_roi": 5,
                },
            }
        )

        self.assertIn("📊 Bot stats", text)
        self.assertIn("👥 Users:", text)
        self.assertIn("• 🧮 Total: 3", text)
        self.assertIn("• ✅ Active: 2", text)
        self.assertIn("• ⏸ Paused: 1", text)
        self.assertIn("🚨 Alerts:", text)
        self.assertIn("• 📤 Sent: 10", text)
        self.assertIn("• 🗑 Dropped: 4", text)
        self.assertIn("🧾 Alert cancellations/failures (all time, DB):", text)
        self.assertIn("• cancelled_after_revalidation: 3", text)
        self.assertIn("⚙️ Delivery cancellations (since restart, runtime):", text)
        self.assertIn("• cancelled_by_updated_preferences: 2", text)
        self.assertIn("• send_failed: 1", text)
        self.assertIn("🧹 Fanout filter blocks (since restart, runtime):", text)
        self.assertIn("• min_roi: 5", text)


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
            calculation_json={},
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
        self.assertIn("• NO on Polymarket: effective price $0.360 = $18", text)
        self.assertIn("• YES on Predict.Fun: effective price $0.500 = $25", text)
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
            calculation_json={},
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

        self.assertIn("• Grizzlies on Polymarket: effective price $0.360 = $18", text)
        self.assertIn("• Hornets on Predict.Fun: effective price $0.500 = $25", text)


    def test_format_alert_message_shows_best_ask_when_it_differs_from_avg_fill(self):
        opportunity = SimpleNamespace(
            direction="A_yes_B_no",
            net_profit=9.77,
            net_roi=0.0651,
            capital_required=150.0,
            shares=159.77,
            avg_price_leg_1=0.631,
            avg_price_leg_2=0.308,
            calculation_json={
                "best_price_leg_1": 0.62,
                "best_price_leg_2": 0.30,
            },
        )
        pair = SimpleNamespace(
            outcome_mapping_json={
                "market_a": {"yes_label": "Yes", "no_label": "No"},
                "market_b": {"yes_label": "Yes", "no_label": "No"},
            },
        )
        market_a = SimpleNamespace(
            platform="polymarket",
            title="Brentford FC vs. Everton FC: Draw at halftime?",
            slug="bre-eve-draw-at-halftime",
            raw_payload_json={"endDate": "2026-04-11T00:00:00+00:00"},
        )
        market_b = SimpleNamespace(
            platform="predict_fun",
            title="Brentford FC vs. Everton FC: Draw at halftime?",
            slug="epl-bre-eve-2026-04-11",
            platform_market_id="pf-123",
            raw_payload_json={"resolveDate": "2026-04-11T00:00:00+00:00"},
        )

        with patch("arbitrage_bot.tg_bot.bot.datetime") as datetime_mock:
            datetime_mock.now.return_value = datetime(2026, 4, 6, tzinfo=timezone.utc)
            text = _format_alert_message(opportunity, pair, market_a, market_b)

        self.assertIn("effective price $0.631 (best ask $0.620)", text)
        self.assertIn("effective price $0.308 (best ask $0.300)", text)


    def test_build_market_url_expands_relative_raw_path(self):
        market = SimpleNamespace(
            platform="predict_fun",
            slug="ignored-slug",
            raw_payload_json={"marketUrl": "/market/epl-bre-eve-2026-04-11"},
        )

        url = _build_market_url(market)

        self.assertEqual(url, "https://predict.fun/market/epl-bre-eve-2026-04-11?ref=077A2")


    def test_format_status_text_contains_status_summary(self):
        text = format_status_text(
            {
                "min_roi_percent": None,
                "min_capital_usd": None,
                "max_capital_usd": None,
                "max_polymarket_capital_usd": None,
                "max_predict_fun_capital_usd": None,
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
                "max_polymarket_capital_usd": None,
                "max_predict_fun_capital_usd": None,
                "min_profit_usd": None,
                "max_days_to_close": None,
                "muted": True,
            }
        )

        self.assertIn("🔴 Status: Paused", text)
        self.assertIn("📭 Telegram alerts are paused.", text)


    def test_format_inactive_chat_text_prompts_start(self):
        self.assertIn("/start", _format_inactive_chat_text())


    def test_format_inactive_chat_text_uses_russian_when_language_is_ru(self):
        text = _format_inactive_chat_text(language="ru")

        self.assertIn("Нажмите /start", text)


    def test_format_unhandled_message_text_explains_button_flow(self):
        text = _format_unhandled_message_text(
            {
                "min_roi_percent": 5.0,
                "min_capital_usd": 10.0,
                "max_capital_usd": 150.0,
                "max_polymarket_capital_usd": None,
                "max_predict_fun_capital_usd": None,
                "min_profit_usd": None,
                "max_days_to_close": 5,
                "muted": False,
            }
        )

        self.assertIn("buttons below", text.lower())
        self.assertIn("Settings", text)
        self.assertIn("Arbitrage Scanner", text)


    def test_format_alert_message_uses_russian_when_language_is_ru(self):
        opportunity = SimpleNamespace(
            direction="A_no_B_yes",
            net_profit=7.0,
            net_roi=0.14,
            capital_required=43.0,
            shares=50.0,
            avg_price_leg_1=0.36,
            avg_price_leg_2=0.50,
            calculation_json={},
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

        with patch("arbitrage_bot.tg_bot.bot.datetime") as datetime_mock:
            datetime_mock.now.return_value = datetime(2026, 3, 21, tzinfo=timezone.utc)
            text = _format_alert_message(opportunity, pair, market_a, market_b, language="ru")

        self.assertIn("Прибыль", text)
        self.assertIn("Объём", text)
        self.assertIn("Завершится", text)
        self.assertIn("Открыть рынки", text)


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


class FakeRowResult:
    def __init__(self, row):
        self.row = row


    def one(self):
        return self.row


    def all(self):
        return self.row


class FakeAdminStatsSession:
    def __init__(self):
        self.compiled_statements = []
        self.execute_calls = 0


    async def execute(self, stmt):
        self.compiled_statements.append(str(stmt))
        self.execute_calls += 1

        if self.execute_calls == 1:
            return FakeRowResult(SimpleNamespace(total=1, paused=0))
        if self.execute_calls == 2:
            return FakeRowResult(SimpleNamespace(sent=0, dropped=0))
        if self.execute_calls == 3:
            return FakeRowResult([])

        raise AssertionError(f"unexpected stmt: {stmt}")


class TelegramBotSettingsUpdateTests(unittest.IsolatedAsyncioTestCase):
    async def test_load_admin_stats_counts_registered_chats_from_telegram_chats(self):
        session = FakeAdminStatsSession()

        with patch(
            "arbitrage_bot.tg_bot.handlers.snapshot_counters",
            return_value={},
        ):
            stats = await _load_admin_stats(session)

        self.assertEqual(stats["users"]["total"], 1)
        self.assertIn("FROM telegram_chats", session.compiled_statements[0])
        self.assertNotIn("FROM subscriptions", session.compiled_statements[0])


    async def test_load_admin_stats_separates_alert_and_filter_runtime_reasons(self):
        session = FakeAdminStatsSession()

        with patch(
            "arbitrage_bot.tg_bot.handlers.snapshot_counters",
            return_value={
                "fanout.drop.max_capital": 2,
                "telegram.alert_cancelled_preferences": 1,
                "telegram.alert_send_failed": 3,
            },
        ):
            stats = await _load_admin_stats(session)

        self.assertEqual(
            stats["runtime_alert_drop_reasons"],
            {
                "cancelled_by_updated_preferences": 1,
                "send_failed": 3,
            },
        )
        self.assertEqual(
            stats["runtime_opportunity_filter_reasons"],
            {
                "max_capital": 2,
            },
        )


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
                    "max_polymarket_capital_usd": None,
                    "max_predict_fun_capital_usd": None,
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
                    "max_polymarket_capital_usd": None,
                    "max_predict_fun_capital_usd": None,
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


    async def test_on_plain_text_setting_answers_with_menu_for_registered_chat(self):
        message = AsyncMock()
        message.chat.id = 123
        message.text = "hello"

        with patch(
            "arbitrage_bot.tg_bot.handlers.AsyncSessionLocal",
            return_value=FakeSessionContext(),
        ), patch(
            "arbitrage_bot.tg_bot.handlers.get_ui_state",
            new=AsyncMock(return_value={}),
        ), patch(
            "arbitrage_bot.tg_bot.handlers._has_started_bot",
            new=AsyncMock(return_value=True),
        ), patch(
            "arbitrage_bot.tg_bot.handlers.get_user_preferences",
            new=AsyncMock(
                return_value={
                    "min_roi_percent": 5.0,
                    "min_capital_usd": 10.0,
                    "max_capital_usd": 150.0,
                    "max_polymarket_capital_usd": None,
                    "max_predict_fun_capital_usd": None,
                    "min_profit_usd": None,
                    "max_days_to_close": 5,
                    "muted": False,
                }
            ),
        ), patch(
            "arbitrage_bot.tg_bot.handlers.clear_ui_state",
            new=AsyncMock(),
        ) as clear_mock:
            await on_plain_text_setting(message)

        message.answer.assert_awaited_once()
        sent_text = message.answer.await_args.args[0]
        self.assertIn("buttons below", sent_text.lower())
        clear_mock.assert_awaited_once()


    async def test_on_plain_text_setting_prompts_start_for_unregistered_chat(self):
        message = AsyncMock()
        message.chat.id = 123
        message.text = "hello"

        with patch(
            "arbitrage_bot.tg_bot.handlers.AsyncSessionLocal",
            return_value=FakeSessionContext(),
        ), patch(
            "arbitrage_bot.tg_bot.handlers.get_ui_state",
            new=AsyncMock(return_value={}),
        ), patch(
            "arbitrage_bot.tg_bot.handlers._has_started_bot",
            new=AsyncMock(return_value=False),
        ), patch(
            "arbitrage_bot.tg_bot.handlers.get_user_preferences",
            new=AsyncMock(),
        ) as preferences_mock, patch(
            "arbitrage_bot.tg_bot.handlers.get_user_language",
            new=AsyncMock(return_value="en"),
        ):
            await on_plain_text_setting(message)

        message.answer.assert_awaited_once_with(_format_inactive_chat_text(language="en"))
        preferences_mock.assert_not_awaited()


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