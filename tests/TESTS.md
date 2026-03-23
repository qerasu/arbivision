# Arbivision — Документация по тестам

Эта документация описывает структуру, настройку и принципы бенчмаркинга тестов в арбитражном боте **Arbivision**. Все тесты расположены в директории `tests/`.

---

## Запуск тестов

Тесты написаны с использованием встроенного модуля `unittest` библиотеки Python. Для запуска и форматирования вывода можно использовать `pytest`, либо базовый `unittest`.

### Запуск всех тестов:
```bash
# Использование кастомного скрипта запуска
PYTHONPATH=. python run_tests.py

# Использование pytest (рекомендуется)
PYTHONPATH=. pytest tests/ -v
```

### Запуск конкретного тест-сьюта или метода:
```bash
# Конкретный файл
PYTHONPATH=. pytest tests/test_normalizer_and_matcher.py -v

# Конкретный тест внутри файла
PYTHONPATH=. pytest tests/test_normalizer_and_matcher.py::MatcherServiceTests::test_auto_approves_direct_condition_match_with_non_binary_labels
```

### Пропуск Live-тестов
Некоторые тесты (`test_live_db.py`, `test_live_api.py`) требуют активного подключения к базе данных или внешним API Polymarket/Predict.Fun.
Они **автоматически пропускаются** (skipped), если не задана переменная окружения `RUN_LIVE_TESTS=1`.

```bash
# Запуск с live-тестами
RUN_LIVE_TESTS=1 PYTHONPATH=. pytest tests/ -v
```

---

## Структура тест-сьютов

Тесты разделены по модулям и компонентам, которые они проверяют. Вот краткое описание каждого файла:

### 1. Тесты сервисов бизнес-логики
- `test_normalizer_and_matcher.py` (Самый объемный сьют)
  - `NormalizerServiceTests` — проверка парсинга дат (`extract_entities`), нормализации текста и labels (`normalize_outcome_label`).
  - `MatcherServiceTests` — проверка комплексной логики сопоставления (`match_candidates`). Тестирует разные сценарии: прямое совпадение по `conditionId`, ложные совпадения (семантический diff guard), различие в числах/датах, правильный маппинг исходов.
- `test_calculator.py`
  - Проверка алгоритмов расчёта: `ArbitrageCalculator`. Учитывается жадный алгоритм обхода ордербука, расчёт `gross_profit`, `net_profit` (с учетом комиссий) и ROI.
- `test_orderbook_service.py`
  - Юнит-тесты маппинга и форматирования ордербуков. Фокус на функции `_extract_level` и перевороте (inversion) ask-ов из bid-ов.
- `test_ingestion_outcomes.py`
  - Тестирование нормализации полей (в т.ч. outcomes) из разных источников перед вставкой в базу (`IngestionService`).

### 2. Тесты алертов и уведомлений
- `test_alert_manager.py`
  - Мокирование `Preferences` и `Redis`. Проверка дедупликации (`_should_notify_redis`), генерации хэшей и самого пайплайна создания алерта.
- `test_system_notifier.py`
  - Проверка ограничений rate-limit: эвикиция (eviction) старых событий из памяти при >500 записей (предотвращение memory leak).
  - Форматирование ошибок и обрезка длинных traceback'ов (`format_error_details`).

### 3. Тесты Telegram-бота
- `test_tg_bot_commands.py`
  - Мокирование `aiogram` и базы данных. Проверка обработки команд: `/start`, `/status`, `/settings`, `/set`.
  - Валидация перехода состояний (UI state) при вводе настроек (например, перехват текста после нажатия inline кнопки настройки).
- `test_tg_preferences.py`
  - Проверка фильтров пользователя (`filter_reason_for_preferences`): `min_roi`, `max_capital`, `max_days`. Отработка граничных случаев (unknown expiry).

### 4. Тесты Worker Loop'а
- `test_worker_pairs.py`
  - Проверка оркестрации матчинга (`_upsert_market_pairs`). Поведение при создании новых пар, обновлении score и переходе пары в статус `stale`.
- `test_worker_orderbook.py`
  - Проверка логики обхода пар при расчёте профитности.

### 5. Тесты инфраструктуры
- `test_adapters.py`
  - Проверка логики парсинга API из Polymarket и Predict.Fun (мокирование `httpx`). Включает тесты `curl` fallback-механизма (`test_poly_curl_fallback`).
- `test_api_internal.py`
  - Тестирование FastAPI endpoint'ов: `/health`, `/status`, `/admin/pairs/{id}/approve` (с проверкой обработки 404).

---

## Паттерны и лучшие практики в тестах

1. **`unittest.mock.MagicMock` / `AsyncMock`**
   Широко используются для изоляции сервисов от реальной базы данных и сети. Обязательно применение `AsyncMock` для всех корутин (например, сессий БД `execute`).
   
2. **`types.SimpleNamespace`**
   Используется для создания легковесных моков объектов-моделей БД (например, `ArbOpportunity` или `Market`) без необходимости инициализации SQLAlchemy инстансов.

3. **Dependency Injection & Monkeypatching**
   Для тестов, включающих обращения к глобальным ресурсам конфигурации (`settings`) или Redis, используется `unittest.mock.patch` контекст-менеджер (или декоратор).

## Добавление нового теста
При добавлении функционала:
1. Выберите подходящий файл в `tests/` или создайте новый, например `test_my_new_service.py`.
2. Создайте класс-наследник `unittest.TestCase`.
3. Методы обязательно должны начинаться со слова `test_`.
4. Для проверки асинхронной логики:
```python
import unittest
import asyncio

class MyTests(unittest.TestCase):
    def test_async_logic(self):
        async def run_test():
            result = await my_async_func()
            self.assertEqual(result, expected)
        asyncio.run(run_test())
```
