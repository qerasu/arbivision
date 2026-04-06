# Arbivision — Документация

Arbivision — арбитражный бот, который мониторит prediction-маркеты на **Polymarket** и **Predict.Fun**, находит пары одинаковых событий на обеих площадках, сканирует ордербуки и отправляет Telegram-алерты при обнаружении арбитражных возможностей (когда суммарная цена покупки обоих исходов < 1).

---

## Содержание

- [Архитектура](#архитектура)
- [Жизненный цикл запуска](#жизненный-цикл-запуска)
- [Core-модули](#core-модули)
- [Адаптеры (adapters)](#адаптеры-adapters)
- [Сервисы (services)](#сервисы-services)
  - [IngestionService](#ingestionservice)
  - [NormalizerService](#normalizerservice)
  - [MatcherService](#matcherservice)
  - [OrderbookService](#orderbookservice)
  - [ArbitrageCalculator](#arbitragecalculator)
  - [AlertManager](#alertmanager)
  - [SystemNotifier](#systemnotifier)
- [Telegram-бот](#telegram-бот)
  - [bot.py — ядро бота](#botpy--ядро-бота)
  - [handlers.py — команды и навигация](#handlerspy--команды-и-навигация)
  - [preferences.py — фильтры и настройки](#preferencespy--фильтры-и-настройки)
- [Worker — основной рабочий цикл](#worker--основной-рабочий-цикл)
- [API](#api)
- [ORM-модели](#orm-модели)
- [Конфигурация](#конфигурация)
- [Тесты](#тесты)

---

## Архитектура

```
┌─────────────────────────────────────────────────────────────────────┐
│                           FastAPI (main.py)                        │
│                      /api/health, /api/status, /admin              │
└────────────────────┬───────────────────────┬────────────────────────┘
                     │                       │
                     ▼                       ▼
         ┌──────────────────┐     ┌──────────────────────┐
         │  Worker Loop     │     │   Telegram Bot        │
         │  (worker.py)     │     │   (bot.py)            │
         │                  │     │                       │
         │  1. Ingestion    │     │  • Polling (aiogram)  │
         │  2. Matching     │     │  • Alert sender       │
         │  3. Orderbooks   │     │  • User commands      │
         │  4. Calculator   │     │  • Settings UI        │
         │  5. AlertManager │     │                       │
         └────────┬─────────┘     └───────────┬───────────┘
                  │                            │
                  ▼                            ▼
         ┌──────────────┐            ┌──────────────────┐
         │  PostgreSQL  │            │      Redis       │
         │  (async ORM) │            │  (dedupe cache)  │
         └──────────────┘            └──────────────────┘
```

По умолчанию все компоненты запускаются как asyncio-таски внутри FastAPI `lifespan`. При shutdown каждый таск корректно отменяется. Для подготовки к масштабированию также поддержан split-runtime режим через `APP_RUNTIME_MODE`.

---

## Жизненный цикл запуска

**Скрипт `start.py`:**
1. Загружает `.env` из `~/.config/arbivision/.env`
2. Поднимает Docker-контейнеры (PostgreSQL + Redis) через `docker compose up -d`
3. Ожидает TCP/Postgres-ready и запускает Alembic-миграции с ретраями
4. Стартует Uvicorn с `--reload` для разработки
5. Пишет PID в tempfile для корректной остановки

**`main.py` (FastAPI lifespan):**
1. Создаёт `asyncio.Task` для `run_sync_loop()` (worker)
2. Создаёт `asyncio.Task` для `start_polling()` (Telegram-бот)
3. При shutdown отменяет оба таска и закрывает shared-сессию system_notifier

**Split runtime:**
1. `APP_RUNTIME_MODE=all` — worker + fanout + telegram внутри API-процесса
2. `APP_RUNTIME_MODE=worker` — внутри `uvicorn` запускается только worker
3. `APP_RUNTIME_MODE=fanout` — внутри `uvicorn` запускается только fanout loop
4. `APP_RUNTIME_MODE=telegram` — внутри `uvicorn` запускается только telegram loop
5. Для полностью раздельного запуска можно использовать:
   `python -m arbitrage_bot.run_worker`
   `python -m arbitrage_bot.run_fanout`
   `python -m arbitrage_bot.run_telegram`
   `uvicorn arbitrage_bot.api_app:app`

**Скрипт `stop.py`:**
1. Читает PID из tempfile, отправляет SIGTERM → SIGKILL
2. Останавливает Docker-контейнеры через `docker compose stop`
*Примечание: запуск `python3 stop.py --drop` полностью удалит контейнеры и данные БД (volume) через `docker compose down -v`. Можно добавить флаг `--yes` для пропуска подтверждения.*

---

## Core-модули

### config.py
Централизованный конфигурационный синглтон. При импорте автоматически загружает `.env` файл и парсит все настройки.

Настройка | Описание | По умолчанию
----------|---------|------------
`POSTGRES_*` | Параметры подключения к PostgreSQL | `localhost:5432`
`REDIS_*` | Параметры подключения к Redis | `localhost:6379`
`PREDICT_FUN_API_KEY` | API-ключ для Predict.Fun | (пусто)
`FEE_POLYMARKET_BPS` | Комиссия Polymarket в BPS | 100.0 (1%)
`FEE_PREDICT_FUN_BPS` | Комиссия Predict.Fun в BPS | 200.0 (2%)
`MARKET_REFRESH_SECONDS` | Интервал синхронизации рынков | 60 сек
`EMPTY_ORDERBOOK_THRESHOLD` | Лимит пустых стаканов до полного игнорирования пары | 3
`ORDERBOOK_CACHE_TTL_SECONDS` | TTL in-memory кеша ордербуков | 2 сек
`ORDERBOOK_CACHE_MAX_ITEMS` | Максимальный размер кеша ордербуков | 2048
`ORDERBOOK_POLYMARKET_BATCH_SIZE` | Размер batch-запроса CLOB-книг Polymarket | 200
`ORDERBOOK_PREDICT_FUN_CONCURRENCY` | Конкурентность запросов Predict.Fun orderbook | 4
`ALERTS_DEDUPE_TTL_SECONDS` | TTL дедупликации алертов в Redis | 600 сек
`ALERTS_DELTA_PROFIT_THRESHOLD_USD` | Минимальный прирост профита для повторного алерта | $3
`ALERTS_DELTA_ROI_THRESHOLD_PERCENT` | Минимальный прирост ROI для повторного алерта | 0.5%
`TELEGRAM_BOT_TOKEN` | Токен Telegram-бота | (пусто)
`TELEGRAM_DEFAULT_CHAT_IDS` | Чаты для отправки алертов (через запятую) | (пусто)
`TELEGRAM_SYSTEM_ERROR_CHAT_IDS` | Чаты для системных ошибок | (пусто)
`TELEGRAM_SYSTEM_ERROR_COOLDOWN_SECONDS` | Кулдаун между одинаковыми ошибками | 300 сек

### database.py
Настройка асинхронного SQLAlchemy engine через `asyncpg`. Экспортирует:
- `engine` — asyncio-движок
- `AsyncSessionLocal` — фабрика сессий
- `get_db()` — dependency для FastAPI

### redis.py
Глобальный Redis connection pool. `get_redis()` возвращает async-клиент.

### logging.py
Настройка `structlog` с ISO timestamps и консольным выводом.

### env_loader.py
Утилита для ручного парсинга `.env` файла из `~/.config/arbivision/.env`. Устанавливает переменные в `os.environ`, не перезаписывая уже существующие.

---

## Адаптеры (adapters)

### BaseAdapter (base.py)
Абстрактный базовый класс. Определяет интерфейс:
- `fetch_markets()` — получить список рынков
- `fetch_orderbook(market_id)` — получить ордербук для конкретного рынка

### PolymarketAdapter (polymarket.py)
Получает данные с Polymarket через их REST API.

**Ключевые методы:**
- `fetch_markets()` — запрашивает все активные рынки через paginated API (`/markets`). Сдвигается по `next_cursor`. Возвращает массив рынков.
- `fetch_orderbook(token_id)` — получает ордербук по `token_id`.
- `fetch_books(token_ids)` — batch-запрос ордербуков для нескольких token ID (используется для получения обоих сторон — yes и no).

**Curl fallback:** при ошибках `httpx` (ConnectTimeout, ConnectError и др.) адаптер автоматически переключается на `curl` через `asyncio.create_subprocess_exec`. Это обеспечивает устойчивость при проблемах с сетевым стеком Python.

### PredictFunAdapter (predict_fun.py)
Получает данные с Predict.Fun. Требует API-ключ (заголовок `x-api-key`).

**Ключевые методы:**
- `fetch_markets()` — cursor-based пагинация через `/api/markets`. Фильтрует только открытые рынки (`tradingStatus == "OPEN"`).
- `fetch_orderbook(market_id)` — получает ордербук через `/api/markets/{id}/orderbook`.

Также имеет curl fallback, аналогично PolymarketAdapter.

---

## Сервисы (services)

### IngestionService

**Файл:** `services/ingestion.py`
**Задача:** синхронизация рынков с обеих площадок → запись в БД.

**Основной метод — `sync_markets()`:**
1. Параллельно (`asyncio.gather`) запрашивает рынки с Polymarket и Predict.Fun
2. Для каждого источника маппит сырые данные в унифицированный формат
3. Выполняет upsert (вставка или обновление) в таблицу `markets`

**Маппинг рынков:**
Каждый рынок нормализуется в единую структуру:
```
platform, platform_market_id, status, tradable, 
title, normalized_title (lowercase),
description, outcomes_json, raw_payload_json,
category, slug
```

**Нормализация outcomes:** исходы из разных площадок приводятся к формату:
```json
{"id": "...", "label": "Yes", "slug": "yes"}
```
Распознаются разные ключи API: `token_id`, `tokenId`, `onChainId`, `asset_id` и т.д.

**Upsert-стратегия:**
1. Первый проход: батчевый SELECT существующих рынков по `(platform, platform_market_id)`. Батчи по 1000.
2. Обновление или вставка в рамках одного flush.
3. При `IntegrityError` (race condition) — fallback на по-одиночную обработку с `begin_nested()`.

---

### NormalizerService

**Файл:** `services/normalizer.py`
**Задача:** нормализация текста для сравнения.

- `normalize_text(text)` — lowercase + удаление спецсимволов + collapse whitespace
- `extract_entities(text)` — извлечение дат (паттерн: `"March 15, 2026"`) и чисел. Числа, входящие в даты, исключаются из `numbers`.
- `normalize_outcome_label(value)` — приведение меток исходов: `"y" → "yes"`, `"n" → "no"`, `"tie" → "draw"`

---

### MatcherService

**Файл:** `services/matcher.py` (~800 строк)
**Задача:** определить, описывают ли два рынка с разных площадок одно и то же событие, и построить маппинг исходов.

Алгоритм работает в несколько этапов:

#### 1. Построение сигнатуры рынка (`build_market_signature`)

Для каждого рынка строится «сигнатура» — набор признаков:
- `title_tokens` — множество значимых слов из нормализованного заголовка (без стоп-слов)
- `tokens` — расширенное множество (добавлены токены из участников)
- `category_tokens` — токены из категории рынка
- `condition_ids` — ID условий (Polymarket `conditionId`, Predict.Fun `polymarketConditionIds`)
- `entities` — извлечённые даты и числа
- `participants` — извлечённые участники (команды, спортсмены)
- `kind` — тип рынка: `matchup` (A vs B), `proposition` ("Will X win...") или `generic`

#### 2. Быстрый отбор кандидатов (`build_candidate_index`, `_candidate_markets_for_poly`)

Для всех Predict.Fun рынков строится inverted index по токенам и `condition_ids`. Для каждого Polymarket рынка:
1. **Прямой матч:** если есть общие `condition_ids` — кандидаты возвращаются сразу (100% совпадение)
2. **Токенный матч:** собираются кандидаты с общими токенами, ранжируются по `candidate_rank_score` (weighted: shared tokens + category overlap + date overlap + number overlap + participant score + kind match), берутся топ-25

#### 3. Детальное сравнение (`explain_match`)

Для каждой пары кандидатов:

**Быстрые фильтры (early rejection):**
- `market_shape_mismatch` — matchup нельзя матчить с proposition
- `date_mismatch` — если оба рынка содержат даты и они не совпадают
- `number_mismatch` — аналогично для чисел
- `empty_title_tokens` — нет слов для сравнения

**Scoring:**
- `title_score` — Jaccard index между title-tokens
- `participant_score` — Jaccard index по токенам участников с учётом subset-бонуса (0.8 минимум при полном включении)
- `score` — max из трёх вариантов: чистый title_score, взвешенная сумма (0.45 × title + 0.55 × participant), чистый participant_score

**Семантический diff guard (`_has_meaningful_title_difference`):**
Если symmetric difference слов содержит **semantic qualifiers** (ordinals: first/second/third, rankings: largest/smallest, gender: men/women, structure: singles/doubles), матч блокируется. Это предотвращает ложные положительные результаты типа:
- "NVIDIA third-largest company" ≠ "NVIDIA largest company"
- "Women's NCAA" ≠ "Men's NCAA"

**Условия автоматического одобрения (`_should_auto_approve`):**
1. Outcome mapping должен существовать
2. Нет meaningful title difference
3. `score ≥ 0.85` и полное совпадение title_tokens, **или**
4. `participant_score ≥ 0.8` и оба рынка — matchup, **или**
5. `participant_score ≥ 0.95`, одинаковый kind, и `title_jaccard ≥ 0.5` (для non-matchup)

#### 4. Построение outcome mapping (`_build_outcome_mapping`)

Определяет, какой исход на одной площадке соответствует какому на другой:

1. **Оба binary (Yes/No):** прямой маппинг
2. **Один binary, другой named:** если binary — вопрос "Will A beat B?", а named — "A vs B", ищется Yes → A, No → B через label matching
3. **Оба named (2 исхода):** сначала по нормализованным label, затем по Jaccard token matching с порогом 0.5

Результат:
```json
{
  "market_a": {"yes": "poly-y", "no": "poly-n", "yes_label": "Yes", "no_label": "No"},
  "market_b": {"yes": "pf-a", "no": "pf-b", "yes_label": "Grizzlies", "no_label": "Hornets"},
  "is_inverted": false,
  "confidence": "high"
}
```

---

### OrderbookService

**Файл:** `services/orderbook.py`
**Задача:** загрузить ордербуки для всех утверждённых пар и подготовить направления для калькулятора.

**`fetch_orderbooks_for_pairs(pairs, db_session)`:**
1. Загружает маппинг `market_id → (platform, platform_market_id)` из БД
2. Параллельно загружает Predict.Fun orderbook'и с ограничением по `ORDERBOOK_PREDICT_FUN_CONCURRENCY`
3. Batch'ами получает нужные Polymarket CLOB-книги
4. Переиспользует короткий in-memory TTL-кеш, чтобы не бить API повторно в соседних циклах

**`diagnose_pair(pair, ...)`:**
Отдельный диагностический путь для admin API. Позволяет понять, почему конкретная пара отвалилась на этапе orderbook:
- `missing_platform_market_id`
- `predict_fun_market_not_found`
- `predict_fun_fetch_failed`
- `missing_outcome_mapping`
- `predict_fun_yes_asks_missing`
- `predict_fun_no_asks_missing`
- `polymarket_yes_asks_missing`
- `polymarket_no_asks_missing`

**`_build_direction_books(pair, pf_orderbook)` — ключевая логика:**

Для арбитража нам нужны два «направления»:
- **A_yes_B_no:** покупаем YES на Polymarket + покупаем NO на Predict.Fun
- **A_no_B_yes:** покупаем NO на Polymarket + покупаем YES на Predict.Fun

**Проблема:** Predict.Fun может не предоставлять прямой ордербук для NO-исхода. Solution: NO-asks вычисляются как инверсия YES-bids:
```
no_ask_price = 1.0 - yes_bid_price
no_ask_size = yes_bid_size
```

Polymarket же имеет отдельные CLOB-токены для YES и NO, поэтому для него делается batch-запрос `fetch_books([yes_token_id, no_token_id])`.

**Парсинг уровней (`_extract_level`):**
Поддерживаются форматы:
- dict: `{"price": 0.45, "size": 100}`
- dict с aliases: `{"p": 0.45, "s": 100}`, `{"rate": 0.45, "quantity": 100}`
- list/tuple: `[0.45, 100]`
- dict-as-list (ключи = цены): `{"0.45": 100}`

---

### ArbitrageCalculator

**Файл:** `services/calculator.py`
**Задача:** пройти по ордербукам обоих бирж и рассчитать профит.

**Алгоритм `calculate_opportunity(poly_asks, pf_asks)`:**

Это greedy-алгоритм "прохода по ордербуку":

```
poly_asks = [(0.40, 500), (0.42, 300), ...]   # sorted by price ASC
pf_asks   = [(0.45, 400), (0.47, 200), ...]   # sorted by price ASC
```

На каждом шаге:
1. Берём текущий уровень на обеих биржах: `(p_price, p_size)` и `(f_price, f_size)`
2. Учитываем комиссии: вычисляем `net_p_price` и `net_f_price`
3. Если `net_p_price + net_f_price ≥ 1.0` — арбитража нет или он убыточен, выходим
4. Если `< 1.0` — есть spread (покупаем shares):
   - `take_size = min(p_size, f_size)` — сколько shares можно купить
   - Аккумулируем: shares, cost_poly, cost_pf, capital
   - Уменьшаем размер уровней, переходим к следующему при исчерпании

*Примечание: если после сбора всех доступных ордеров и вычета всех комиссий итоговая `net_profit <= 0`, калькулятор возвращает `None` и такая возможность сразу отбрасывается.*

**Результат:**
```python
{
    "shares": 500,              # общее количество shares
    "capital_required": 425.0,  # суммарный cost
    "avg_price_leg_1": 0.41,    # средневзвешенная цена на Polymarket
    "avg_price_leg_2": 0.44,    # средневзвешенная цена на Predict.Fun
    "gross_profit": 75.0,       # shares * 1.0 - capital (при выигрыше получаем $1 за share)
    "net_profit": 73.5,         # gross_profit - комиссии
    "gross_roi": 0.176,         # gross_profit / capital
    "net_roi": 0.173,           # net_profit / capital
}
```

**`calculate_opportunities(direction_books)`:**
Вызывает `calculate_opportunity` для каждого направления (`A_yes_B_no`, `A_no_B_yes`) и добавляет ключ `direction` к результату.

---

### AlertManager

**Файл:** `services/alert_manager.py`
**Задача:** принять рассчитанную возможность, применить фильтры, дедуплицировать и создать алерт в БД.

**`process_opportunity(pair, calc_result, market_a, market_b, preferences)`:**

1. **Глобальная фильтрация по preferences:**
   - min_roi — если ROI ниже порога пользователя
   - min_capital — если объём сделки ниже минимального порога
   - max_capital — если объём сделки превышает порог
   - min_profit — если ожидаемый профит ниже порога
   - max_days_to_close — если рынок закрывается слишком нескоро (или дата неизвестна)

2. **Smart deduplication (Redis):**
   - Ключ: `alert-dedupe:{pair_hash}:{direction}`
   - При наличии предыдущего алерта: пропускаем, если `profit_diff < $3` **и** `roi_diff < 0.5%`
   - Это позволяет отправить повторный алерт, только если возможность стала значительно лучше

3. **Сохранение в БД:**
   - Создаётся `ArbOpportunity` (расчёт сохраняется)
   - Fanout идет через `Subscription` + `UserPreference`, а `TELEGRAM_DEFAULT_CHAT_IDS` используется только как legacy fallback
   - Для подходящих Telegram-target'ов создаются `Alert` со статусом `"queued"`
   - Обновляется dedupe-кэш в Redis

4. **Rollback-safety:** если commit в БД упал, dedupe-ключ из Redis удаляется (иначе можно потерять алерт навсегда)

---

### SystemNotifier

**Файл:** `services/system_notifier.py`
**Задача:** отправлять уведомления об ошибках в Telegram.

**Ключевые механизмы:**

- **Дедупликация по кулдауну:** одинаковые ошибки (по source + operation + type + details) не дублируются в течение `TELEGRAM_SYSTEM_ERROR_COOLDOWN_SECONDS` (по умолчанию 5 минут)
- **Eviction:** словарь `_last_sent_at` автоматически чистится при превышении 500 записей (удаляется 50% самых старых)
- **Shared bot:** используется отдельный экземпляр `Bot`, сессия которого корректно закрывается при shutdown через `close_shared_bot()`
- **Форматирование:** SQL-шум, URL response info и длинные строки обрезаются до 280 символов

---

## Telegram-бот

### bot.py — ядро бота

**`start_polling()` — главный цикл:**
1. Создаёт `Bot` и `Dispatcher` через `setup_bot()`
2. Запускает `_drain_queued_alerts(bot)` как фоновую задачу
3. Стартует long-polling через `dp.start_polling(bot)`
4. При ошибке — логирует, уведомляет и пытается перезапуститься через 5 секунд

**`_drain_queued_alerts(bot)` — отправщик алертов:**
Бесконечный цикл с интервалом `TELEGRAM_ALERTS_POLL_SECONDS` (0.5 сек):
1. Выбирает до 20 алертов со статусом `"queued"` или `"retry"` через JOIN:
   `Alert → ArbOpportunity → MarketPair → Market (A) + Market (B)`
2. Для каждого формирует HTML-сообщение и отправляет
3. **Retry-логика:**
   - Первая ошибка → статус `"retry"` + ожидание 3 секунды + повтор
   - Вторая ошибка → статус `"failed"`
4. При `ProgrammingError` (таблица не существует) — ждёт миграций

**Формат алерта:**
```
🚨 [Заголовок рынка]

💰 Profit: $75
📈 Spread: 17.30%
💵 Volume: $425
⏳ Max market end: 2026-04-15 (23 days)

🧾 Buy 500 shares each:
• Yes on Polymarket @ $0.410 = $205
• No on Predict.Fun @ $0.440 = $220
📊 Voluemes ratio: 1.07x

🔗 Open markets:
Polymarket | Predict.Fun
```

**Volumes ratio** — отношение бо́льшей цены share к меньшей. Нужно для быстрого пересчета количества shares под нужный пользователю объем.

Link preview отключён через `LinkPreviewOptions(is_disabled=True)`.

### handlers.py — команды и навигация

**Команды бота:**
- `/start` — главное меню с кнопками Pause/Resume и Settings
- `/settings` — просмотр и изменение фильтров
- `/set roi 1.5` — минимальный ROI
- `/set minvolume 50` — минимальный объем сделки в USD
- `/set volume 500` — максимальный объем сделки в USD
- `/set profit 10` — минимальный профит в USD
- `/set expires 30` — максимум дней до закрытия рынка
- для `minvolume`, `volume`, `profit`, `expires` поддерживается `off`

**Inline-навигация (callback queries):**
- `tg_nav:home` → главное меню
- `tg_nav:settings` → настройки
- `tg_nav:toggle_mute` → пауза/возобновление алертов
- `tg_nav:reset` → сброс всех фильтров
- `tg_edit:min_roi_percent` → промпт для ввода значения

**UI State Machine:**
При нажатии кнопки "→ Min ROI" бот входит в режим `awaiting_value`:
1. `set_ui_state(session, chat_id, {"mode": "awaiting_value", "field_name": "min_roi_percent", "prompt_message_id": 123})`
2. Следующее текстовое сообщение обрабатывается как значение настройки
3. Бот обновляет сообщение-промпт (edit) вместо создания нового
4. UI state очищается

### preferences.py — фильтры и настройки

**Основные настройки пользователя хранятся в `user_preferences`, а UI state и legacy global fallback — в `settings`:**
```python
{
    "min_roi_percent": 1,
    "min_capital_usd": 10,
    "max_capital_usd": 150,
    "min_profit_usd": None,
    "max_days_to_close": 5,
    "muted": False,
}
```

**`filter_reason_for_preferences(opportunity, market_a, market_b, preferences)`:**
Проверяет фильтры последовательно:
1. `min_roi` — ROI opportunity < порога
2. `min_capital` — capital_required < минимального порога
3. `max_capital` — capital_required > порога
4. `min_profit` — net_profit < порога
5. `max_days_to_close` — дата закрытия рынка > порога. Если дата неизвестна — фильтруется.

**Извлечение даты закрытия (`extract_pair_close_datetime`):**
Берётся максимальная дата из обоих рынков. Парсер проверяет ~20 полей (`endDate`, `resolveDate`, `closedTime`...) и поддерживает ISO-строки, Unix timestamps (секунды и миллисекунды).

---

## Worker — основной рабочий цикл

**Файл:** `worker.py`
**Функция:** `run_sync_loop()` — бесконечный цикл с интервалом `MARKET_REFRESH_SECONDS`.

Каждая итерация:

```
1. sync_markets()          — ingestion: загрузка рынков с обеих площадок в БД
       ↓
2. _upsert_market_pairs()  — matching: поиск пар одинаковых рынков
       ↓
3. _process_candidates()   — orderbook + calculator + alert_manager
```

### Шаг 2: Matching пар

1. Загружаются все active Polymarket и Predict.Fun рынки
2. Строится inverted index для Predict.Fun рынков
3. Для каждого Polymarket рынка отбираются кандидаты и запускается `match_candidates`
4. Результаты reconcile с существующими парами:
   - Новые → `db.add_all()`
   - Существующие → update (score, mapping)
   - Отсутствующие → статус `stale`
   - Ручно approved парам статус не меняется

### Шаг 3: Обработка кандидатов

1. Загружаются все пары со статусом `auto_approved` или `approved`
2. **Смарт-фильтрация API-запросов:** бот проверяет количество неудачных попыток чтения стакана. Если ранее стакан пары возвращался пустым `EMPTY_ORDERBOOK_THRESHOLD` раз подряд, она перманентно игнорируется для экономии лимитов бирж.
3. Загружаются preferences пользователя
4. `OrderbookService` параллельно (semaphore=4) загружает ордербуки
4. `ArbitrageCalculator` считает opportunities для каждого направления
5. `AlertManager` фильтрует, дедуплицирует и создаёт алерты

---

## API

**Файл:** `api/internal.py`
**Роутер:** `/api/...`

Endpoint | Auth | Описание
---------|------|----------
`GET /api/health` | Нет | `{"status": "ok"}`
`GET /api/status` | Нет | Количества рынков, пар, opportunities, очередь алертов
`GET /api/admin/runtime-metrics` | `X-Admin-Token` | Снимок runtime counters, поддерживает `?reset=true`
`GET /api/admin/pairs?status=auto_approved` | `X-Admin-Token` | Список пар с фильтрацией по статусу
`POST /api/admin/pairs/{id}/approve` | `X-Admin-Token` | Ручное подтверждение пары
`GET /api/admin/pairs/{id}/diagnose` | `X-Admin-Token` | Диагностика, на каком этапе пара отвалилась: orderbook, calculator или fanout/preferences
`GET /api/admin/matcher/debug?market_id=42` | `X-Admin-Token` | Отладка matcher: top-N кандидатов с score и reject_reason

Admin-endpoint'ы требуют заголовок `X-Admin-Token` со значением из `ADMIN_API_TOKEN`.

### Как посмотреть admin endpoint со статистикой

1. Убедитесь, что сервер запущен:
```bash
python3 start.py
```

2. Считайте admin token из `~/.config/arbivision/.env`:
```bash
ADMIN_TOKEN=$(grep '^ADMIN_API_TOKEN=' ~/.config/arbivision/.env | cut -d= -f2-)
```

3. Посмотрите runtime-статистику:
```bash
curl -H "X-Admin-Token: $ADMIN_TOKEN" \
  http://127.0.0.1:8000/api/admin/runtime-metrics
```

4. Если нужно вернуть counters и сразу их сбросить:
```bash
curl -H "X-Admin-Token: $ADMIN_TOKEN" \
  "http://127.0.0.1:8000/api/admin/runtime-metrics?reset=true"
```

5. Через веб-интерфейс можно открыть `http://127.0.0.1:8000/docs`, найти `GET /api/admin/runtime-metrics`, нажать `Try it out` и передать заголовок `X-Admin-Token`.

---

## ORM-модели

**Файл:** `models/orm.py`

### Market
```
id, platform, platform_market_id (unique per platform),
status, tradable, title, normalized_title,
description, outcomes_json, raw_payload_json,
category, slug, updated_at, created_at
```

### MarketPair
```
id, market_id_a (FK → markets), market_id_b (FK → markets),
pair_hash (unique, SHA-256 from sorted IDs),
status (auto_approved / approved / stale),
match_score, match_reason_json, outcome_mapping_json,
created_at
```

### ArbOpportunity
```
id, market_pair_id (FK → market_pairs),
direction, price_leg_1, price_leg_2,
avg_price_leg_1, avg_price_leg_2,
shares, capital_required,
gross_profit, net_profit, gross_roi, net_roi,
calculation_json, created_at
```

### Alert
```
id, opportunity_id (FK → arb_opportunities),
telegram_chat_id, message_hash,
status (queued / retry / sent / failed),
sent_at, error_message, created_at
```

### SettingsRecord
```
id, key (unique), value_json, updated_at
```
Хранит UI state и legacy/global Telegram fallback-настройки.

### User
```
id, status, created_at
```

### TelegramChat
```
id, user_id (FK → users), chat_id, chat_type,
is_primary, is_verified, created_at
```

### UserPreference
```
id, user_id (FK → users),
min_roi_percent, min_capital_usd, max_capital_usd,
min_profit_usd, max_days_to_close, muted,
updated_at, created_at
```

### Subscription
```
id, user_id (FK → users), channel, destination,
status, updated_at, created_at
```

### BlacklistRule
```
id, rule_type, rule_value, reason, created_at
```
Правила черного списка для фильтрации рынков.

---

## Конфигурация

Все настройки читаются из файла `~/.config/arbivision/.env`. Пример:

```env
POSTGRES_USER=arb_user
POSTGRES_PASSWORD=arb_pass
POSTGRES_DB=arbitrage_db
POSTGRES_HOST=localhost
POSTGRES_PORT=5432

REDIS_HOST=localhost
REDIS_PORT=6379

PREDICT_FUN_API_KEY=your_api_key_here

TELEGRAM_BOT_TOKEN=123456:ABC-DEF
TELEGRAM_DEFAULT_CHAT_IDS=123456789,987654321
TELEGRAM_SYSTEM_ERROR_CHAT_IDS=123456789

ADMIN_API_TOKEN=your_admin_token

MARKET_REFRESH_SECONDS=60
FEE_POLYMARKET_BPS=100.0
FEE_PREDICT_FUN_BPS=200.0
ORDERBOOK_CACHE_TTL_SECONDS=2
ORDERBOOK_CACHE_MAX_ITEMS=2048
ORDERBOOK_POLYMARKET_BATCH_SIZE=200
ORDERBOOK_PREDICT_FUN_CONCURRENCY=4
ALERTS_DEDUPE_TTL_SECONDS=600
```

---

## Тесты

Тесты запускаются через скрипт `run_tests.py`, который использует `unittest discover` по директории `tests/`.

### Юнит-тесты (запуск без зависимостей)

```bash
python3 run_tests.py
```

Сейчас полный локальный прогон дает 130 тестов. Live-тесты по умолчанию пропускаются.

Опции:
- `-v` / `--verbose` — подробный вывод с именами тестов
- `--no-buffer` — отключить буферизацию stdout

### Live-тесты (требуют работающего сервера)

Запускаются только при наличии переменных окружения:

```bash
# smoke-тесты FastAPI эндпоинтов (health, status, admin API)
RUN_LIVE_TESTS=1 python3 run_tests.py

# дополнительно — прямое подключение к PostgreSQL
RUN_LIVE_TESTS=1 RUN_LIVE_DB_TESTS=1 python3 run_tests.py
```

Для live-тестов необходим запущенный сервер (`python3 start.py`) и заполненный `ADMIN_API_TOKEN` в `.env`.

### Ручной admin smoke-тест

Файл `tests/test_admin_api.py` — отдельный скрипт (не unittest), который вызывается напрямую и выводит сводку состояния бота в терминал:

```bash
python3 tests/test_admin_api.py                    # краткий вывод
python3 tests/test_admin_api.py --verbose          # полный JSON-ответ каждого эндпоинта
python3 tests/test_admin_api.py --market-id 42     # отладка матчера для конкретного рынка
python3 tests/test_admin_api.py --status approved  # показать только вручную одобренные пары
python3 tests/test_admin_api.py --pair-id 42       # диагностика конкретной пары
```

### Состав тестов

Файл | Описание
-----|---------
`test_adapters.py` | HTTP-запросы адаптеров Polymarket и Predict.Fun (mock)
`test_alert_manager.py` | Фильтрация, дедупликация и сохранение алертов
`test_api_internal.py` | FastAPI эндпоинты (`/health`, `/status`, `/admin/*`) через TestClient
`test_calculator.py` | Greedy-алгоритм расчёта арбитража и учёт комиссий
`test_ingestion_outcomes.py` | Нормализация outcomes при impорте рынков
`test_normalizer_and_matcher.py` | NormalizerService и MatcherService: токенизация, scoring, semantic diff guard
`test_orderbook_service.py` | Построение directional books и парсинг уровней ордербука
`test_system_notifier.py` | Дедупликация системных ошибок и форматирование сообщений
`test_tg_bot_commands.py` | Форматирование алертов, команды бота, UI state machine
`test_tg_preferences.py` | Фильтры preferences, парсинг дат закрытия рынков
`test_worker_orderbook.py` | Извлечение уровней цен из различных форматов ордербуков
`test_worker_pairs.py` | Reconcile пар: создание, обновление, stale-маркировка
`test_live_api.py` | Smoke-тесты FastAPI (требуют `RUN_LIVE_TESTS=1`)
`test_live_db.py` | Smoke-тесты подключения к PostgreSQL (требуют `RUN_LIVE_DB_TESTS=1`)