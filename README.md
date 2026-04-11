# Arbivision

Arbivision ищет арбитражные возможности между **Polymarket** и **Predict.Fun**, сохраняет найденные пары рынков в PostgreSQL и отправляет Telegram-алерты по подходящим возможностям.

## Что умеет сервис

- синхронизирует рынки с обеих площадок
- убирает дубли market rows перед upsert в PostgreSQL
- обновляет только реально изменившиеся `markets`, без лишних `UPDATE`
- матчингует похожие рынки и строит `outcome_mapping`
- пересчитывает `market_pairs` инкрементально, только для изменившихся рынков
- загружает ордербуки батчами и считает profitable directions
- дедуплицирует возможности через Redis
- создаёт и доставляет Telegram-алерты
- поддерживает пользовательские лимиты по общему объёму и по отдельному балансу на `Polymarket` и `Predict.Fun`
- даёт внутренние API-ручки для health, status и админ-диагностики

## Стек

- Python 3
- FastAPI
- SQLAlchemy + asyncpg
- Alembic
- Redis
- aiogram
- PostgreSQL
- Docker Compose

## Структура проекта

```text
arbitrage_bot/
  adapters/        интеграции с Polymarket и Predict.Fun
  api/             внутренние HTTP-ручки
  core/            config, db, redis, logging, observability
  models/          SQLAlchemy ORM-модели
  services/        ingestion, matcher, orderbook, calculator, fanout
  tg_bot/          Telegram UI, обработчики и настройки пользователей
  main.py          FastAPI app c lifespan-рантаймом
  api_app.py       FastAPI app только с API, без фоновых рантаймов
  runtime.py       общий запуск worker / fanout / telegram
  worker.py        основной цикл обработки рынков
  run_worker.py    запуск только worker
  run_fanout.py    запуск только fanout
  run_telegram.py  запуск только telegram polling
start.py           локальный dev-запуск проекта
stop.py            безопасная остановка процесса и контейнеров
```

## Правила оформления

- между функциями, методами и классами оставляем по 2 пустые строки
- завершающий перевод строки в конце файла не добавляем

## Как работает пайплайн

1. `IngestionService` загружает рынки, дедуплицирует входные market rows, делает upsert в БД и возвращает ids реально изменившихся рынков.
2. `MatcherService` строит или обновляет `MarketPair` между площадками только для затронутых рынков, а не пересчитывает весь граф без необходимости.
3. `OrderbookService` получает ордербуки батчами через `fetch_orderbooks_for_pairs(...)` и готовит направления `A_yes_B_no` и `A_no_B_yes`.
4. `ArbitrageCalculator` считает объём, profit и ROI.
5. `AlertManager` сохраняет `ArbOpportunity` со статусом `queued`, если изменение прошло dedupe-порог.
6. `FanoutManager` подбирает Telegram-получателей по пользовательским фильтрам и создаёт `Alert`.
7. Telegram-бот отправляет алерт сразу из worker-пайплайна или добирает queued/retry `Alert` из БД, а recovery `fanout` loop при отдельном запуске разбирает queued/retry `ArbOpportunity` батчами.

## Режимы запуска

Параметр `APP_RUNTIME_MODE` определяет, какие фоновые процессы поднимаются внутри `arbitrage_bot.main:app`.

- `all` — worker + telegram, без автоматического recovery `fanout` loop
- `worker` — только worker
- `fanout` — только recovery loop для queued/retry `ArbOpportunity`
- `telegram` — только Telegram polling

`all` рассчитан на основной сценарий, где worker сам создаёт и пытается сразу отправить алерты, а Telegram loop добирает queued/retry `Alert`.

`fanout` вынесен отдельно и нужен, когда хочется отдельно прогонять накопившиеся queued/retry `ArbOpportunity`. Если нужен recovery уже созданных возможностей, его стоит запускать отдельным процессом рядом с `all` или `worker`.

## Особенности worker

- worker не использует warmup-режим: после запуска opportunities обрабатываются так же, как и в любом следующем цикле
- ордербуки для активных пар забираются батчами в рамках одного прохода worker, а не по одному pair-запросу
- частота самого worker-цикла задаётся через `MARKET_REFRESH_SECONDS`
- полная синхронизация источников дополнительно ограничивается `MARKET_SYNC_INTERVAL_SECONDS`
- после sync worker rematch-ит только рынки, которые реально изменились в ingestion
- список approved pair и `market_map` кешируется между циклами, чтобы не читать их из PostgreSQL без необходимости
- в `worker cycle summary` теперь отдельно логируется `deliverable_opportunities`, чтобы не путать найденные opportunities и те, которые прошли fanout-фильтры

## Быстрый старт для разработки

1. Подготовьте файл окружения в `~/.config/arbivision/.env`.
2. Установите зависимости:

```bash
python3 -m pip install -r requirements.txt
```

3. Запустите проект:

```bash
python3 start.py
```

Что делает `start.py`:

- загружает `.env`
- запускает `docker compose up -d`
- ждёт готовности Postgres
- прогоняет `alembic upgrade head`
- стартует `uvicorn arbitrage_bot.main:app --reload`
- пишет PID в временный файл, чтобы `stop.py` мог остановить именно этот процесс

Остановка:

```bash
python3 stop.py
```

`stop.py` завершает только сохранённый PID, не пытаясь убивать посторонние `uvicorn`-процессы, а затем делает `docker compose stop`.

Опция `python3 stop.py --drop` удаляет контейнеры и volume базы данных. Это разрушительное действие, поэтому скрипт дополнительно спрашивает подтверждение.

## Альтернативные способы запуска

Только API без фоновых циклов:

```bash
uvicorn arbitrage_bot.api_app:app --reload
```

Только worker:

```bash
python3 -m arbitrage_bot.run_worker
```

Только fanout:

```bash
python3 -m arbitrage_bot.run_fanout
```

Только Telegram:

```bash
python3 -m arbitrage_bot.run_telegram
```

## Основные переменные окружения

### Инфраструктура

- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `POSTGRES_DB`
- `POSTGRES_HOST`
- `POSTGRES_PORT`
- `REDIS_HOST`
- `REDIS_PORT`
- `REDIS_DB`
- `APP_HOST`
- `APP_PORT`
- `LOG_LEVEL`

### Источники данных

- `PREDICT_FUN_API_KEY`

### Логика поиска и расчёта

- `MARKET_REFRESH_SECONDS`
- `MARKET_SYNC_INTERVAL_SECONDS`
- `MAX_MARKET_PAIRS_PER_LOOP`
- `EMPTY_ORDERBOOK_THRESHOLD`
- `ORDERBOOK_CACHE_TTL_SECONDS`
- `ORDERBOOK_CACHE_MAX_ITEMS`
- `ORDERBOOK_POLYMARKET_BATCH_SIZE`
- `ORDERBOOK_PREDICT_FUN_CONCURRENCY`
- `FEE_POLYMARKET_BPS`
- `FEE_PREDICT_FUN_BPS`

### Алерты и доставка

- `ALERTS_DEDUPE_TTL_SECONDS`
- `ALERTS_DELTA_PROFIT_THRESHOLD_USD`
- `ALERTS_DELTA_ROI_THRESHOLD_PERCENT`
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_DEFAULT_CHAT_IDS`
- `TELEGRAM_SYSTEM_ERROR_CHAT_IDS`
- `TELEGRAM_ALERTS_POLL_SECONDS`
- `TELEGRAM_DELIVERY_RETRY_SECONDS`
- `TELEGRAM_DELIVERY_MAX_ATTEMPTS`
- `FANOUT_TARGET_CACHE_TTL_SECONDS`
- `TELEGRAM_SYSTEM_ERROR_COOLDOWN_SECONDS`

### API и рантайм

- `APP_RUNTIME_MODE`

## Telegram-бот

Команда `/start` открывает экран выбора языка (English / Русский). После выбора открывается главное меню. Бот поддерживает:

- выбор языка интерфейса при первом запуске (English / Русский)
- паузу и возобновление алертов
- пользовательские фильтры через inline-кнопки
  - `min ROI`
  - `min capital`
  - `max capital`
  - `Polymarket balance`
  - `Predict.Fun balance`
  - `min profit`
  - `max days to close`
- отдельные лимиты баланса на `Polymarket` и `Predict.Fun`
- ввод числовых значений следующим сообщением
- выключение числового фильтра через `off` / `выкл`
- админский экран статистики для чатов из `TELEGRAM_SYSTEM_ERROR_CHAT_IDS`

Выбранный язык сохраняется в `UserPreference.language` и применяется ко всем сообщениям и кнопкам. Локализация реализована в `arbitrage_bot/tg_bot/localization.py` через функцию `translate(language, en_text, ru_text)`.

Кнопка `Stats` открывает сводку по пользователям, алертам и причинам дропа. Текст этого окна формируется в `arbitrage_bot/tg_bot/handlers.py` в функции `_format_admin_stats_text`.

## HTTP API

Приложение регистрирует роутер с префиксом `/api`.

Основные ручки:

- `GET /api/health`
- `GET /api/status`

`GET /api/status` возвращает агрегаты по рынкам, парам, opportunities и queued-alerts.

## Тесты

Основные тесты лежат в директории `tests/`.

Запуск:

```bash
python3 run_tests.py
```

Для полного запуска тестов нужны переменные окружения (работает только при запущенном проекте)

```bash
RUN_LIVE_TESTS=1 RUN_LIVE_DB_TESTS=1 python3 run_tests.py
```

Или через unittest:

```bash
python3 -m unittest discover -s tests
```

Для точечного прогона, например Telegram-команд:

```bash
python3 -m unittest tests.test_tg_bot_commands -v
```

Для актуального пайплайна особенно полезны:

```bash
python3 -m unittest tests.test_main_runtime -v
python3 -m unittest tests.test_worker_pairs tests.test_alert_manager tests.test_fanout_manager tests.test_orderbook_service tests.test_ingestion_outcomes -v
```

Эти наборы покрывают маршрутизацию `APP_RUNTIME_MODE`, дедупликацию opportunities, инкрементальный rematch рынков и batched orderbook fetch в worker.

## Примечания

- `.env` загружается из `~/.config/arbivision/.env`
- `main.py` поднимает API и фоновые рантаймы через FastAPI lifespan
- `api_app.py` нужен, когда хочется запустить только HTTP API без worker и Telegram
- Redis используется для dedupe и служебных кешей
- при недоступном Redis часть dedupe/cache логики деградирует мягко, без обязательного падения всего сервиса
- язык пользователя хранится в `user_preferences.language` и применяется ко всем текстам и кнопкам бота