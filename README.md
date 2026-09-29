# Arbivision

## Что делает сервис

- синхронизирует рынки с обеих площадок
- пересчитывает пары рынков только при изменениях
- асинхронно проверяет ордербуки и рассчитывает прибыльные направления
- создаёт и доставляет алерты в Telegram
- поддерживает пользовательские фильтры
- предоставляет внутренние маршруты health и status, показывает статистику для администраторов в Telegram

## Стек

- Python 3.11+
- FastAPI
- SQLAlchemy + asyncpg
- Alembic
- Redis
- aiogram
- PostgreSQL
- Docker Compose

## Запуск

Локальный `docker-compose.yml` поднимает только PostgreSQL и Redis. Само приложение запускается отдельно.

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
mkdir -p ~/.config/arbivision
docker compose --env-file ~/.config/arbivision/.env up -d
.venv/bin/python -m alembic upgrade head
APP_RUNTIME_MODE=api .venv/bin/python -m uvicorn arbitrage_bot.main:app --host 127.0.0.1 --port 8000
```

Для обработки рынков заполните `PREDICT_FUN_API_KEY`. Для режима `all` добавьте `TELEGRAM_BOT_TOKEN`. Для `GET /api/status` задайте `ADMIN_API_TOKEN`. После запуска API проверьте его командой:

```bash
curl http://127.0.0.1:8000/api/health
```

Режим `api` нужен для проверки HTTP API. Для worker и Telegram замените его на `all`, `worker` или `telegram`. Начальные значения и список переменных находятся в [`.env.example`](.env.example).

## Структура проекта

```text
arbitrage_bot/
  adapters/         интеграции с Polymarket и Predict.Fun
  api/              маршруты внутреннего API
  core/             конфигурация, БД, Redis и логирование
  models/           модели ORM на SQLAlchemy
  services/         загрузка, сопоставление, стаканы, расчёты и алерты
  tg_bot/           Telegram UI, обработчики и настройки пользователей
  main.py           приложение на FastAPI и его жизненный цикл
  runtime.py        запуск worker и Telegram
  worker.py         основной цикл обработки рынков
alembic/            миграции базы данных
tests/              модульные и интеграционные тесты
utilities/
  run_tests.py      запуск тестов
```


## Как работает пайплайн

1. `IngestionService` загружает рынки, убирает дубли, сохраняет изменения в БД и возвращает идентификаторы изменившихся рынков. Отсутствующие рынки помечаются закрытыми только после полной загрузки данных от источника.
2. `MatcherService` строит или обновляет `MarketPair` между площадками только для затронутых рынков. Итоговый `match_score` равен меньшему из `title_score` и `participant_score`, похожий заголовок не компенсирует слабое совпадение участников или исходов.
3. `OrderbookService` параллельно получает ордербуки Predict.Fun и Polymarket и готовит направления `A_yes_B_no` и `A_no_B_yes`.
4. `ArbitrageCalculator` рассчитывает доступный объём, прибыль и ROI.
5. `AlertManager` применяет общий фильтр по ключу `pair_hash + direction` в Redis и пропускает только новые или заметно улучшившиеся возможности.
6. `FanoutManager` применяет пользовательские фильтры и выбирает получателей.
7. Worker сразу отправляет уведомления по рассчитанным данным, без повторного запроса ордербуков.

Неудачные отправки повторяются с экспоненциальной задержкой. Число попыток и размер очереди задаются параметрами `TELEGRAM_ALERT_RETRY_MAX_ATTEMPTS`, `TELEGRAM_ALERT_RETRY_BASE_DELAY_SECONDS` и `TELEGRAM_ALERT_RETRY_QUEUE_MAX_SIZE`. Очередь хранится только в памяти и очищается при перезапуске worker.

Дедупликация работает на двух уровнях. Общий фильтр `AlertManager` хранит последнее состояние пары и направления в Redis с TTL `ALERTS_DEDUPE_TTL_SECONDS`. После успешной доставки для каждого чата Redis отдельно хранит состояние события минимум 24 часа и до закрытия рынка. Состояние общего фильтра фиксируется после завершения доставок, если хотя бы одна доставка успешна.

Повторное уведомление по той же возможности отправляется только при росте `net_profit` или `net_roi` не меньше заданного порога и помечается как обновление. Отдельная отметка хеша доставленного текста не даёт повторить то же сообщение после перезапуска. Если за один проход для чата найдено несколько возможностей, они отправляются одним дайджестом.

## Режимы запуска

Параметр `APP_RUNTIME_MODE` определяет, какие фоновые процессы поднимаются внутри `arbitrage_bot.main:app`.

- `all` — worker + telegram
- `worker` — worker без Telegram polling
- `telegram` — Telegram polling без worker
- `api` — только HTTP API, без фоновых процессов

`all` рассчитан на основной сценарий, где worker пытается быстро доставить свежее уведомление, а Telegram loop обслуживает пользовательский интерфейс бота.

## Основные настройки

| Переменная | По умолчанию | Назначение |
|---|---:|---|
| `PREDICT_FUN_API_KEY` | — | ключ API Predict.Fun для worker |
| `PREDICT_FUN_REST_RPS` | `4` | лимит всех запросов по REST к Predict.Fun в секунду |
| `ADMIN_API_TOKEN` | — | токен типа Bearer для доступа к `GET /api/status` |
| `TELEGRAM_BOT_TOKEN` | — | токен бота в Telegram |
| `TELEGRAM_DEFAULT_CHAT_IDS` | — | резервный список получателей через запятую |
| `TELEGRAM_SYSTEM_ERROR_CHAT_IDS` | — | чаты с доступом к `/stats` и основной список для системных ошибок |
| `APP_RUNTIME_MODE` | `all` | `all`, `worker`, `telegram` или `api` |
| `LOG_LEVEL` | `info` | уровень логирования |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` | `arb_user` / `arb_pass` / `arbitrage_db` | учётные данные PostgreSQL |
| `POSTGRES_HOST` / `POSTGRES_PORT` | `localhost` / `5432` | подключение к PostgreSQL |
| `REDIS_HOST` / `REDIS_PORT` / `REDIS_DB` | `localhost` / `6379` / `0` | подключение к Redis |
| `REDIS_PASSWORD` | — | пароль Redis |
| `FEE_POLYMARKET_BPS` / `FEE_PREDICT_FUN_BPS` | `90` / `100` | комиссии площадок в базисных пунктах |
| `ALERTS_DEDUPE_TTL_SECONDS` | `600` | TTL общего состояния дедупликации `AlertManager` |
| `ALERTS_DELTA_PROFIT_THRESHOLD_USD` | `3` | минимальный рост прибыли для повторного уведомления |
| `ALERTS_DELTA_ROI_THRESHOLD_PERCENT` | `0.5` | минимальный рост ROI для повторного уведомления |
| `MARKET_REFRESH_SECONDS` / `MARKET_SYNC_INTERVAL_SECONDS` | `5` / `60` | частота цикла worker и синхронизации рынков |
| `POLYMARKET_INCREMENTAL_MAX_PAGES` | `20` | максимум страниц инкрементальной синхронизации Polymarket |
| `POLYMARKET_FULL_SYNC_INTERVAL_SECONDS` | `1800` | период полной синхронизации Polymarket |
| `MATCHER_FULL_REMATCH_INTERVAL_SECONDS` | `21600` | период полного повторного сопоставления рынков |
| `MAX_MARKET_PAIRS_PER_LOOP` | `0` | ограничение числа пар за проход; `0` означает отсутствие лимита |
| `HOT_PAIR_QUEUE_MAX_SIZE` | `1000` | максимальный размер очереди изменившихся пар |
| `EMPTY_ORDERBOOK_THRESHOLD` | `3` | порог обработки пустых стаканов |
| `MAX_ACTIVE_PAIRS_PER_CYCLE` | `450` | максимум проверяемых пар за цикл |
| `ORDERBOOK_CACHE_TTL_SECONDS` / `ORDERBOOK_CACHE_MAX_ITEMS` | `1` / `5000` | TTL и максимальный размер кеша стаканов |
| `ORDERBOOK_POLYMARKET_BATCH_SIZE` | `100` | размер пачки запросов стаканов Polymarket |
| `ORDERBOOK_PREDICT_FUN_CONCURRENCY` | `12` | параллельность запросов Predict.Fun orderbook |
| `ORDERBOOK_STREAMING_ENABLED` | `true` | получать стаканы в реальном времени через WebSocket; REST остаётся резервом |
| `TELEGRAM_SEND_CONCURRENCY` | `8` | число параллельных отправок в Telegram |
| `FANOUT_TARGET_CACHE_TTL_SECONDS` | `2` | TTL кеша получателей алертов |
| `TELEGRAM_SYSTEM_ERROR_COOLDOWN_SECONDS` | `300` | пауза между повторными системными уведомлениями |
| `TELEGRAM_ALERT_RETRY_MAX_ATTEMPTS` | `3` | максимум попыток отправки одного уведомления |
| `TELEGRAM_ALERT_RETRY_BASE_DELAY_SECONDS` | `5` | начальная задержка перед повторной отправкой |
| `TELEGRAM_ALERT_RETRY_QUEUE_MAX_SIZE` | `1000` | максимальный размер очереди повторных отправок |
| `DB_CLEANUP_INTERVAL_SECONDS` / `DB_CLEANUP_RETENTION_SECONDS` | `10800` / `21600` | период очистки и срок хранения служебных записей |

Полный список параметров и их значения по умолчанию находится в [`arbitrage_bot/core/config.py`](arbitrage_bot/core/config.py). Переменные `APP_HOST`, `APP_PORT` и `APP_SCHEME` используются в тестах для подключения к уже запущенному API.

## Ограничения и отказоустойчивость

- после запуска worker сразу обрабатывает найденные возможности;
- некорректные уровни стакана со значениями `NaN` или `Infinity` отбрасываются до расчёта;
- частичный ответ API не приводит к ошибочному закрытию ранее загруженных рынков;
- за цикл проверяется не больше `MAX_ACTIVE_PAIRS_PER_CYCLE` пар, новые и обновлённые пары получают приоритет;
- если установлен `MAX_MARKET_PAIRS_PER_LOOP`, непроверенные пары не переводятся в статус `stale`;
- при недоступном Redis часть дедупликации и кеширования временно работает в памяти, подключение повторяется каждые 5 секунд.

## Запуск отдельных режимов

Только API без фоновых циклов:

```bash
APP_RUNTIME_MODE=api .venv/bin/python -m uvicorn arbitrage_bot.main:app --reload
```

API и worker:

```bash
APP_RUNTIME_MODE=worker .venv/bin/python -m uvicorn arbitrage_bot.main:app --reload
```

API и Telegram:

```bash
APP_RUNTIME_MODE=telegram .venv/bin/python -m uvicorn arbitrage_bot.main:app --reload
```


## Бот в Telegram

Команда `/start` открывает экран выбора языка (English / Русский). После выбора открывается главное меню. Бот поддерживает:

- выбор языка интерфейса при первом запуске (English / Русский)
- паузу и возобновление алертов
- пользовательские фильтры через кнопки под сообщениями Telegram: `min ROI`, `min volume`, `max volume`, `Polymarket balance`, `Predict.Fun balance`, `min profit`, `min market end`, `max market end`
- отдельные лимиты требуемого капитала на `Polymarket` и `Predict.Fun`
- ввод числовых значений следующим сообщением
- выключение числового фильтра через `off` / `выкл`
- сброс всех фильтров в `None` через кнопку «Disable all» / «Отключить всё»
- отдельную команду `/stats` для админской статистики в чатах из `TELEGRAM_SYSTEM_ERROR_CHAT_IDS`

Новые пользователи Telegram по умолчанию получают фильтры:

- `min ROI = 2%`
- `min volume = $10`
- `max volume = $50`
- `max market end = 15 days`

В интерфейсе `volume` означает требуемый капитал для сделки. Лимиты `Polymarket balance` и `Predict.Fun balance` вводятся пользователем и используются для пересчёта алертов.

Выбранный язык сохраняется в `UserPreference.language` и применяется ко всем сообщениям и кнопкам. Локализация реализована в `arbitrage_bot/tg_bot/localization.py` через функцию `translate(language, en_text, ru_text)`.

Изменять можно только заранее разрешённые настройки. Неизвестные поля в данных кнопок игнорируются.

Команда `/stats` доступна только чатам из `TELEGRAM_SYSTEM_ERROR_CHAT_IDS` и показывает:

- число пользователей и отправленных уведомлений
- причины фильтрации возможностей
- состояние проверок `orderbook coverage`, `deliverable opportunities` и `telegram polling`
- длительность и время последнего сбоя Telegram polling

Счётчики отправленных алертов, причин фильтрации и состояния мониторинга относятся к текущему запуску процесса и сбрасываются после перезапуска. Общее число пользователей хранится в БД. Системные ошибки отправляются в `TELEGRAM_SYSTEM_ERROR_CHAT_IDS`, а если список пуст, используется `TELEGRAM_DEFAULT_CHAT_IDS`.

## HTTP API

Приложение регистрирует роутер с префиксом `/api`. Корневой `GET /` возвращает ссылки на health и status.

- `GET /api/health`
- `GET /api/status`

`GET /api/health` возвращает `503`, если PostgreSQL недоступен; состояние Redis показывается как `ok` или `degraded`, поскольку сервис умеет временно работать без него.

`GET /api/status` требует заголовок `Authorization: Bearer <ADMIN_API_TOKEN>` и возвращает агрегаты по рынкам, парам и показателям работы приложения в полях `opportunity_counts.total`, `opportunity_counts.filtered_runtime` и `alert_counts.sent_runtime`. Неправильный или отсутствующий заголовок даёт `401`. Если токен не настроен, ручка отвечает `503`. Счётчики текущего запуска сбрасываются после перезапуска процесса.

Примеры запросов:

```bash
curl http://127.0.0.1:8000/
curl http://127.0.0.1:8000/api/health
curl -H "Authorization: Bearer $ADMIN_API_TOKEN" http://127.0.0.1:8000/api/status
```

Swagger, ReDoc и OpenAPI schema отключены. `GET /api/health` остаётся публичным для проверок состояния.

## Тесты

Тесты лежат в директории `tests/`.

Запуск:

```bash
python3 utilities/run_tests.py
```

Обычный запуск не включает проверки запущенного API. `RUN_LIVE_TESTS=1` требует уже запущенный API по адресу из `APP_SCHEME`, `APP_HOST` и `APP_PORT`, а также `ADMIN_API_TOKEN` в локальном файле переменных окружения. `RUN_LIVE_DB_TESTS=1` требует доступную БД и хотя бы одну сохранённую запись рынка.

```bash
RUN_LIVE_TESTS=1 RUN_LIVE_DB_TESTS=1 python3 utilities/run_tests.py
```

## Эксплуатационные замечания

- По умолчанию очистка БД запускается раз в 3 часа и удаляет служебные записи старше 6 часов. Пользователи, чаты, подписки и пользовательские настройки автоматически не удаляются.
- если локальный `~/.config/arbivision/.env` не найден, приложение продолжает работу с дефолтами и пустыми секретами;
- при резервном запросе через curl ключ `PREDICT_FUN_API_KEY` передаётся через stdin и не попадает в аргументы процесса.
