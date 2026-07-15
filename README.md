# Arbivision

## Что умеет сервис

- синхронизирует рынки с обеих площадок
- убирает дубли перед записью рынков в PostgreSQL
- обновляет только изменившиеся рынки
- сопоставляет похожие рынки и определяет соответствие исходов
- пересчитывает пары рынков только при изменениях
- асинхронно проверяет ордербуки и рассчитывает прибыльные направления
- подавляет повторные уведомления через Redis
- создаёт и доставляет Telegram-алерты
- поддерживает пользовательские лимиты по общему объёму и по отдельному балансу на `Polymarket` и `Predict.Fun`
- даёт внутренние API-ручки для health и status, а админ-статистику показывает в Telegram

## Стек

- Python 3.11+
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
  adapters/         интеграции с Polymarket и Predict.Fun
  api/              внутренние HTTP-ручки
  core/             config, env loading, db, redis, logging, observability
  models/           SQLAlchemy ORM-модели
  services/         ingestion, matcher, orderbook, calculator, fanout
  tg_bot/           Telegram UI, обработчики и настройки пользователей
    bot.py          доставка алертов и форматирование сообщений
    handlers.py     обработчики команд и callback-ов
    localization.py функция translate(language, en, ru)
    preferences.py  CRUD пользовательских настроек и UI-state
  main.py           FastAPI-приложение и его цикл
  runtime.py        запуск worker и Telegram
  worker.py         основной цикл обработки рынков
utilities/
  run_tests.py      запуск тестов
```


## Как работает пайплайн

1. `IngestionService` загружает рынки, убирает дубли, сохраняет изменения в БД и возвращает идентификаторы изменившихся рынков. Отсутствующие рынки помечаются закрытыми только после полной загрузки данных от источника.
2. `MatcherService` строит или обновляет `MarketPair` между площадками только для затронутых рынков. Итоговый `match_score` равен меньшему из `title_score` и `participant_score`, похожий заголовок не компенсирует слабое совпадение участников или исходов.
3. `OrderbookService` параллельно получает ордербуки Predict.Fun и Polymarket и готовит направления `A_yes_B_no` и `A_no_B_yes`.
4. `ArbitrageCalculator` рассчитывает доступный объём, прибыль и ROI.
5. `AlertManager` проверяет Redis по ключу `pair_hash + direction` и пропускает только новые или заметно улучшившиеся возможности.
6. `FanoutManager` применяет пользовательские фильтры и выбирает получателей.
7. Worker сразу отправляет уведомления по рассчитанным данным, без повторного запроса ордербуков.

Неудачные отправки повторяются с экспоненциальной задержкой. Число попыток и размер очереди задаются параметрами `TELEGRAM_ALERT_RETRY_MAX_ATTEMPTS`, `TELEGRAM_ALERT_RETRY_BASE_DELAY_SECONDS` и `TELEGRAM_ALERT_RETRY_QUEUE_MAX_SIZE`. Очередь хранится только в памяти и очищается при перезапуске worker.

Состояние дедупликации фиксируется после первой успешной доставки. Повторное уведомление по той же возможности отправляется только при заметном росте `net_profit` или `net_roi` и помечается как обновление. Если за один проход для чата найдено несколько возможностей, они отправляются одним дайджестом.

Redis также хранит отметку о доставленном тексте, чтобы не повторять его после перезапуска.

## Режимы запуска

Параметр `APP_RUNTIME_MODE` определяет, какие фоновые процессы поднимаются внутри `arbitrage_bot.main:app`.

- `all` — worker + telegram
- `worker` — worker без Telegram polling
- `telegram` — Telegram polling без worker
- `api` — только HTTP API, без фоновых процессов

`all` рассчитан на основной сценарий, где worker пытается быстро доставить свежий alert, а Telegram loop обслуживает пользовательский интерфейс бота.

## Основные настройки

Приложение загружает переменные из `~/.config/arbivision/.env`. Основные параметры:

| Переменная | По умолчанию | Назначение |
|---|---:|---|
| `PREDICT_FUN_API_KEY` | — | API-ключ Predict.Fun для worker |
| `PREDICT_FUN_REST_RPS` | `4` | лимит резервных REST-запросов Predict.Fun в секунду |
| `ADMIN_API_TOKEN` | — | Bearer-токен для доступа к `GET /api/status` |
| `TELEGRAM_BOT_TOKEN` | — | токен Telegram-бота |
| `TELEGRAM_DEFAULT_CHAT_IDS` | пусто | резервный список получателей через запятую |
| `TELEGRAM_SYSTEM_ERROR_CHAT_IDS` | пусто | идентификаторы чатов с доступом к `/stats` |
| `APP_RUNTIME_MODE` | `all` | `all`, `worker`, `telegram` или `api` |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` | `arb_user` / `arb_pass` / `arbitrage_db` | учётные данные PostgreSQL |
| `POSTGRES_HOST` / `POSTGRES_PORT` | `localhost` / `5432` | подключение к PostgreSQL |
| `REDIS_HOST` / `REDIS_PORT` / `REDIS_DB` | `localhost` / `6379` / `0` | подключение к Redis |
| `REDIS_PASSWORD` | — | пароль Redis |
| `FEE_POLYMARKET_BPS` / `FEE_PREDICT_FUN_BPS` | `90` / `100` | комиссии площадок в базисных пунктах |
| `ALERTS_DEDUPE_TTL_SECONDS` | `600` | срок первичной дедупликации; успешный Telegram-алерт запоминается до закрытия рынка, минимум на 24 часа |
| `ALERTS_DELTA_PROFIT_THRESHOLD_USD` | `3` | минимальный рост прибыли для повторного уведомления |
| `ALERTS_DELTA_ROI_THRESHOLD_PERCENT` | `0.5` | минимальный рост ROI для повторного уведомления |
| `MARKET_REFRESH_SECONDS` / `MARKET_SYNC_INTERVAL_SECONDS` | `5` / `60` | частота worker-цикла и синхронизации рынков |
| `MAX_ACTIVE_PAIRS_PER_CYCLE` | `450` | максимум проверяемых пар за цикл |
| `ORDERBOOK_PREDICT_FUN_CONCURRENCY` | `12` | параллельность запросов Predict.Fun orderbook |
| `ORDERBOOK_STREAMING_ENABLED` | `true` | получать live-стаканы через WebSocket; REST остаётся резервом |
| `TELEGRAM_SEND_CONCURRENCY` | `8` | число параллельных отправок в Telegram |
| `TELEGRAM_ALERT_RETRY_MAX_ATTEMPTS` | `3` | максимум попыток отправки одного уведомления |
| `TELEGRAM_ALERT_RETRY_BASE_DELAY_SECONDS` | `5` | начальная задержка перед повторной отправкой |
| `TELEGRAM_ALERT_RETRY_QUEUE_MAX_SIZE` | `1000` | максимальный размер очереди повторных отправок |
| `DB_CLEANUP_INTERVAL_SECONDS` / `DB_CLEANUP_RETENTION_SECONDS` | `10800` / `21600` | период очистки и срок хранения служебных записей |

Полный список параметров и их дефолты находится в `arbitrage_bot/core/config.py`.

## Особенности worker

- после запуска worker сразу обрабатывает найденные возможности
- активные пары проверяются параллельно с лимитом `ORDERBOOK_PREDICT_FUN_CONCURRENCY`
- некорректные уровни стакана со значениями `NaN` или `Infinity` отбрасываются до расчёта
- частота обработки задаётся через `MARKET_REFRESH_SECONDS`, а синхронизации рынков — через `MARKET_SYNC_INTERVAL_SECONDS`
- за цикл проверяется не больше `MAX_ACTIVE_PAIRS_PER_CYCLE` пар; новые и обновлённые пары получают приоритет
- частичный ответ API не приводит к ошибочному закрытию ранее загруженных рынков
- обычно повторно сопоставляются только изменившиеся рынки; полный проход выполняется по `MATCHER_FULL_REMATCH_INTERVAL_SECONDS`
- если установлен лимит `MAX_MARKET_PAIRS_PER_LOOP`, непроверенные пары не переводятся в статус `stale`
- отправка нескольким получателям выполняется параллельно с лимитом `TELEGRAM_SEND_CONCURRENCY`
- старые служебные записи удаляются по расписанию, заданному `DB_CLEANUP_INTERVAL_SECONDS`
- при недоступном Redis часть дедупликации и кеширования временно работает в памяти

## Альтернативные способы запуска

Только API без фоновых циклов:

```bash
APP_RUNTIME_MODE=api python -m uvicorn arbitrage_bot.main:app --reload
```

API + worker:

```bash
APP_RUNTIME_MODE=worker python -m uvicorn arbitrage_bot.main:app --reload
```

API + Telegram:

```bash
APP_RUNTIME_MODE=telegram python -m uvicorn arbitrage_bot.main:app --reload
```

## Telegram-бот

Команда `/start` открывает экран выбора языка (English / Русский). После выбора открывается главное меню. Бот поддерживает:

- выбор языка интерфейса при первом запуске (English / Русский)
- паузу и возобновление алертов
- пользовательские фильтры через inline-кнопки: `min ROI`, `min volume`, `max volume`, `Polymarket balance`, `Predict.Fun balance`, `min profit`, `min market end`, `max market end`
- отдельные лимиты баланса на `Polymarket` и `Predict.Fun`
- ввод числовых значений следующим сообщением
- выключение числового фильтра через `off` / `выкл`
- сброс всех фильтров в `None` через кнопку «Disable all» / «Отключить всё»
- отдельную команду `/stats` для админской статистики в чатах из `TELEGRAM_SYSTEM_ERROR_CHAT_IDS`

Новые Telegram-пользователи по умолчанию получают фильтры:

- `min ROI = 2%`
- `min volume = $10`
- `max volume = $50`
- `max market end = 15 days`

Выбранный язык сохраняется в `UserPreference.language` и применяется ко всем сообщениям и кнопкам. Локализация реализована в `arbitrage_bot/tg_bot/localization.py` через функцию `translate(language, en_text, ru_text)`.

Изменять можно только заранее разрешённые настройки. Неизвестные поля в данных кнопок игнорируются.

Команда `/stats` доступна только чатам из `TELEGRAM_SYSTEM_ERROR_CHAT_IDS` и показывает:

- число пользователей и отправленных уведомлений
- причины фильтрации возможностей
- состояние проверок `orderbook coverage`, `deliverable opportunities` и `telegram polling`
- длительность и время последнего сбоя Telegram polling

Эти проверки, включая восстановление после сбоя, не отправляют отдельных уведомлений и доступны только через `/stats`.

## HTTP API

Приложение регистрирует роутер с префиксом `/api`.

Основные ручки:

- `GET /api/health`
- `GET /api/status`

`GET /api/health` возвращает `503`, если PostgreSQL недоступен; состояние Redis показывается как `ok` или `degraded`, поскольку сервис умеет временно работать без него.

`GET /api/status` требует заголовок `Authorization: Bearer <ADMIN_API_TOKEN>` и возвращает агрегаты по рынкам, парам и runtime-метрикам в полях `opportunity_counts.total`, `opportunity_counts.filtered_runtime` и `alert_counts.sent_runtime`. Если токен не настроен, ручка отвечает `503`.

Swagger, ReDoc и OpenAPI schema отключены. `GET /api/health` остаётся публичным для health-проб.

Перед публикацией задайте длинный случайный `ADMIN_API_TOKEN` в секретах Amvera; не сохраняйте его в `amvera.yml` или репозитории.

## Тесты

Тесты лежат в директории `tests/`.

Запуск:

```bash
python utilities/run_tests.py
```

Для полного запуска тестов нужны переменные окружения (работает только при локально запущенном проекте и наличии хотя бы 1 записи в базе данных)

```bash
RUN_LIVE_TESTS=1 RUN_LIVE_DB_TESTS=1 python utilities/run_tests.py
```

## Примечания

- По умолчанию очистка БД запускается раз в 3 часа и удаляет служебные записи старше 6 часов. Пользователи, чаты, подписки и пользовательские настройки автоматически не удаляются.
- `.env` загружается из `~/.config/arbivision/.env`; если файл не найден, приложение продолжает работу с дефолтами и пустыми секретами
- при недоступном Redis сервис продолжает работу с ограниченной дедупликацией и автоматически повторяет подключение каждые 5 секунд
- при резервном запросе через curl ключ `PREDICT_FUN_API_KEY` передаётся через stdin и не попадает в аргументы процесса
