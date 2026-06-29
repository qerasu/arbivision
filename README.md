# Arbivision

Arbivision ищет арбитражные возможности между **Polymarket** и **Predict.Fun** и отправляет Telegram-алерты по подходящим возможностям.

## Что умеет сервис

- синхронизирует рынки с обеих площадок
- убирает дубли market rows перед upsert в PostgreSQL
- обновляет только изменившиеся `markets`
- сопоставляет похожие рынки и строит `outcome_mapping`
- пересчитывает `market_pairs` инкрементально
- проверяет ордербуки асинхронно по парам и считает profitable directions
- дедуплицирует неэффективности через Redis
- создаёт и доставляет Telegram-алерты
- поддерживает пользовательские лимиты по общему объёму и по отдельному балансу на `Polymarket` и `Predict.Fun`
- даёт внутренние API-ручки для health и status, а админ-статистику показывает в Telegram

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
  main.py           FastAPI app c lifespan-рантаймом
  runtime.py        общий запуск worker / telegram
  worker.py         основной цикл обработки рынков
utilities/
  start.py          локальный dev-запуск сервиса
  stop.py           безопасная остановка процесса и контейнеров
  run_tests.py      запуск тестов
  backup.py         бэкап данных
  bootstrap.py      начальная настройка окружения
  auto_update.py    pull-only автообновление из origin/main
  run_auto_update.ps1
                    Windows-обёртка для auto_update.py с lock-файлом и логом
  install_auto_update_task.ps1
                    установка Windows Scheduled Task для автообновления
```


## Как работает пайплайн

1. `IngestionService` загружает рынки, дедуплицирует входные market rows, делает upsert в БД и возвращает ids реально изменившихся рынков. Отсутствующие рынки помечаются закрытыми только после подтверждённо полной пагинации источника.
2. `MatcherService` строит или обновляет `MarketPair` между площадками только для затронутых рынков. Итоговый `match_score` равен меньшему из `title_score` и `participant_score`, похожий заголовок не компенсирует слабое совпадение участников или исходов.
3. `OrderbookService` получает ордербуки через `fetch_orderbooks_for_pairs(...)`; для single-pair проверки Predict.Fun и Polymarket запрашиваются параллельно, после чего готовятся направления `A_yes_B_no` и `A_no_B_yes`.
4. `ArbitrageCalculator` считает объём, profit и ROI.
5. `AlertManager` проверяет dedupe в Redis по `pair_hash + direction` и создаёт snapshot opportunity только если изменение прошло пороги по `net_profit` или `net_roi`.
6. `FanoutManager` подбирает Telegram-получателей по пользовательским фильтрам и собирает delivery.
7. Worker не делает отдельную ревалидацию перед отправкой: если по текущим стаканам найден спред, он собирает delivery и сразу делает одну попытку отправки. Dedupe-state opportunity фиксируется в Redis только после хотя бы одной успешной доставки. Telegram layer хранит per-user state по `chat_id + pair_hash + direction`: первый alert отправляется как новый, повторный alert по тому же событию уходит только при заметном улучшении `net_profit` или `net_roi` и явно помечается как update в тексте сообщения. Отдельной retry-очереди для обычных alert delivery сейчас нет, а после успешной доставки также пишется delivery-marker в Redis по `chat_id + message_hash`, чтобы не отправлять тот же текст повторно после рестарта. Постоянная запись opportunities/alerts в PostgreSQL не используется.

## Режимы запуска

Параметр `APP_RUNTIME_MODE` определяет, какие фоновые процессы поднимаются внутри `arbitrage_bot.main:app`.

- `all` — worker + telegram
- `worker` — worker без Telegram polling
- `telegram` — Telegram polling без worker
- `api` — только HTTP API, без фоновых процессов

`all` рассчитан на основной сценарий, где worker пытается быстро доставить свежий alert, а Telegram loop обслуживает пользовательский интерфейс бота.

## Особенности worker

- worker не использует warmup режим: после запуска opportunities обрабатываются так же, как и в любом следующем цикле
- ордербуки активных пар обрабатываются как асинхронные pair-задачи с лимитом `ORDERBOOK_PREDICT_FUN_CONCURRENCY`
- для одной пары `OrderbookService` параллельно запрашивает Predict.Fun orderbook и Polymarket books; если Predict.Fun быстро возвращает отсутствие рынка или ошибку, лишний Polymarket-запрос отменяется
- некорректные уровни стакана со значениями `NaN` или `Infinity` отбрасываются до расчёта
- частота самого worker-цикла задаётся через `MARKET_REFRESH_SECONDS`
- за один worker-цикл проверяется не больше `MAX_ACTIVE_PAIRS_PER_CYCLE` пар; новые или обновлённые matched pairs попадают в hot queue и проверяются первыми, остальные пары с ближайшим временем закрытия получают приоритет, внутри переполненных очередей используется ротация между циклами
- полная синхронизация источников дополнительно ограничивается `MARKET_SYNC_INTERVAL_SECONDS`
- если API упёрся в лимит страниц, повторил страницу или вернул частичный результат, ingestion сохраняет существующие активные рынки вместо stale detection
- после обычного sync worker заново сопоставляет только рынки, которые реально изменились в ingestion; полный rematch всех активных рынков идёт отдельно по `MATCHER_FULL_REMATCH_INTERVAL_SECONDS`; при достижении `MAX_MARKET_PAIRS_PER_LOOP` непроверенные пары не переводятся в `stale`
- список approved pair и `market_map` кешируется между циклами, чтобы не читать их из PostgreSQL без необходимости
- worker пишет timing-счётчики стадий `worker.timing.*_ms_total/count` для queue wait, orderbook fetch, calculation, fanout и Telegram send
- Telegram delivery по нескольким получателям отправляется параллельно с лимитом `TELEGRAM_SEND_CONCURRENCY`
- в `worker cycle summary` отдельно логируется `deliverable_opportunities`, чтобы не путать найденные opportunities и те, которые прошли fanout-фильтры
- worker раз в `DB_CLEANUP_INTERVAL_SECONDS` чистит старые `stale/failed market_pairs` и давно закрытые `markets`, которые больше не используются никакими парами; cleanup запрос использует SQL subquery вместо загрузки всех `market_id` в память
- ошибки Redis в счётчиках пустых ордербуков (`empty count` трекинг) логируются на уровне DEBUG; при недоступном Redis функция деградирует в in-memory fallback
- сетевые сбои `predict.fun` по orderbook логируются агрегированно на батч, чтобы не зашумлять логи warning-ами по каждому market id

## Старт для разработки

1. Перейдите в папку проекта:

```bash
cd arbivision
```

2. Создайте виртуальное окружение:

```bash
python3 -m venv .venv
source .venv/bin/activate # macos/linux

venv\Scripts\activate.bat # windows
```

3. Подготовьте файл окружения:

```bash
mkdir -p ~/.config/arbivision
cp .env.example ~/.config/arbivision/.env
```

Заполните в `~/.config/arbivision/.env` реальные значения `PREDICT_FUN_API_KEY`, `TELEGRAM_BOT_TOKEN` и нужные chat ids.

4. Установите зависимости:

```bash
python -m pip install -r requirements.txt
```

5. Запустите проект:

```bash
python utilities/start.py
```

Что делает `utilities/start.py`:

- загружает `.env`
- запускает `docker compose up -d`
- ждёт готовности Postgres
- прогоняет `alembic upgrade head`
- стартует `uvicorn arbitrage_bot.main:app --reload`
- пишет PID в временный файл, чтобы `utilities/stop.py` мог остановить именно этот процесс

Остановка:

```bash
python utilities/stop.py
```

`utilities/stop.py` завершает только сохранённый PID, не пытаясь убивать посторонние `uvicorn`-процессы, а затем делает `docker compose stop`.

Опция `python utilities/stop.py --drop` удаляет контейнеры, сеть и volumes для Postgres и Redis.

## Автообновление на Windows-сервере

`utilities/auto_update.py`:

- делает `git fetch origin main`
- сравнивает локальный `HEAD` с `origin/main`
- если коммиты совпадают, завершает работу без изменений
- если есть новый коммит, выполняет `git pull --ff-only origin main`
- все git-команды выполняются с `timeout=60s`; при зависании сети процесс не блокируется навсегда
- при ошибке выбрасывается `RuntimeError`, а не `SystemExit`, что безопасно при вызове из другого модуля

`auto_update.py` не вызывает `utilities/stop.py` и `utilities/start.py`. При обычном запуске через `utilities/start.py` код подхватывает `uvicorn --reload`, поэтому отдельный рестарт из автообновления не нужен и может привести к двум экземплярам Telegram polling.

`run_auto_update.ps1` защищает запуск lock-файлом `tmp/auto_update.lock`, чтобы две задачи планировщика не тянули git одновременно. Wrapper записывает в lock PID процесса и автоматически перехватывает stale lock, если процесс уже завершился или lock старше 15 минут.

Ручная проверка на Windows:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\utilities\run_auto_update.ps1
```

Установка задачи планировщика с интервалом 5 минут:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\utilities\install_auto_update_task.ps1
```

Установка с интервалом 10 минут:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\utilities\install_auto_update_task.ps1 -IntervalMinutes 10
```

Проверка задачи:

```powershell
schtasks /Query /TN "Arbivision Auto Update" /V /FO LIST
```

Лог автообновления пишется в `logs/auto_update.log`. В нём должны быть строки `run auto_update.py`, `local HEAD`, `remote HEAD`, `no updates found` или `update completed`, а также `exit code: 0`.

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

Настройки пользователя защищены whitelist-ом допустимых полей (`ALLOWED_PREFERENCE_FIELDS`). Callback data с неизвестным `field_name` игнорируется на уровне хэндлера, а `set_user_preference` выбрасывает `ValueError` для полей не из whitelist.

Команда `/stats` открывает Telegram-сводку по пользователям, runtime-алертам, причинам fanout/drop и состоянию monitor-проверок `orderbook coverage` / `deliverable opportunities` / `telegram polling`. Она зарегистрирована в списке команд бота наравне с `/start`, но отдаёт данные только чатам из `TELEGRAM_SYSTEM_ERROR_CHAT_IDS`. Monitor-проверки `orderbook coverage`, `deliverable opportunities` и `telegram polling`, включая recovery, не отправляют отдельные Telegram-уведомления и доступны только в `/stats`. Для `telegram polling` в окне статистики также показываются текущий severity, длительность сбоя и время последнего failure. Текст этого окна формируется в `arbitrage_bot/tg_bot/handlers.py` в функции `_format_admin_stats_text`.

## HTTP API

Приложение регистрирует роутер с префиксом `/api`.

Основные ручки:

- `GET /api/health`
- `GET /api/status`

`GET /api/status` возвращает агрегаты по рынкам, парам и runtime-метрикам в полях `opportunity_counts.total`, `opportunity_counts.filtered_runtime` и `alert_counts.sent_runtime`.

## Тесты

Тесты лежат в директории `tests/`.

Запуск:

```bash
python utilities/run_tests.py
```

Для полного запуска тестов нужны переменные окружения (работает только при локально запущенном проекте)

```bash
RUN_LIVE_TESTS=1 RUN_LIVE_DB_TESTS=1 python utilities/run_tests.py
```

## Примечания
-  Worker делает одну немедленную попытку отправки обычных user-alerts. Подавление повторов обеспечивается комбинацией dedupe-state opportunity, per-user event state и delivery-marker в Redis. `TELEGRAM_SEND_CONCURRENCY` управляет параллельностью этой отправки.
- По умолчанию cleanup БД запускается раз в 3 часа и удаляет записи старше 6 часов только из runtime-таблиц рынков и пар: пользовательские сущности (`users`, `telegram_chats`, `subscriptions`, `user_preferences`) автоматически не удаляются.
- `.env` загружается из `~/.config/arbivision/.env`; если файл не найден, приложение продолжает работу с дефолтами и пустыми секретами
- `main.py` поднимает API и фоновые рантаймы через FastAPI lifespan
- Redis используется для dedupe и служебных кешей; `get_redis()` — синхронная функция, возвращающая глобальный пул соединений
- при недоступном Redis часть dedupe/cache логики деградирует мягко, без обязательного падения всего сервиса; подключение автоматически повторяется каждые 5 секунд
- при curl fallback ключ `PREDICT_FUN_API_KEY` передаётся через stdin и не попадает в аргументы процесса
- `TELEGRAM_DEFAULT_CHAT_IDS` и `TELEGRAM_SYSTEM_ERROR_CHAT_IDS` хранятся как `frozenset` для O(1) membership check
- `MatcherService.build_market_signature` вычисляет context haystack один раз и передаёт его во все внутренние `_detect_*` методы, чтобы избежать повторной нормализации одного и того же текста; аналогично `title_score` вычисляется один раз в `explain_match` и передаётся в `_should_auto_approve`
