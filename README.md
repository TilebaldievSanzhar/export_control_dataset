# Export Control Dataset Pipeline

Пайплайн для формирования датасета экспортного контроля Кыргызской Республики. Система извлекает данные из PostgreSQL, документы из MinIO, выполняет OCR для сканированных документов и классифицирует товары по необходимости лицензирования.

## Оглавление

- [Требования](#требования)
- [Установка](#установка)
- [Конфигурация](#конфигурация)
- [Использование](#использование)
  - [Инкрементальная обработка новых файлов](#инкрементальная-обработка-новых-файлов)
- [Docker](#docker)
- [Этапы пайплайна](#этапы-пайплайна)
- [Структура проекта](#структура-проекта)
- [Выходные данные](#выходные-данные)
- [Возобновление работы](#возобновление-работы)
  - [Режимы возобновления](#режимы-возобновления)
  - [Как работает --incremental](#как-работает---incremental)
  - [Как работает --resume](#как-работает---resume)
  - [Команда refresh-mapping](#команда-refresh-mapping)
- [Диагностика](#диагностика)
- [Расширение паттернов](#расширение-паттернов)

## Требования

- Python 3.11+
- PostgreSQL (с таблицами `saf`, `saf_product_index`, `saf_document_index`)
- MinIO (S3-совместимое хранилище)
- Доступ к OCR API

## Установка

### 1. Клонирование и создание виртуального окружения

```bash
cd export_control_dataset
python -m venv .venv

# Windows
.venv\Scripts\activate

# Linux/macOS
source .venv/bin/activate
```

### 2. Установка зависимостей

```bash
pip install -r requirements.txt
```

### 3. Настройка конфигурации

```bash
# Создать файл .env из примера
cp .env.example .env

# Отредактировать .env с реальными значениями
```

## Конфигурация

Создайте файл `.env` в корне проекта:

```env
# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=export_control
DB_USER=user
DB_PASSWORD=password

# MinIO
MINIO_ENDPOINT=minio.example.com:9000
MINIO_ACCESS_KEY=access_key
MINIO_SECRET_KEY=secret_key
MINIO_BUCKET=documents
MINIO_SECURE=true

# OCR API
OCR_API_URL=https://ocr.trade.kg/documents/api/v1
OCR_SECRET_KEY=django-insecure-your-long-rarwefgvwerndom&%
OCR_POLL_INTERVAL=5
OCR_MAX_CONCURRENT=5
OCR_TIMEOUT=300

# Output
OUTPUT_DIR=./output
STATE_DIR=./state
LOGS_DIR=./logs

# Processing
BATCH_SIZE=100
```

## Использование

### Проверка подключений

Перед запуском убедитесь, что все сервисы доступны:

```bash
python main.py check
```

### Запуск всего пайплайна

```bash
python main.py run --all
```

### Запуск отдельных этапов

```bash
# Этап 1: Формирование базового датасета
python main.py run --step 1

# Этап 2: OCR технических описаний
python main.py run --step 2

# Этап 3: Извлечение текста из permit/license
python main.py run --step 3

# Этап 4: Классификация и финальный датасет
python main.py run --step 4
```

### Тестирование на ограниченной выборке

```bash
# Ограничить обработку 100 записями
python main.py run --step 2 --limit 100
```

### Возобновление после остановки

```bash
# Продолжить с места остановки
python main.py run --step 2 --resume
```

### Инкрементальная обработка новых файлов

Если в MinIO добавились новые файлы после первоначального прогона:

```bash
# 1. Обновить маппинг документов (без пересоздания base dataset)
python main.py refresh-mapping

# 2. Обработать только SAF номера с новыми файлами
python main.py run --step 2 --incremental
python main.py run --step 3 --incremental

# 3. Пересобрать финальный датасет
python main.py run --step 4
```

Если в БД тоже появились новые записи:

```bash
# Полный перезапуск step 1 (обновит и base dataset, и mapping)
python main.py run --step 1

# Затем incremental для step 2/3
python main.py run --step 2 --incremental
python main.py run --step 3 --incremental
python main.py run --step 4
```

### Проверка статуса

```bash
python main.py status
```

### Просмотр статистики

```bash
python main.py stats
```

### Сброс состояния

```bash
# Сбросить конкретный этап
python main.py reset --step 2

# Сбросить всё
python main.py reset --all
```

### Экспорт в CSV

```bash
python main.py run --step 4 --output-format csv
```

## Docker

### Сборка образа

```bash
cd export_control_dataset
docker build -t export-control-pipeline .
```

### Запуск всего пайплайна

```bash
docker run --rm \
  --memory=4g \
  --cpus=2 \
  --env-file .env \
  -v $(pwd)/output:/app/output \
  -v $(pwd)/state:/app/state \
  -v $(pwd)/logs:/app/logs \
  export-control-pipeline
```

### Запуск отдельного шага

```bash
docker run --rm \
  --memory=4g \
  --cpus=2 \
  --env-file .env \
  -v $(pwd)/output:/app/output \
  -v $(pwd)/state:/app/state \
  -v $(pwd)/logs:/app/logs \
  export-control-pipeline run --step 2 --resume
```

### Проверка статуса

```bash
docker run --rm \
  --env-file .env \
  -v $(pwd)/output:/app/output \
  -v $(pwd)/state:/app/state \
  export-control-pipeline status
```

### Ограничения ресурсов

| Параметр | Описание | Рекомендация |
|----------|----------|-------------|
| `--memory` | Лимит оперативной памяти | `4g` для стандартных объёмов |
| `--cpus` | Лимит CPU | `2` чтобы не занять все ядра |

Volumes (`-v`) монтируют `output/`, `state/` и `logs/` с хост-машины, чтобы результаты и прогресс сохранялись между запусками. `.env` передаётся через `--env-file` и не копируется в образ.

**Важно:** В `.env` для Docker адреса сервисов (`DB_HOST`, `MINIO_ENDPOINT`) должны быть доступны из контейнера. `localhost` не подойдёт — используйте реальные IP/хосты или `host.docker.internal`.

## Этапы пайплайна

### Этап 1: Формирование базового датасета

**Входные данные:** PostgreSQL

**Выходные данные:** `output/step1_base_dataset.parquet`

Извлекает данные из таблиц `saf` и `saf_product_index`, создаёт маппинг документов из MinIO.

**SQL-запрос:**

```sql
SELECT
    p.saf_number,
    p.hs_code_10 AS hs_code,
    p.product_description,
    s.lecense_need AS license_need_db
FROM saf_product_index p
LEFT JOIN saf s ON p.saf_number = s.saf_number
ORDER BY p.saf_number, p.id
```

Используется `LEFT JOIN`, чтобы сохранить все записи из `saf_product_index`, даже если для них нет записи в таблице `saf`. В этом случае `license_need_db` будет `NULL`.

### Этап 2: OCR технических описаний

**Входные данные:** MinIO (`specs/`), OCR API

**Выходные данные:** `output/step2_tech_specs.parquet`

Обрабатывает сканированные PDF через OCR API для извлечения технических описаний товаров.

**Особенности:**
- До 5 параллельных запросов к OCR
- Автоматический retry при ошибках сети
- Сохранение прогресса каждые 100 записей (BATCH_SIZE)
- Чанковая запись результатов на диск для экономии памяти (см. [Управление памятью](#управление-памятью))

### Этап 3: Извлечение текста из permit/license

**Входные данные:** MinIO (`permit/`, `license/`)

**Выходные данные:** `output/step3_permit_license.parquet`

Извлекает текст из машиночитаемых PDF без OCR (быстрее).

**Особенности:**
- Чанковая запись результатов на диск для экономии памяти (аналогично этапу 2)

### Этап 4: Классификация

**Входные данные:** Результаты этапов 1-3

**Выходные данные:** `output/final_dataset.parquet`

Объединяет все данные и определяет `license_need` на основе паттернов. Этапы 1-3 объединяются через `LEFT JOIN` по `saf_number`, поэтому все записи из базового датасета сохраняются, а `tech_description`, `permit_text`, `license_text` могут быть `NULL`.

## Структура проекта

```
export_control_dataset/
├── config/
│   ├── settings.py          # Конфигурация из .env
│   └── patterns.py          # Паттерны для классификации
├── core/
│   ├── database.py          # PostgreSQL клиент
│   ├── minio_client.py      # MinIO клиент
│   └── ocr_client.py        # OCR API клиент
├── extractors/
│   ├── pdf_extractor.py     # Извлечение из PDF
│   └── ocr_extractor.py     # OCR обработка
├── pipeline/
│   ├── step1_base_dataset.py
│   ├── step2_tech_specs.py
│   ├── step3_permit_license.py
│   └── step4_classification.py
├── utils/
│   ├── logger.py            # Логирование
│   ├── progress.py          # Прогресс и состояние
│   └── retry.py             # Retry логика
├── state/                   # Файлы состояния
├── output/                  # Выходные датасеты
├── logs/                    # Логи
├── main.py                  # CLI интерфейс
├── diagnostic.py            # Диагностика покрытия данных
├── Dockerfile               # Docker-образ
├── .dockerignore
├── requirements.txt
└── .env.example
```

## Выходные данные

### Финальный датасет (`output/final_dataset.parquet`)

| Поле | Тип | Описание |
|------|-----|----------|
| `saf_number` | string | Номер заявки |
| `hs_code` | string | Код ТН ВЭД (10 знаков) |
| `product_description` | string | Описание товара из БД |
| `tech_description` | string \| null | Текст из тех. описания (OCR) |
| `permit_text` | string \| null | Текст из документов permit/ |
| `license_text` | string \| null | Текст из документов license/ |
| `license_need` | boolean \| null | Результат классификации |
| `license_need_db` | boolean \| null | Значение из БД (NULL если нет записи в таблице `saf`) |

### Чтение данных

```python
import pandas as pd

# Parquet
df = pd.read_parquet("output/final_dataset.parquet")

# CSV (если экспортировали)
df = pd.read_csv("output/final_dataset.csv")
```

## Управление памятью

Этапы 2 и 3 используют **чанковую запись** для экономии оперативной памяти:

- Каждые `BATCH_SIZE` записей (по умолчанию 100) результаты сбрасываются на диск как отдельный chunk-файл (`output/step2_tech_specs_chunk_N.parquet`)
- Список результатов в памяти очищается после каждого checkpoint
- В конце этапа все chunk-файлы собираются в финальный parquet, chunk-и удаляются

**В памяти в любой момент находится не более ~100 записей**, независимо от общего объёма данных. Это позволяет обрабатывать десятки тысяч SAF-номеров без риска OOM.

При resume chunk-файлы с предыдущего запуска сохраняются на диске, новые результаты дописываются в новые chunk-и.

## Возобновление работы

Пайплайн сохраняет состояние в директории `state/`:

- `step2_tech_specs_progress.json` — прогресс этапа 2
- `step3_permit_license_progress.json` — прогресс этапа 3
- `document_mapping.json` — маппинг документов

### Режимы возобновления

| Сценарий | Команда | Описание |
|----------|---------|----------|
| Обработать новые файлы | `--incremental` | Только SAF номера с новыми файлами |
| Продолжить incremental после сбоя | `--incremental` | Автоматически продолжит |
| Полная обработка с нуля | без флагов | Обрабатывает всё заново |
| Продолжить полную обработку | `--resume` | Продолжает с места остановки |

### Как работает `--incremental`

1. Загружает state с информацией о ранее обработанных файлах
2. Сравнивает с текущим маппингом из MinIO
3. Находит SAF номера, у которых появились новые файлы
4. Обрабатывает только их
5. Объединяет новые результаты с существующими

**Важно:** Если скрипт прервался во время `--incremental`, просто запустите ту же команду снова — он продолжит с места остановки. Дополнительный флаг `--resume` не нужен.

### Как работает `--resume`

Используется для продолжения полной обработки (без `--incremental`):

1. Загружает список обработанных SAF номеров
2. Пропускает уже обработанные
3. Продолжает с места остановки

### Команда `refresh-mapping`

Обновляет маппинг документов из MinIO без пересоздания базового датасета:

```bash
python main.py refresh-mapping
```

Используйте когда:
- В MinIO добавились новые файлы
- База данных не изменилась (нет новых SAF номеров)
- Не хотите перезапускать step 1 полностью

## Диагностика

Скрипт `diagnostic.py` анализирует покрытие данных между PostgreSQL и MinIO:

```bash
python diagnostic.py
```

Показывает:
- Количество SAF-номеров в каждой таблице БД
- Количество директорий в MinIO (`specs/`, `permit/`, `license/`)
- Пересечение БД и MinIO (сколько SAF-номеров реально обрабатываются)
- Проверка формата SAF-номеров (несовпадение регистра, пробелов и т.д.)
- Анализ "потерянных" SAF-номеров (есть в MinIO, но нет в БД и наоборот)
- Статистика по выходным файлам и ошибкам OCR

## Расширение паттернов

Паттерны классификации находятся в `config/patterns.py`:

```python
# Паттерны "лицензия НЕ нужна"
LICENSE_NOT_REQUIRED_PATTERNS = [
    r"не\s+требуется\s+получение\s+лицензии",
    r"не\s+подлежит\s+лицензированию",
    # Добавьте свои паттерны
]

# Паттерны "лицензия НУЖНА"
LICENSE_REQUIRED_PATTERNS = [
    r"согласовывает\s+выдачу",
    r"требуется\s+(?:получение\s+)?лицензи[яию]",
    # Добавьте свои паттерны
]
```

Паттерны используют регулярные выражения Python. Флаги `IGNORECASE` и `UNICODE` включены по умолчанию.

## Логирование

Логи сохраняются в `logs/`:

- `pipeline_YYYY-MM-DD.log` — общий лог
- `step2_tech_specs_YYYY-MM-DD.log` — детальный лог этапа 2
- `step3_permit_license_YYYY-MM-DD.log` — детальный лог этапа 3
- `errors_YYYY-MM-DD.log` — только ошибки

## Обработка ошибок

| Тип ошибки | Стратегия |
|------------|-----------|
| Network timeout (OCR) | 3 попытки с exponential backoff |
| File not found (MinIO) | Пропуск + логирование |
| OCR API error (5xx) | 3 попытки |
| OCR API error (4xx) | Пропуск + логирование |
| PDF parsing error | Пропуск + логирование |
| Database connection | 5 попыток |

## Примерное время выполнения

| Метрика | Значение |
|---------|----------|
| Количество заявок | ~100-200K |
| Товаров (строк) | ~300-500K |
| Время этапа 2 (OCR) | ~20-50 часов |
| Время этапа 3 | ~2-5 часов |

## Troubleshooting

### Ошибка подключения к PostgreSQL

```bash
# Проверьте настройки в .env
python main.py check
```

### OCR timeout

Увеличьте таймаут в `.env`:
```env
OCR_TIMEOUT=600
```

### Нехватка памяти

Уменьшите размер батча (меньше записей в памяти между checkpoint-ами):
```env
BATCH_SIZE=50
```

При запуске в Docker ограничьте ресурсы:
```bash
docker run --memory=4g --cpus=2 ...
```

### Слишком много ошибок OCR

Уменьшите параллельность:
```env
OCR_MAX_CONCURRENT=2
```
