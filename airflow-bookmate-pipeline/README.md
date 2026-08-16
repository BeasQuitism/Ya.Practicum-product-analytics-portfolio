# Airflow. Оркестрация пайплайнов (Яндекс Книги)

## Описание проекта

Сервис **Яндекс Книги** предоставляет доступ к контенту разных форматов (текст, аудио и др.).
В рамках проекта построен пайплайн в **Apache Airflow**, который запускает
**PySpark-скрипт** для обработки данных и создания витрин. Витрины помогают
команде сервиса быстрее готовить отчёты.

## Стек технологий

| Компонент | Назначение |
|-----------|------------|
| Apache Airflow | Оркестрация пайплайна |
| PySpark | Обработка и агрегация данных |
| Yandex DataProc | Spark-кластер |
| ClickHouse | Хранилище результата |
| S3 (Yandex Object Storage) | Хранение исходных данных |

## Описание данных

### `bookmate.audition` — активность пользователей

| Поле | Описание |
|------|----------|
| `audition_id` | ID сессии чтения/прослушивания |
| `puid` | ID пользователя |
| `usage_platform_ru` | Платформа |
| `msk_business_dt_str` | Дата/время (МСК) |
| `app_version` | Версия приложения |
| `adult_content_flg` | Контент 18+ (True/False) |
| `hours` | Длительность сессии (ч) |
| `hours_sessions_long` | Длительность длинных сессий (ч) |
| `kids_content_flg` | Детский контент (True/False) |
| `main_content_id` | ID контента |
| `usage_geo_id` | ID геолокации |

### `bookmate.content` — каталог контента

| Поле | Описание |
|------|----------|
| `main_content_id` | ID контента |
| `main_author_id` | ID автора |
| `main_content_type` | Тип (аудио/текст/другой) |
| `main_content_name` | Название |
| `main_content_duration_hours` | Длительность (ч) |
| `published_topic_title_list` | Жанры/темы |

## Архитектура пайплайна

```
S3 (входной CSV)
       │
       ▼
┌─────────────────┐
│  S3KeySensor    │  ← ожидает появления файла
└────────┬────────┘
         ▼
┌─────────────────┐
│  PySpark Job    │  ← агрегация по пользователям
│  (DataProc)     │
└────────┬────────┘
         ▼
   ClickHouse
(таблица-витрина)
```

## Установка и запуск

### 1. Клонирование

```bash
git clone https://github.com/<username>/airflow-bookmate-pipeline.git
cd airflow-bookmate-pipeline
```

### 2. Конфигурация

Заполните параметры подключения в файлах:

- `jobs/my_spark_job.py` — хост, порт, БД и креды ClickHouse.
- `dags/bookmate_dag.py` — `cluster_id`, путь к S3.

### 3. Развёртывание DAG

Скопируйте файл DAG в папку `dags/` вашего Airflow:

```bash
scp dags/bookmate_dag.py <airflow-host>:/path/to/dags/
```

### 4. Запуск

Активируйте DAG в Airflow UI или через CLI:

```bash
airflow dags trigger audition_content_analysis
```

## Расписание

DAG запускается ежедневно в **16:00 UTC** (`0 16 * * *`), начиная с `2025-01-01`.
Пропущенные запуски не восстанавливаются (`catchup=False`).

## Результат

В ClickHouse записывается агрегат по пользователям:

| Поле | Описание |
|------|----------|
| `puid` | ID пользователя |
| `audition_count` | Кол-во сессий |
| `avg_hours` | Средняя длительность сессии (ч) |
