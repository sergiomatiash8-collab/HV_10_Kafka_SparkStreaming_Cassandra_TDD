"""
Unit-тести для схеми Cassandra.

Тестуємо ТІЛЬКИ файл cassandra/init.cql — без підключення до Cassandra.
Це юніт-тест: перевіряємо що CQL-файл містить правильні визначення.

Нова схема таблиці edits:
  PRIMARY KEY ((user_id), created_at, domain)
  Поля: user_id TEXT, domain TEXT, created_at TIMESTAMP, page_title TEXT

Запуск: pytest tests/smoke/test_cassandra_schema.py -v
"""
import os
import re
import pytest
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent.resolve()
CQL_PATH = PROJECT_ROOT / "cassandra" / "init.cql"


@pytest.fixture(scope="module")
def cql_content() -> str:
    """Читаємо CQL-файл один раз на всі тести."""
    assert CQL_PATH.exists(), (
        f"init.cql не знайдено: {CQL_PATH}\n"
        "Переконайся що файл cassandra/init.cql існує в корені проєкту."
    )
    return CQL_PATH.read_text(encoding="utf-8")


# ── БЛОК 1: keyspace ──────────────────────────────────────────────────────────

class TestKeyspaceDefinition:
    """Перевіряємо визначення keyspace у CQL."""

    def test_keyspace_create_exists(self, cql_content):
        """CREATE KEYSPACE присутній у файлі."""
        assert "CREATE KEYSPACE" in cql_content.upper()

    def test_keyspace_name_is_wiki_namespace(self, cql_content):
        """Keyspace має назву wiki_namespace."""
        assert "wiki_namespace" in cql_content

    def test_keyspace_has_if_not_exists(self, cql_content):
        """IF NOT EXISTS — ідемпотентне створення (можна запускати повторно)."""
        assert "IF NOT EXISTS" in cql_content.upper()

    def test_keyspace_has_replication(self, cql_content):
        """Keyspace має визначення реплікації."""
        assert "replication" in cql_content.lower()

    def test_keyspace_uses_simple_strategy(self, cql_content):
        """SimpleStrategy — правильна стратегія для single-node dev оточення."""
        assert "SimpleStrategy" in cql_content


# ── БЛОК 2: таблиця edits ─────────────────────────────────────────────────────

class TestTableDefinition:
    """Перевіряємо визначення таблиці edits."""

    def test_table_create_exists(self, cql_content):
        """CREATE TABLE edits присутній у файлі."""
        assert "CREATE TABLE" in cql_content.upper()
        assert "edits" in cql_content

    def test_table_has_if_not_exists(self, cql_content):
        """CREATE TABLE IF NOT EXISTS — ідемпотентне."""
        upper = cql_content.upper()
        assert "CREATE TABLE IF NOT EXISTS" in upper

    def test_table_has_correct_fields(self, cql_content):
        """
        Нова схема: user_id, domain, created_at, page_title.
        ЗМІНЕНО: старі поля id/user_text/dt замінено на нові.
        """
        required_fields = ["user_id", "domain", "created_at", "page_title"]
        for field in required_fields:
            assert field in cql_content, (
                f"Поле '{field}' відсутнє в init.cql!\n"
                f"Нова схема вимагає: {required_fields}"
            )

    def test_user_id_is_text_type(self, cql_content):
        """user_id TEXT — PRIMARY KEY партиції, зберігає performer.user_text."""
        # Шукаємо рядок виду: user_id TEXT
        assert re.search(r"user_id\s+TEXT", cql_content, re.IGNORECASE), (
            "user_id має бути TEXT (performer.user_text — рядок, не UUID)"
        )

    def test_domain_is_text_type(self, cql_content):
        """domain TEXT — meta.domain як рядок."""
        assert re.search(r"domain\s+TEXT", cql_content, re.IGNORECASE), (
            "domain має бути TEXT"
        )

    def test_created_at_is_timestamp_type(self, cql_content):
        """created_at TIMESTAMP — meta.dt як часова мітка."""
        assert re.search(r"created_at\s+TIMESTAMP", cql_content, re.IGNORECASE), (
            "created_at має бути TIMESTAMP"
        )

    def test_page_title_is_text_type(self, cql_content):
        """page_title TEXT."""
        assert re.search(r"page_title\s+TEXT", cql_content, re.IGNORECASE), (
            "page_title має бути TEXT"
        )

    def test_old_fields_not_present(self, cql_content):
        """
        Стара схема (id UUID, user_text, dt) НЕ має бути в новій таблиці.
        Перевіряємо що рефакторинг схеми завершено.
        """
        # Ці поля були в старій схемі — їх більше не повинно бути
        # (перевіряємо обережно: user_text може з'явитись у коментарях)
        table_section = _extract_table_section(cql_content)
        if table_section:
            assert "user_text" not in table_section, (
                "Поле 'user_text' знайдено в таблиці — має бути 'user_id'"
            )
            # 'dt' як окрема колонка не повинна бути (замінена на created_at)
            assert not re.search(r"\bdt\b\s+TIMESTAMP", table_section, re.IGNORECASE), (
                "Поле 'dt' знайдено — має бути 'created_at'"
            )


# ── БЛОК 3: PRIMARY KEY ───────────────────────────────────────────────────────

class TestPrimaryKey:
    """Перевіряємо визначення PRIMARY KEY нової схеми."""

    def test_primary_key_defined(self, cql_content):
        """PRIMARY KEY визначений у таблиці."""
        assert "PRIMARY KEY" in cql_content.upper()

    def test_primary_key_contains_user_id(self, cql_content):
        """user_id — partition key (ключ партиції)."""
        # PRIMARY KEY ((user_id), ...)
        assert re.search(
            r"PRIMARY\s+KEY\s*\(\s*\(\s*user_id\s*\)",
            cql_content, re.IGNORECASE
        ), (
            "user_id має бути partition key: PRIMARY KEY ((user_id), ...)\n"
            "Це дозволяє ефективно шукати всі правки конкретного користувача."
        )

    def test_primary_key_contains_created_at_clustering(self, cql_content):
        """created_at — clustering column (для сортування по часу)."""
        # PRIMARY KEY може бути multiline → шукаємо жадібно до кінця блоку
        pk_match = re.search(
            r"PRIMARY\s+KEY\s*\(.*?\)\s*\)",
            cql_content, re.IGNORECASE | re.DOTALL
        ) or re.search(
            r"PRIMARY\s+KEY\s*\([^;]+\)",
            cql_content, re.IGNORECASE | re.DOTALL
        )
        if pk_match:
            pk_def = pk_match.group()
            assert "created_at" in pk_def, (
                f"created_at має бути clustering column у PRIMARY KEY.\nЗнайдено: {pk_def!r}"
            )

    def test_primary_key_contains_domain_clustering(self, cql_content):
        """domain — другий clustering column."""
        pk_match = re.search(
            r"PRIMARY\s+KEY\s*\(.*?\)\s*\)",
            cql_content, re.IGNORECASE | re.DOTALL
        ) or re.search(
            r"PRIMARY\s+KEY\s*\([^;]+\)",
            cql_content, re.IGNORECASE | re.DOTALL
        )
        if pk_match:
            pk_def = pk_match.group()
            assert "domain" in pk_def, (
                f"domain має бути clustering column у PRIMARY KEY.\nЗнайдено: {pk_def!r}"
            )

    def test_clustering_order_created_at_desc(self, cql_content):
        """
        CLUSTERING ORDER BY (created_at DESC) — останні правки першими.
        Це стандартна практика для time-series даних.
        """
        assert re.search(
            r"CLUSTERING\s+ORDER\s+BY.*created_at\s+DESC",
            cql_content, re.IGNORECASE | re.DOTALL
        ), (
            "Очікується CLUSTERING ORDER BY (created_at DESC, ...)\n"
            "Це дозволяє ефективно читати останні правки користувача."
        )


# ── БЛОК 4: індекси ───────────────────────────────────────────────────────────

class TestIndexes:
    """Перевіряємо secondary indexes для полів пошуку."""

    def test_domain_index_exists(self, cql_content):
        """Індекс по domain — для фільтрації по домену в запитах."""
        assert re.search(
            r"CREATE\s+INDEX.*ON\s+edits\s*\(\s*domain\s*\)",
            cql_content, re.IGNORECASE | re.DOTALL
        ), "Відсутній індекс по полю domain"

    def test_page_title_index_exists(self, cql_content):
        """Індекс по page_title — для пошуку по назві сторінки."""
        assert re.search(
            r"CREATE\s+INDEX.*ON\s+edits\s*\(\s*page_title\s*\)",
            cql_content, re.IGNORECASE | re.DOTALL
        ), "Відсутній індекс по полю page_title"


# ── БЛОК 5: загальна цілісність файлу ────────────────────────────────────────

class TestCQLFileIntegrity:
    """Загальні перевірки цілісності CQL-файлу."""

    def test_cql_file_exists(self):
        """Файл cassandra/init.cql існує."""
        assert CQL_PATH.exists(), f"init.cql не знайдено: {CQL_PATH}"

    def test_cql_file_not_empty(self, cql_content):
        """Файл не порожній."""
        assert len(cql_content.strip()) > 100, "init.cql підозріло короткий"

    def test_cql_has_use_statement(self, cql_content):
        """USE wiki_namespace — перемикаємось у правильний keyspace."""
        assert re.search(r"USE\s+wiki_namespace", cql_content, re.IGNORECASE), (
            "Відсутній 'USE wiki_namespace;' — таблиця може створитись у system keyspace"
        )

    def test_no_drop_statements(self, cql_content):
        """DROP TABLE/KEYSPACE відсутні — init.cql не має руйнівних операцій."""
        assert "DROP TABLE" not in cql_content.upper(), \
            "init.cql містить DROP TABLE — небезпечно для production!"
        assert "DROP KEYSPACE" not in cql_content.upper(), \
            "init.cql містить DROP KEYSPACE — небезпечно!"


# ── Допоміжні функції ─────────────────────────────────────────────────────────

def _extract_table_section(cql_content: str) -> str | None:
    """Витягує лише блок CREATE TABLE для більш точного аналізу."""
    match = re.search(
        r"CREATE\s+TABLE.*?edits\s*\(.*?\)\s*(?:WITH.*?;|;)",
        cql_content, re.IGNORECASE | re.DOTALL
    )
    return match.group() if match else None