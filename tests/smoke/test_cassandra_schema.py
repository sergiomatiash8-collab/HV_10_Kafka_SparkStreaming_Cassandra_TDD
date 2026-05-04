import os
import re
import pytest
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent.resolve()
CQL_PATH = PROJECT_ROOT / "cassandra" / "init.cql"

@pytest.fixture(scope="module")
def cql_content() -> str:
    assert CQL_PATH.exists(), f"init.cql not found: {CQL_PATH}"
    return CQL_PATH.read_text(encoding="utf-8")

# ── Block 1: Keyspace & Session ──────────────────────────────────────────────

class TestKeyspaceUsage:
    def test_cql_has_use_statement(self, cql_content):
        assert re.search(r"USE\s+wiki_namespace", cql_content, re.IGNORECASE), \
            "Missing 'USE wiki_namespace;' - table might be created in the wrong keyspace."

# ── Block 2: Table Definition & Fields ───────────────────────────────────────

class TestTableDefinition:
    def test_table_create_exists(self, cql_content):
        assert "CREATE TABLE" in cql_content.upper()
        assert "edits" in cql_content

    def test_table_has_correct_fields(self, cql_content):
        required_fields = ["user_id", "domain", "page_title", "created_at", "user_is_bot"]
        for field in required_fields:
            assert field in cql_content, f"Field '{field}' is missing in init.cql!"

    def test_user_id_is_text_type(self, cql_content):
        assert re.search(r"user_id\s+TEXT", cql_content, re.IGNORECASE)

    def test_domain_is_text_type(self, cql_content):
        assert re.search(r"domain\s+TEXT", cql_content, re.IGNORECASE)

    def test_created_at_is_timestamp_type(self, cql_content):
        assert re.search(r"created_at\s+TIMESTAMP", cql_content, re.IGNORECASE)

    def test_page_title_is_text_type(self, cql_content):
        assert re.search(r"page_title\s+TEXT", cql_content, re.IGNORECASE)

    def test_user_is_bot_is_boolean_type(self, cql_content):
        assert re.search(r"user_is_bot\s+BOOLEAN", cql_content, re.IGNORECASE)

# ── Block 3: Primary Key & Clustering ────────────────────────────────────────

class TestPrimaryKey:
    def test_primary_key_defined(self, cql_content):
        assert "PRIMARY KEY" in cql_content.upper()

    def test_primary_key_structure(self, cql_content):
        # Перевірка твого PK: (domain, created_at, user_id)
        pattern = r"PRIMARY\s+KEY\s*\(\s*domain\s*,\s*created_at\s*,\s*user_id\s*\)"
        assert re.search(pattern, cql_content, re.IGNORECASE), \
            f"PK must be (domain, created_at, user_id). Check your init.cql!"

    def test_clustering_order_created_at_desc(self, cql_content):
        assert re.search(r"CLUSTERING\s+ORDER\s+BY.*created_at\s+DESC", 
                         cql_content, re.IGNORECASE | re.DOTALL), \
            "Expected CLUSTERING ORDER BY (created_at DESC, ...)"

# ── Block 4: Indexes ─────────────────────────────────────────────────────────

class TestIndexes:
    def test_page_title_index_exists(self, cql_content):
        assert re.search(r"CREATE\s+INDEX.*ON\s+edits\s*\(\s*page_title\s*\)", 
                         cql_content, re.IGNORECASE | re.DOTALL), \
            "Missing index on page_title"

# ── Block 5: File Integrity ──────────────────────────────────────────────────

class TestCQLFileIntegrity:
    def test_cql_file_exists(self):
        assert CQL_PATH.exists()

    def test_cql_file_not_empty(self, cql_content):
        assert len(cql_content.strip()) > 100

    def test_has_drop_statement_for_dev(self, cql_content):
        # Оскільки ти використовуєш це для розробки, перевіряємо наявність DROP
        assert "DROP TABLE IF EXISTS edits" in cql_content

def _extract_table_section(cql_content: str) -> str | None:
    match = re.search(r"CREATE\s+TABLE.*?edits\s*\(.*?\)\s*(?:WITH.*?;|;)", 
                      cql_content, re.IGNORECASE | re.DOTALL)
    return match.group() if match else None