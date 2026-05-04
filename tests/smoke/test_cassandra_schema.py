import os
import re
import pytest
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent.resolve()
CQL_PATH = PROJECT_ROOT / "cassandra" / "init.cql"


@pytest.fixture(scope="module")
def cql_content() -> str:
    
    assert CQL_PATH.exists(), (
        f"init.cql not found: {CQL_PATH}\n"
        
    )
    return CQL_PATH.read_text(encoding="utf-8")


# ── Block 1: keyspace ──────────────────────────────────────────────────────────

class TestKeyspaceDefinition:
    

    def test_keyspace_create_exists(self, cql_content):
        
        assert "CREATE KEYSPACE" in cql_content.upper()

    def test_keyspace_name_is_wiki_namespace(self, cql_content):
        
        assert "wiki_namespace" in cql_content

    def test_keyspace_has_if_not_exists(self, cql_content):
        
        assert "IF NOT EXISTS" in cql_content.upper()

    def test_keyspace_has_replication(self, cql_content):
        
        assert "replication" in cql_content.lower()

    def test_keyspace_uses_simple_strategy(self, cql_content):
        
        assert "SimpleStrategy" in cql_content


# ── Block 2: table edits ─────────────────────────────────────────────────────

class TestTableDefinition:
    

    def test_table_create_exists(self, cql_content):
        
        assert "CREATE TABLE" in cql_content.upper()
        assert "edits" in cql_content

    def test_table_has_if_not_exists(self, cql_content):
       
        upper = cql_content.upper()
        assert "CREATE TABLE IF NOT EXISTS" in upper

    def test_table_has_correct_fields(self, cql_content):
       
        required_fields = ["user_id", "domain", "created_at", "page_title"]
        for field in required_fields:
            assert field in cql_content, (
                f"Field '{field}' is abscent в init.cql!\n"
                f"Нова схема вимагає: {required_fields}"
            )

    def test_user_id_is_text_type(self, cql_content):
        
        
        assert re.search(r"user_id\s+TEXT", cql_content, re.IGNORECASE), (
            "user_id must be text TEXT (performer.user_text — row, not UUID)"
        )

    def test_domain_is_text_type(self, cql_content):
        
        assert re.search(r"domain\s+TEXT", cql_content, re.IGNORECASE), (
            "domain must be TEXT"
        )

    def test_created_at_is_timestamp_type(self, cql_content):
        
        assert re.search(r"created_at\s+TIMESTAMP", cql_content, re.IGNORECASE), (
            "created_at must beTIMESTAMP"
        )

    def test_page_title_is_text_type(self, cql_content):
        
        assert re.search(r"page_title\s+TEXT", cql_content, re.IGNORECASE), (
            "page_title must be TEXT"
        )

    def test_old_fields_not_present(self, cql_content):
        
        
        table_section = _extract_table_section(cql_content)
        if table_section:
            assert "user_text" not in table_section, (
                "Field 'user_text' found in text - must be 'user_id'"
            )
            
            assert not re.search(r"\bdt\b\s+TIMESTAMP", table_section, re.IGNORECASE), (
                "Field 'dt' found-must be 'created_at'"
            )


# ── Block 3: PRIMARY KEY ───────────────────────────────────────────────────────

class TestPrimaryKey:
    

    def test_primary_key_defined(self, cql_content):
        
        assert "PRIMARY KEY" in cql_content.upper()

    def test_primary_key_contains_user_id(self, cql_content):
        
        assert re.search(
            r"PRIMARY\s+KEY\s*\(\s*\(\s*user_id\s*\)",
            cql_content, re.IGNORECASE
        ), (
            "user_id must be partition key: PRIMARY KEY ((user_id), ...)\n"
            "This allows you to search effectively for all edits made by a specific user."
        )

    def test_primary_key_contains_created_at_clustering(self, cql_content):
        
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
                f"created_at must be clustering column у PRIMARY KEY.\nFound: {pk_def!r}"
            )

    def test_primary_key_contains_domain_clustering(self, cql_content):
        
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
                f"domain must be clustering column у PRIMARY KEY.\nFound: {pk_def!r}"
            )

    def test_clustering_order_created_at_desc(self, cql_content):
        
        assert re.search(
            r"CLUSTERING\s+ORDER\s+BY.*created_at\s+DESC",
            cql_content, re.IGNORECASE | re.DOTALL
        ), (
            "Expecting CLUSTERING ORDER BY (created_at DESC, ...)\n"
            "This allows you to view the user’s latest edits effectively"
        )


# ── Block 4: indexes ───────────────────────────────────────────────────────────

class TestIndexes:
    

    def test_domain_index_exists(self, cql_content):
        
        assert re.search(
            r"CREATE\s+INDEX.*ON\s+edits\s*\(\s*domain\s*\)",
            cql_content, re.IGNORECASE | re.DOTALL
        ), "Index is abscent by field- domain"

    def test_page_title_index_exists(self, cql_content):
        
        assert re.search(
            r"CREATE\s+INDEX.*ON\s+edits\s*\(\s*page_title\s*\)",
            cql_content, re.IGNORECASE | re.DOTALL
        ), "Index is abscent by field page_title"


# ── Block 5: the overall integrity of the file ────────────────────────────────────────

class TestCQLFileIntegrity:
    

    def test_cql_file_exists(self):
        
        assert CQL_PATH.exists(), f"init.cql not found: {CQL_PATH}"

    def test_cql_file_not_empty(self, cql_content):
        
        assert len(cql_content.strip()) > 100, "init.cql too short"

    def test_cql_has_use_statement(self, cql_content):
        
        assert re.search(r"USE\s+wiki_namespace", cql_content, re.IGNORECASE), (
            "Відсутній 'USE wiki_namespace;' — tab can be created in system keyspace"
        )

    def test_no_drop_statements(self, cql_content):
        
        assert "DROP TABLE" not in cql_content.upper(), \
            "init.cql contains a DROP TABLE statement — dangerous for production!"
        assert "DROP KEYSPACE" not in cql_content.upper(), \
            "init.cql містить DROP KEYSPACE — dangerous!"


# ── Additional features ─────────────────────────────────────────────────────────

def _extract_table_section(cql_content: str) -> str | None:
    
    match = re.search(
        r"CREATE\s+TABLE.*?edits\s*\(.*?\)\s*(?:WITH.*?;|;)",
        cql_content, re.IGNORECASE | re.DOTALL
    )
    return match.group() if match else None