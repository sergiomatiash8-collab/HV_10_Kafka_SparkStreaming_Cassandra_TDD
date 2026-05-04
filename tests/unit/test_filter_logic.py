import json
import pytest

ALLOWED_DOMAINS = ["en.wikipedia.org", "www.wikidata.org", "commons.wikimedia.org"]

def should_pass_filter(event: dict) -> bool:
    try:
        domain = event.get("meta", {}).get("domain")
        is_bot = event.get("performer", {}).get("user_is_bot", True)
        return domain in ALLOWED_DOMAINS and not is_bot
    except (AttributeError, TypeError):
        return False

def make_event(domain="en.wikipedia.org", is_bot=False,
               user_text="TestUser", page_title="TestPage") -> dict:
    return {
        "meta": {
            "id": "550e8400-e29b-41d4-a716-446655440000",
            "dt": "2026-05-01T10:00:00.000Z",
            "domain": domain,
        },
        "page_title": page_title,
        "performer": {
            "user_text": user_text,
            "user_is_bot": is_bot,
        }
    }

class TestDomainFiltering:
    def test_domain_filtering_allowed_en_wikipedia(self):
        event = make_event(domain="en.wikipedia.org")
        assert should_pass_filter(event) is True

    def test_domain_filtering_allowed_wikidata(self):
        event = make_event(domain="www.wikidata.org")
        assert should_pass_filter(event) is True

    def test_domain_filtering_allowed_commons(self):
        event = make_event(domain="commons.wikimedia.org")
        assert should_pass_filter(event) is True

    def test_domain_filtering_blocked_de_wikipedia(self):
        event = make_event(domain="de.wikipedia.org")
        assert should_pass_filter(event) is False

    def test_domain_filtering_none_domain(self):
        event = {"meta": {"domain": None}, "performer": {"user_is_bot": False}}
        assert should_pass_filter(event) is False

    @pytest.mark.parametrize("domain", ALLOWED_DOMAINS)
    def test_all_allowed_domains_pass(self, domain):
        event = make_event(domain=domain)
        assert should_pass_filter(event) is True

class TestBotFiltering:
    def test_bot_filtering_human_user_passes(self):
        event = make_event(is_bot=False)
        assert should_pass_filter(event) is True

    def test_bot_filtering_bot_is_blocked(self):
        event = make_event(is_bot=True)
        assert should_pass_filter(event) is False

    def test_bot_filtering_missing_user_is_bot_defaults_to_blocked(self):
        event = {
            "meta": {"domain": "en.wikipedia.org", "dt": "2026-05-01T10:00:00Z"},
            "page_title": "Test",
            "performer": {"user_text": "MysteriousUser"}
        }
        assert should_pass_filter(event) is False

class TestCombinedFiltering:
    @pytest.mark.parametrize("domain,is_bot,expected", [
        ("en.wikipedia.org",      False, True),
        ("www.wikidata.org",      False, True),
        ("commons.wikimedia.org", False, True),
        ("en.wikipedia.org",      True,  False),
        ("de.wikipedia.org",      False, False),
    ])
    def test_filter_matrix(self, domain, is_bot, expected):
        event = make_event(domain=domain, is_bot=is_bot)
        assert should_pass_filter(event) is expected

class TestFieldExtraction:
    def extract_fields(self, event: dict) -> dict | None:
        if not should_pass_filter(event):
            return None
        return {
            "user_id":    event["performer"]["user_text"],
            "domain":     event["meta"]["domain"],
            "created_at": event["meta"]["dt"],
            "page_title": event["page_title"],
        }

    def test_extract_all_four_required_fields(self):
        event = make_event(
            domain="en.wikipedia.org",
            user_text="SomeEditor",
            page_title="Python",
            is_bot=False
        )
        result = self.extract_fields(event)
        assert result is not None
        assert set(result.keys()) == {"user_id", "domain", "created_at", "page_title"}