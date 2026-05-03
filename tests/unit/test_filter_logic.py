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
 
 
#  domain filtering ──────────────────────────────────────────────────
 
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
 
    def test_domain_filtering_blocked_fr_wikipedia(self):
        
        event = make_event(domain="fr.wikipedia.org")
        assert should_pass_filter(event) is False
 
    def test_domain_filtering_blocked_unknown_domain(self):
        
        event = make_event(domain="random.example.com")
        assert should_pass_filter(event) is False
 
    def test_domain_filtering_empty_domain(self):
        
        event = make_event(domain="")
        assert should_pass_filter(event) is False
 
    def test_domain_filtering_none_domain(self):
        
        event = {"meta": {"domain": None}, "performer": {"user_is_bot": False}}
        assert should_pass_filter(event) is False
 
    def test_domain_filtering_missing_meta(self):
        
        event = {"page_title": "Test", "performer": {"user_is_bot": False}}
        assert should_pass_filter(event) is False
 
    @pytest.mark.parametrize("domain", ALLOWED_DOMAINS)
    def test_all_allowed_domains_pass(self, domain):
        
        event = make_event(domain=domain)
        assert should_pass_filter(event) is True, f"Домен {domain} має проходити фільтр"
 
 
# ── bot filtering ─────────────────────────────────────────────────────
 
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
            # user_is_bot відсутній
        }
        assert should_pass_filter(event) is False
 
    def test_bot_filtering_explicit_false_string_is_blocked(self):
       
        event = make_event()
        event["performer"]["user_is_bot"] = "false"  # рядок, не bool
        # "false" як рядок — truthy → фільтруємо
        assert should_pass_filter(event) is False
 
    def test_bot_filtering_named_bot_account(self):
        
        event = make_event(user_text="AutoWikiBot", is_bot=True)
        assert should_pass_filter(event) is False
 
    def test_bot_filtering_named_bot_but_flagged_human(self):
       
        event = make_event(user_text="RoboticsEnthusiast", is_bot=False)
        assert should_pass_filter(event) is True
 
 
# ── combined ─────────────────────────────────────────────────
 
class TestCombinedFiltering:
    
 
    def test_combined_allowed_domain_and_human_passes(self):
        
        event = make_event(domain="en.wikipedia.org", is_bot=False)
        assert should_pass_filter(event) is True
 
    def test_combined_allowed_domain_but_bot_is_blocked(self):
        
        event = make_event(domain="en.wikipedia.org", is_bot=True)
        assert should_pass_filter(event) is False
 
    def test_combined_human_but_blocked_domain_is_filtered(self):
        
        event = make_event(domain="uk.wikipedia.org", is_bot=False)
        assert should_pass_filter(event) is False
 
    def test_combined_bot_and_blocked_domain_both_fail(self):
        
        event = make_event(domain="uk.wikipedia.org", is_bot=True)
        assert should_pass_filter(event) is False
 
    @pytest.mark.parametrize("domain,is_bot,expected", [
        ("en.wikipedia.org",      False, True),   
        ("www.wikidata.org",      False, True),   
        ("commons.wikimedia.org", False, True),   
        ("en.wikipedia.org",      True,  False),  
        ("de.wikipedia.org",      False, False),  
        ("de.wikipedia.org",      True,  False),  
    ])
    def test_filter_matrix(self, domain, is_bot, expected):
        
        event = make_event(domain=domain, is_bot=is_bot)
        assert should_pass_filter(event) is expected
 
 
# ── transformation after filtering ─────────────────────────────
 
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
 
    def test_extract_user_id_from_performer_user_text(self):
        
        event = make_event(user_text="WikiEditor42", domain="en.wikipedia.org")
        result = self.extract_fields(event)
        assert result is not None
        assert result["user_id"] == "WikiEditor42"
 
    def test_extract_domain_from_meta(self):
    
        event = make_event(domain="www.wikidata.org")
        result = self.extract_fields(event)
        assert result["domain"] == "www.wikidata.org"
 
    def test_extract_created_at_from_meta_dt(self):
        
        event = make_event()
        event["meta"]["dt"] = "2026-05-01T10:00:00.000Z"
        result = self.extract_fields(event)
        assert result["created_at"] == "2026-05-01T10:00:00.000Z"
 
    def test_extract_page_title(self):
        
        event = make_event(page_title="Quantum Computing")
        result = self.extract_fields(event)
        assert result["page_title"] == "Quantum Computing"
 
    def test_extract_all_four_required_fields(self):
        
        event = make_event(
            domain="en.wikipedia.org",
            user_text="SomeEditor",
            page_title="Python (programming language)",
            is_bot=False
        )
        result = self.extract_fields(event)
        assert result is not None
        assert set(result.keys()) == {"user_id", "domain", "created_at", "page_title"}
 
    def test_filtered_event_returns_none(self):
        
        event = make_event(is_bot=True)
        result = self.extract_fields(event)
        assert result is None