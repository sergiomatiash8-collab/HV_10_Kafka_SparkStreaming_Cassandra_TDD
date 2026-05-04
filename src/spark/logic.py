# import json
# 
# def transform_wikipedia_event(raw_json):
#
#     try:
#         data = json.loads(raw_json)
#         
#         
#         meta = data.get("meta", {})
#         performer = data.get("performer", {})
#         
#         return {
#             "id": meta.get("id"),
#             "page_title": data.get("page_title"),
#             "user_text": performer.get("user_text"),
#             "dt": meta.get("dt")
#         }
#     except (json.JSONDecodeError, AttributeError, TypeError):
#         return None