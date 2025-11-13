# services/elasticsearch_service.py
import os, time
from dotenv import load_dotenv
load_dotenv()
from typing import Dict, Any
from elasticsearch import Elasticsearch, NotFoundError

ES = Elasticsearch(
    os.getenv("ES_URL", "http://localhost:9200"),
    request_timeout=10,
    retry_on_timeout=True,
)

INDEX = os.getenv("INDEX", "plans").strip()        # <- allow .env override

INDEX = os.getenv("INDEX", "plans").strip()
ALIAS = os.getenv("ALIAS", "indexplan").strip()

# ----- helpers ---------------------------------------------------------------

def _routing(pid: str) -> Dict[str, Any]:
    return {"routing": pid, "refresh": "wait_for"}

def _ensure_alias():
    # idempotent alias add
    ES.indices.put_alias(index=INDEX, name=ALIAS, ignore=[400, 404])

def ensure_index() -> None:
    """Create index (if needed) with join field 'rel' and add alias."""
    if ES.indices.exists(index=INDEX):
        _ensure_alias()
        return

    body = {
        "settings": {"index": {"number_of_shards": 1, "number_of_replicas": 0}},
        "mappings": {
            "properties": {
                "rel": {
                    "type": "join",
                    "relations": {
                        "plan": ["planCostShares", "linkedPlanServices", "planserviceCostShares"]
                    },
                    "eager_global_ordinals": True
                },
                "_org": {"type": "keyword"},
                "objectId": {"type": "keyword"},
                "objectType": {"type": "keyword"},
                "planType": {"type": "keyword"},
                "creationDate": {"type": "date", "format": "yyyy-MM-dd||strict_date_optional_time"},
                "name": {"type": "text"},
                "copay": {"type": "float"},
                "deductible": {"type": "float"},
            }
        },
    }
    ES.indices.create(index=INDEX, body=body)
    _ensure_alias()

def _upsert_parent(plan_id: str, fields: Dict[str, Any]) -> None:
    if not fields:
        return
    doc = {"objectId": plan_id, "objectType": "plan", "rel": "plan", **fields}
    ES.index(index=INDEX, id=plan_id, document=doc, **_routing(plan_id))

def _upsert_child(parent_id: str, child_id: str, rel_name: str, doc: Dict[str, Any]) -> None:
    payload = {**doc, "rel": {"name": rel_name, "parent": parent_id}}
    ES.index(index=INDEX, id=child_id, document=payload, **_routing(parent_id))

# ----- public API used by worker --------------------------------------------

def index_plan(plan_doc: Dict[str, Any]) -> None:
    """
    Fan-out a full plan JSON (like from POST/PUT) into:
      - parent plan
      - child planCostShares (if present)
      - child linkedPlanServices + planserviceCostShares (if present)
    """
    plan_id = plan_doc.get("objectId")
    if not plan_id:
        raise ValueError("index_plan: objectId (plan id) is required")

    parent_fields = {k: plan_doc.get(k) for k in ("_org", "planType", "creationDate") if k in plan_doc}
    _upsert_parent(plan_id, parent_fields)

    # planCostShares (single)
    pcs = plan_doc.get("planCostShares") or {}
    if pcs.get("objectId"):
        _upsert_child(plan_id, pcs["objectId"], "planCostShares", pcs)

    # linkedPlanServices (array): may include linkedService and planserviceCostShares
    for item in plan_doc.get("linkedPlanServices", []) or []:
        svc = item.get("linkedService") or {}
        if svc.get("objectId"):
            _upsert_child(plan_id, svc["objectId"], "linkedPlanServices", svc)

        pscs = item.get("planserviceCostShares") or {}
        if pscs.get("objectId"):
            _upsert_child(plan_id, pscs["objectId"], "planserviceCostShares", pscs)

def patch_plan(plan_id: str, updates: Dict[str, Any]) -> None:
    """
    Partial update. Supports:
      - plan-level fields: _org, planType, creationDate
      - linkedPlanServices[] items: upserts for linkedService and planserviceCostShares
      - planCostShares object
    """
    # Merge all top-level fields except child arrays/objects into parent
    exclude_keys = {"planCostShares", "linkedPlanServices"}
    parent_fields = {k: v for k, v in updates.items() if k not in exclude_keys}
    _upsert_parent(plan_id, parent_fields)

    if "planCostShares" in updates:
        pcs = updates.get("planCostShares") or {}
        if pcs.get("objectId"):
            _upsert_child(plan_id, pcs["objectId"], "planCostShares", pcs)

    for item in updates.get("linkedPlanServices", []) or []:
        svc = item.get("linkedService") or {}
        if svc.get("objectId"):
            _upsert_child(plan_id, svc["objectId"], "linkedPlanServices", svc)

        pscs = item.get("planserviceCostShares") or {}
        if pscs.get("objectId"):
            _upsert_child(plan_id, pscs["objectId"], "planserviceCostShares", pscs)

def delete_plan_from_index(plan_id: str) -> None:
    """Delete parent and cascade delete children via routing."""
    # Delete parent document
    try:
        ES.delete(index=INDEX, id=plan_id, **_routing(plan_id))
    except NotFoundError:
        pass
    # Delete all child documents routed to this parent
    ES.delete_by_query(
        index=INDEX,
        body={"query": {"has_parent": {"parent_type": "plan", "query": {"term": {"_id": plan_id}}}}},
        refresh=True,
    )
    # Also delete any docs with _routing = plan_id (for completeness)
    ES.delete_by_query(
        index=INDEX,
        body={"query": {"term": {"_routing": plan_id}}},
        refresh=True,
    )
