from flask import request, jsonify, make_response
from utils.schema_validator import validate_schema
from constants.schema import plan_schema
from services.redis_service import set_data, get_data, delete_data
from services.elasticsearch_service import (
    # no direct ES calls anymore; worker will do them
    # keep delete function in case you still want sync fallback later
    delete_plan_from_index
)
from services.queue_service import publish
import hashlib
import json

# --------- helpers ---------
def _key(object_id: str) -> str:
    return f"plan_{object_id}"

def _etag(payload: dict) -> str:
    return hashlib.md5(json.dumps(payload, sort_keys=True).encode()).hexdigest()

def _publish_safe(job: dict):
    """Publish to RabbitMQ; if queue is down, tell the client (503) instead of 500."""
    try:
        publish(job)
        return None
    except Exception as e:
        return jsonify({"message": "Queue unavailable", "error": str(e)}), 503


# --------- CREATE (POST) ---------
def create_plan():
    data = request.get_json() or {}

    valid, error = validate_schema(plan_schema, data)
    if not valid:
        return jsonify({"message": "Invalid data", "errors": error}), 400

    object_id = data.get("objectId")
    if not object_id:
        return jsonify({"message": "objectId is required"}), 400

    key = _key(object_id)
    set_data(key, data)

    # enqueue indexing (async) -> worker will index parent + children
    err = _publish_safe({"type": "index", "id": object_id, "doc": data})
    if err:
        return err

    etag = _etag(data)
    resp = make_response(jsonify({"message": "Plan created successfully", "objectId": object_id}), 201)
    resp.headers["ETag"] = etag
    resp.headers["Location"] = f"/api/v1/plan/{object_id}"
    return resp


# --------- READ (GET) with conditional 304 ---------
def get_plan(object_id):
    data = get_data(_key(object_id))
    if not data:
        return jsonify({"message": "Not found"}), 404

    etag = _etag(data)
    if request.headers.get("If-None-Match") == etag:
        # Not Modified
        return "", 304

    resp = make_response(jsonify(data), 200)
    resp.headers["ETag"] = etag
    return resp


# --------- FULL REPLACE (PUT) ---------
def put_plan(object_id):
    payload = request.get_json() or {}

    valid, error = validate_schema(plan_schema, payload)
    if not valid:
        return jsonify({"message": "Invalid data", "errors": error}), 400

    # Optional advanced semantics: only update if client has fresh copy
    existing = get_data(_key(object_id))
    if existing:
        client_if_match = request.headers.get("If-Match")
        if client_if_match and client_if_match != _etag(existing):
            return jsonify({"message": "Resource changed"}), 412

    # save to KV
    set_data(_key(object_id), payload)

    # enqueue full upsert/replace (just reuse "index" job type)
    err = _publish_safe({"type": "index", "id": object_id, "doc": payload})
    if err:
        return err

    etag = _etag(payload)
    resp = make_response(jsonify({"message": "Plan created successfully", "objectId": object_id}), 201)
    resp.headers["ETag"] = etag
    return resp


# --------- MERGE (PATCH) ---------
def patch_plan(object_id):
    key = _key(object_id)
    existing = get_data(key)
    if not existing:
        return jsonify({"message": "Not found"}), 404

    # Optional: optimistic concurrency via If-Match
    client_if_match = request.headers.get("If-Match")
    current_etag = _etag(existing)
    if client_if_match and client_if_match != current_etag:
        return jsonify({"message": "Resource changed"}), 412

    updates = request.get_json() or {}

    # shallow merge is fine for top-level, but do not try to merge arrays here
    merged = {**existing, **{k: v for k, v in updates.items() if k != "linkedPlanServices"}}

    # keep original array unless client sent a new one
    if "linkedPlanServices" in updates:
        merged["linkedPlanServices"] = updates["linkedPlanServices"]

    valid, error = validate_schema(plan_schema, merged)
    if not valid:
        return jsonify({"message": "Invalid data", "errors": error}), 400

    # persist merged doc in KV
    set_data(key, merged)

    # Build a normalized summary of what we're patching, so the worker can fan-out
    applied = []
    child_ops = []

    # Plan-level fields that changed (optional)
    for fld in ("planType", "_org", "creationDate"):
        if fld in updates:
            applied.append({"type": "plan", fld: updates[fld]})

    # Children: linkedPlanServices (service + cost shares)
    for item in updates.get("linkedPlanServices", []):
        svc = item.get("linkedService") or {}
        if svc.get("objectId"):
            child_ops.append({
                "type": "linkedService",
                "parentId": object_id,
                "id": svc["objectId"],
                "doc": svc
            })
            applied.append({"type": "linkedService", **svc})

        cost = item.get("planserviceCostShares") or {}
        if cost.get("objectId"):
            child_ops.append({
                "type": "planserviceCostShares",
                "parentId": object_id,
                "id": cost["objectId"],
                "doc": cost
            })
            applied.append({"type": "planserviceCostShares", **cost})

    # enqueue partial update job with explicit child ops
    job = {
        "type": "patch",
        "id": object_id,
        "plan_doc": {k: updates[k] for k in ("planType", "_org", "creationDate") if k in updates},
        "child_ops": child_ops
    }
    err = _publish_safe(job)
    if err:
        return err

    new_etag = _etag(merged)
    resp = make_response(jsonify({
        "planId": object_id,
        "applied": applied
    }), 200)
    resp.headers["ETag"] = new_etag
    return resp

# --------- DELETE (cascaded) ---------
def delete_plan(object_id):
    key = _key(object_id)
    existed = bool(get_data(key))
    delete_data(key)

    # enqueue cascaded delete in ES
    err = _publish_safe({"type": "delete", "id": object_id})
    if err:
        return err

    # If it didn't exist in KV, return 404; else async accepted
    if not existed:
        return jsonify({"message": "Plan not found"}), 404

    return "", 202
