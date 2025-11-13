# routes/plan_routes.py
from flask import Blueprint
from utils.auth import require_auth
from controllers.plan_controller import (
    create_plan, get_plan, put_plan, patch_plan, delete_plan
)

bp = Blueprint("plans", __name__, url_prefix="/api/v1")

@bp.post("/plan")
@require_auth
def create():
    return create_plan()

@bp.get("/plan/<string:object_id>")
@require_auth
def get_(object_id):
    return get_plan(object_id)

@bp.put("/plan/<string:object_id>")
@require_auth
def put_(object_id):
    return put_plan(object_id)

@bp.patch("/plan/<string:object_id>")
@require_auth
def patch_(object_id):
    return patch_plan(object_id)

@bp.delete("/plan/<string:object_id>")
@require_auth
def delete_(object_id):
    return delete_plan(object_id)
