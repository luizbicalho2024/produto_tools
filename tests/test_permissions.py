from services.flow_permissions import can_approve, can_edit, can_review, permission_for


def test_permissions_and_collaborators():
    record = {
        "owner_username": "owner",
        "visibility": "private",
        "collaborators": [
            {"username": "viewer", "level": "viewer"},
            {"username": "editor", "level": "editor"},
            {"username": "approver", "level": "approver"},
        ],
    }
    assert permission_for(record, "owner") == "owner"
    assert permission_for(record, "viewer") == "viewer"
    assert can_edit(permission_for(record, "editor"))
    assert can_review(permission_for(record, "approver"))
    assert can_approve(permission_for(record, "approver"))
    assert not can_edit(permission_for(record, "viewer"))
