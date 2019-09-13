from redirector import common


def test_is_box_file_public(create_file):
    unshared_file = create_file(shared_link=None)
    assert common.is_box_file_public(unshared_file) is False

    shared_incorrect_access_file = create_file(
        shared_link={"effective_access": "company", "effective_permission": "can_download"}
    )
    assert common.is_box_file_public(shared_incorrect_access_file) is False

    shared_incorrect_permission_file = create_file(
        shared_link={"effective_access": "public", "effective_permission": "can_preview"}
    )
    assert common.is_box_file_public(shared_incorrect_permission_file) is False

    shared_file = create_file(shared_link={"effective_access": "open", "effective_permission": "can_download"})
    assert common.is_box_file_public(shared_file) is True
