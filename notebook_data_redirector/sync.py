import common


def lambda_handler(event, context):
    mode = event.get("mode", "drain-queue")
    common.log_action("INFO", "sync", "sync_start", mode=mode)

    if mode == "full-sync":
        _full_sync()
        return
    elif mode == "drain-queue":
        common.log_action("INFO", "sync", "mode_not_implemented", mode=mode)
        return
    elif mode == "reconciliation":
        common.log_action("INFO", "sync", "mode_not_implemented", mode=mode)
        return
    else:
        common.log_action("WARNING", "sync", "unknown_mode", mode=mode)
        return


def _full_sync():
    ddb_table = common.get_ddb_table()
    box_client = common.get_box_client()
    root_folder = common.get_folder(box_client, common.BOX_FOLDER_ID)
    root_shared = common.is_box_object_public(root_folder)

    common.log_action("INFO", "sync", "checking_box_files")
    shared_file_ids = set()
    shared_filepaths = set()
    count = 0
    for file, shared in common.iterate_files(root_folder, shared=root_shared):
        count += 1
        if (not common.is_box_object_public(file)) and shared:
            # this includes an API call
            file = common.create_shared_link(box_client, file, access="open", allow_download=True)
        elif (common.is_box_object_public(file)) and (not shared):
            file = common.remove_shared_link(box_client, file)

        if common.is_box_object_public(file):
            shared_file_ids.add(file.id)
            shared_filepaths.add(common.get_filepath(file))
            common.put_file_item(ddb_table, file)
        else:
            common.delete_file_item(ddb_table, file)
    common.log_action("INFO", "sync", "box_files_processed", count=count)

    common.log_action("INFO", "sync", "checking_ddb_items")
    count = 0
    scan_response = ddb_table.scan()
    delete_keys = set()
    while True:
        for item in scan_response["Items"]:
            count += 1
            if (item["box_file_id"] not in shared_file_ids) or (item["filepath"] not in shared_filepaths):
                delete_keys.add(item["filepath"])

        # If the data returned by a scan would exceed 1MB, DynamoDB will begin paging.
        # The LastEvaluatedKey field is the placeholder used to request the next page.
        if scan_response.get("LastEvaluatedKey"):
            scan_response = ddb_table.scan(ExclusiveStartKey=scan_response["LastEvaluatedKey"])
        else:
            # this clause isn't reached by testing atm
            break

    for key in delete_keys:
        ddb_table.delete_item(Key={"filepath": key})
    common.log_action("INFO", "sync", "ddb_items_processed", count=count)
