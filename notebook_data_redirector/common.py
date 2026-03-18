import os
import json
import logging
import time
from datetime import datetime, timezone

import boto3
from boto3.dynamodb.conditions import Key
import urllib3
import urllib3.util
from boxsdk import Client, JWTAuth
from boxsdk.exception import BoxAPIException
from boxsdk.object.webhook import Webhook

SECRET_ARN = os.environ["SECRET_ARN"]
MANIFEST_TABLE_NAME = os.environ["MANIFEST_TABLE_NAME"]
BOX_FOLDER_ID = os.environ["BOX_FOLDER_ID"]
SECRET_ROLE_ARN = os.environ["SECRET_ROLE_ARN"]
VALIDATION_QUEUE_TABLE_NAME = os.environ.get("VALIDATION_QUEUE_TABLE_NAME", "")
SYNC_STATE_TABLE_NAME = os.environ.get("SYNC_STATE_TABLE_NAME", "")
# NOTE: FolderCacheTable exists in DDB but is not currently used. Folder sharing
# state is cached in-memory (_folder_sharing_cache) which is sufficient since each
# Lambda invocation processes many files sharing the same ancestor folders. A future
# improvement could persist folder sharing state to DDB so it survives across
# invocations, reducing Box API calls when multiple Lambdas check the same folders.
FOLDER_CACHE_TABLE_NAME = os.environ.get("FOLDER_CACHE_TABLE_NAME", "")
EVENT_DEDUP_TABLE_NAME = os.environ.get("EVENT_DEDUP_TABLE_NAME", "")
WEBHOOK_QUEUE_TABLE_NAME = os.environ.get("WEBHOOK_QUEUE_TABLE_NAME", "")
SERVICE_ACCOUNT_USER_ID = os.environ.get("SERVICE_ACCOUNT_USER_ID", "")

STALE_THRESHOLD_SECONDS = 72000  # 20 hours
FOLDER_CACHE_TTL = 300  # 5 minutes
EVENT_DEDUP_TTL_SECONDS = 172800  # 48 hours
WEBHOOK_QUEUE_TTL_SECONDS = 604800  # 7 days


# --- Structured JSON Logging ---


class _JsonFormatter(logging.Formatter):
    def format(self, record):
        entry = {
            "level": record.levelname,
            "message": record.getMessage(),
        }
        if hasattr(record, "_structured"):
            for key, value in record._structured.items():
                if value is not None:
                    entry[key] = value
        return json.dumps(entry, default=str)


def _configure_logging():
    root = logging.getLogger()
    if not any(isinstance(h.formatter, _JsonFormatter) for h in root.handlers):
        # Replace existing handlers' formatters rather than nuking them,
        # so Lambda's built-in CloudWatch handler (and its request ID
        # correlation) is preserved.
        if root.handlers:
            for h in root.handlers:
                h.setFormatter(_JsonFormatter())
        else:
            handler = logging.StreamHandler()
            handler.setFormatter(_JsonFormatter())
            root.addHandler(handler)
        root.setLevel(logging.INFO)

    # Suppress noisy/dangerous third-party loggers — boxsdk and urllib3
    # emit JWT assertions and client_secret at INFO level during auth.
    for name in ("boxsdk", "urllib3", "boto3", "botocore"):
        logging.getLogger(name).setLevel(logging.WARNING)


_configure_logging()

LOGGER = logging.getLogger(__name__)


def log_action(level, function, action, **kwargs):
    """Emit a structured JSON log entry.

    Required: level, function, action.
    Optional kwargs (omitted when None): filepath, box_file_id,
    duration_ms, error_type, remediation.
    """
    extra = {"function": function, "action": action}
    for key, value in kwargs.items():
        if value is not None:
            extra[key] = value

    logger = logging.getLogger(function)
    log_func = getattr(logger, level.lower())
    log_func(action, extra={"_structured": extra})


# --- HTTP Pool (lazy-init for URL validation) ---

_http = None

# --- Folder Sharing Cache (in-invocation, TTL-based) ---

_folder_sharing_cache = {}  # {folder_id: {"public": bool, "checked_at": float}}


def _get_http_pool():
    global _http
    if _http is None:
        _http = urllib3.PoolManager(num_pools=1)
    return _http


def validate_download_url(url):
    """Validate a Box download URL via 1-byte Range GET.

    Returns "valid", "broken", or "uncertain".
    Caller is responsible for all logging.
    """
    try:
        response = _get_http_pool().request(
            "GET",
            url,
            headers={"Range": "bytes=0-0"},
            timeout=urllib3.Timeout(connect=5.0, read=5.0),
            retries=urllib3.util.Retry(redirect=3),
            preload_content=False,
        )
        status = response.status
        response.release_conn()
        if status in (206, 200):
            return "valid"
        elif status in (403, 404):
            return "broken"
        else:
            return "uncertain"
    except Exception:
        return "uncertain"


def is_stale(last_validated):
    """Return True if last_validated is None or older than 20 hours."""
    if last_validated is None:
        return True
    validated_at = datetime.fromisoformat(last_validated).replace(tzinfo=timezone.utc)
    age_seconds = (datetime.now(timezone.utc) - validated_at).total_seconds()
    return age_seconds > STALE_THRESHOLD_SECONDS


# --- DDB Table Accessors ---


def get_validation_queue_table():
    return boto3.resource("dynamodb").Table(VALIDATION_QUEUE_TABLE_NAME)


def get_sync_state_table():
    return boto3.resource("dynamodb").Table(SYNC_STATE_TABLE_NAME)


def get_folder_cache_table():
    # Currently unused — see note on FOLDER_CACHE_TABLE_NAME above.
    return boto3.resource("dynamodb").Table(FOLDER_CACHE_TABLE_NAME)


def get_event_dedup_table():
    return boto3.resource("dynamodb").Table(EVENT_DEDUP_TABLE_NAME)


def get_webhook_queue_table():
    return boto3.resource("dynamodb").Table(WEBHOOK_QUEUE_TABLE_NAME)


# --- Box Client Lifecycle (lazy-init + caching) ---

_cached_secret = None
_box_client = None
_box_client_expires_at = 0


def _get_cached_secret():
    global _cached_secret
    if _cached_secret is None:
        _cached_secret = _get_secret()
    return _cached_secret


def get_webhook_signature_key():
    """Return the webhook signature key without initializing a Box client."""
    return _get_cached_secret()["box_webhook_signature_key"]


def get_box_client():
    """Lazy-init Box client, re-init on JWT expiry (55-min TTL)."""
    global _box_client, _box_client_expires_at
    if _box_client is None or time.time() > _box_client_expires_at:
        _box_client = _create_box_client()
        _box_client_expires_at = time.time() + 3300  # 55 min
    return _box_client


def _create_box_client():
    secret = _get_cached_secret()

    auth = JWTAuth(
        client_id=secret["box_client_id"],
        client_secret=secret["box_client_secret"],
        enterprise_id=secret["box_enterprise_id"],
        jwt_key_id=secret["box_jwt_key_id"],
        rsa_private_key_data=secret["box_rsa_private_key_data"],
        rsa_private_key_passphrase=secret["box_rsa_private_key_passphrase"],
    )
    auth.authenticate_instance()

    client = Client(auth)

    users = client.users()
    try:
        app_user = next(users)
    except StopIteration:
        log_action("WARNING", "common", "no_app_user", remediation="using service account as box client")
        return client

    return client.as_user(app_user)


def _clear_box_client_cache():
    global _box_client, _box_client_expires_at
    _box_client = None
    _box_client_expires_at = 0


def _reset_all_caches():
    """Test-only: clear all module-level caches."""
    global _cached_secret, _box_client, _box_client_expires_at, _http, _folder_sharing_cache
    _cached_secret = None
    _box_client = None
    _box_client_expires_at = 0
    _http = None
    _folder_sharing_cache = {}


def with_box_retry(func, *args, **kwargs):
    """Call func; on 401 BoxAPIException, clear client cache and retry once."""
    try:
        return func(*args, **kwargs)
    except BoxAPIException as e:
        if e.status == 401:
            log_action(
                "WARNING",
                "common",
                "box_auth_retry",
                error_type="jwt_expired",
                remediation="cache_cleared_retry",
            )
            _clear_box_client_cache()
            return func(*args, **kwargs)
        raise


def validate_webhook_message(body, headers, signature_key):
    """Validate a webhook message signature without requiring a Box client."""
    return Webhook.validate_message(body, headers, signature_key)


HANDLED_FILE_TRIGGERS = {
    "SHARED_LINK.CREATED",
    "SHARED_LINK.UPDATED",
    "SHARED_LINK.DELETED",
    "FILE.TRASHED",
    "FILE.RESTORED",
    "FILE.MOVED",
    "FILE.RENAMED",
    "FILE.DELETED",
    "FILE.UPLOADED",
    "FILE.COPIED",
}

HANDLED_FOLDER_TRIGGERS = {
    "SHARED_LINK.CREATED",
    "SHARED_LINK.UPDATED",
    "SHARED_LINK.DELETED",
    "FOLDER.RESTORED",
    "FOLDER.TRASHED",
    "FOLDER.MOVED",
    "FOLDER.RENAMED",
    "FOLDER.DELETED",
    "FOLDER.COPIED",
}

HANDLED_TRIGGERS = HANDLED_FILE_TRIGGERS | HANDLED_FOLDER_TRIGGERS

GET_ITEMS_FIELDS = {"name", "path_collection", "shared_link"}

GET_ITEMS_LIMIT = 1000


def is_box_object_public(file):
    if not hasattr(file, "shared_link"):
        raise RuntimeError("cannot operate on summary file, call get() first")

    return (
        file.shared_link is not None
        and file.shared_link["effective_access"] == "open"
        and file.shared_link["effective_permission"] == "can_download"
    )


def is_any_parent_public(client, file):
    # checks if any parent folder of the file is public
    # necessary due to changes in the Box API when a folder is shared
    filepath_collection = file.path_collection
    start_index = [e["id"] for e in filepath_collection["entries"]].index(BOX_FOLDER_ID)
    for fpc in filepath_collection["entries"][start_index:]:
        folder = get_folder(client, fpc["id"]).get()
        if is_box_object_public(folder):
            return True

    return False


def _get_cached_folder_sharing(folder_id):
    entry = _folder_sharing_cache.get(folder_id)
    if entry and (time.time() - entry["checked_at"]) < FOLDER_CACHE_TTL:
        return entry["public"]
    return None


def _cache_folder_sharing(folder_id, public):
    _folder_sharing_cache[folder_id] = {"public": public, "checked_at": time.time()}


def ensure_folder_shared(client, file):
    """Walk ancestor folders from file toward root, correcting sharing as needed.

    Stops early if an ancestor is already correctly shared (inheritance).
    Uses module-level cache to avoid redundant Box API calls within an invocation.
    """
    entries = file.path_collection["entries"]
    folder_ids = [e["id"] for e in entries]

    if BOX_FOLDER_ID not in folder_ids:
        return  # file is outside the managed tree

    root_idx = folder_ids.index(BOX_FOLDER_ID)
    # Walk bottom-up (from immediate parent toward root) for efficiency
    for entry in reversed(entries[root_idx:]):
        folder_id = entry["id"]

        cached = _get_cached_folder_sharing(folder_id)
        if cached is True:
            break  # correctly shared — all ancestors above are also correct

        if cached is None:
            # Not cached — read from Box
            folder = with_box_retry(get_folder, client, folder_id)
            if folder is None:
                continue  # folder deleted? skip
            if is_box_object_public(folder):
                _cache_folder_sharing(folder_id, True)
                break  # correctly shared — stop walking

        # Need to fix sharing
        with_box_retry(folder.create_shared_link, access="open", allow_download=True)
        log_action("INFO", "sync", "fix_folder_sharing", folder_id=folder_id)
        _cache_folder_sharing(folder_id, True)


def create_shared_link(client, file, **boxargs):
    if not hasattr(file, "shared_link"):
        raise RuntimeError("cannot operate on summary file, call get() first")
    # technically this could be a file or a folder
    # create_shared_link returns a new object with the shared link; the original object is not modified
    # see boxsdk docstring
    return file.create_shared_link(**boxargs)


def remove_shared_link(client, file):
    if not hasattr(file, "shared_link"):
        raise RuntimeError("cannot operate on summary file, call get() first")
    # unlike create_shared_link, remove_shared_link returns a boolean indicating whether the operation was successful
    # to avoid confusion, I'm going to get and return the new file without the shared link
    response = file.remove_shared_link()
    if not response:
        # not sure how to reach this in testing
        raise RuntimeError("boxsdk API call to remove_shared_link returned False")
    return file.get()


def get_ddb_table():
    return boto3.resource("dynamodb").Table(MANIFEST_TABLE_NAME)


def get_filepath(file):
    # want to start the path after "All Files/<BoxFolderName>/"
    filepath_collection = file.path_collection
    start_index = [e["id"] for e in filepath_collection["entries"]].index(BOX_FOLDER_ID) + 1
    filepath_tokens = [fp["name"] for fp in filepath_collection["entries"][start_index:]] + [file.name]
    return "/".join(filepath_tokens)


def make_ddb_item(file):
    return {"filepath": get_filepath(file), "box_file_id": file.id, "download_url": file.shared_link["download_url"]}


def put_file_item(ddb_table, file):
    if not is_box_object_public(file):
        raise ValueError("cannot put a file that hasn't been shared publicly")

    # this could cause concurrency issues in a scenario where lots of threads were operating on the ddb at once
    item = make_ddb_item(file)
    result = ddb_table.get_item(Key={"filepath": item["filepath"]})
    if result.get("Item") != item:
        ddb_table.put_item(Item=item)


def delete_file_item(ddb_table, file):
    ddb_table.delete_item(Key={"filepath": get_filepath(file)})


def get_manifest_item_by_box_file_id(ddb_table, box_file_id):
    response = ddb_table.query(
        IndexName="box_file_id-index",
        KeyConditionExpression=Key("box_file_id").eq(box_file_id),
    )
    items = response.get("Items", [])
    return items[0] if items else None


def get_download_url(ddb_table, filepath):
    result = ddb_table.get_item(Key={"filepath": filepath})
    if result.get("Item"):
        return result["Item"]["download_url"]
    else:
        return None


def get_file(client, box_file_id):
    return _get_box_resource(lambda: client.file(box_file_id).get())


def get_folder(client, box_folder_id):
    return _get_box_resource(lambda: client.folder(box_folder_id).get())


def _get_box_resource(callback):
    try:
        return callback()
    except BoxAPIException as e:
        if e.status == 404:
            return None
        else:
            raise e


def iterate_files(folder, shared=False):
    offset = 0
    while True:
        count = 0
        for item in folder.get_items(limit=GET_ITEMS_LIMIT, offset=offset, fields=GET_ITEMS_FIELDS):
            count += 1
            if item.object_type == "folder":
                folder_shared = shared or is_box_object_public(item)
                if not folder_shared:
                    try:
                        with_box_retry(item.create_shared_link, access="open", allow_download=True)
                        log_action("INFO", "sync", "fix_folder_sharing", folder_id=item.id)
                        folder_shared = True
                    except Exception as e:
                        log_action(
                            "WARNING",
                            "sync",
                            "fix_folder_sharing_failed",
                            folder_id=item.id,
                            error_type=type(e).__name__,
                        )
                yield from iterate_files(item, shared=folder_shared)
            elif item.object_type == "file":
                yield item, shared
        if count >= GET_ITEMS_LIMIT:
            offset += count
        else:
            # unclear why pytest reports this break is never tested...
            break


def repair_broken_url(filepath, manifest_item, manifest_table, caller="repair"):
    """Attempt to repair a broken Box download URL.

    Returns "repaired", "deleted", or "failed".
    """
    try:
        client = get_box_client()
        box_file_id = manifest_item["box_file_id"]
        file = with_box_retry(get_file, client, box_file_id)

        if file is None:
            manifest_table.delete_item(Key={"filepath": filepath})
            log_action("WARNING", caller, "file_not_found", filepath=filepath, box_file_id=box_file_id)
            return "deleted"

        file = with_box_retry(create_shared_link, client, file, access="open", allow_download=True)

        # Ensure ancestor folders are correctly shared (best-effort)
        try:
            ensure_folder_shared(client, file)
        except Exception as e:
            log_action("WARNING", caller, "folder_sharing_check_failed", filepath=filepath, error_type=type(e).__name__)

        item = make_ddb_item(file)
        item["last_validated"] = datetime.now(timezone.utc).isoformat()
        manifest_table.put_item(Item=item)

        log_action("INFO", caller, "repair_success", filepath=filepath, box_file_id=box_file_id)
        return "repaired"
    except Exception as e:
        log_action(
            "WARNING",
            caller,
            "repair_failed",
            filepath=filepath,
            box_file_id=manifest_item.get("box_file_id"),
            error_type=type(e).__name__,
        )
        return "failed"


def _get_secret():
    client = boto3.client("secretsmanager")
    try:
        response = client.get_secret_value(SecretId=SECRET_ARN)
    except client.exceptions.ClientError:  # pragma: no cover
        sts_client = boto3.client("sts")
        assumed_role_object = sts_client.assume_role(RoleArn=SECRET_ROLE_ARN, RoleSessionName="AssumeRoleSession1")

        credentials = assumed_role_object["Credentials"]

        client = boto3.client(
            "secretsmanager",
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
        )

        response = client.get_secret_value(SecretId=SECRET_ARN)

    if "SecretString" in response:
        secret = response["SecretString"]
        return json.loads(secret)
    else:
        raise NotImplementedError("Binary secret not implemented")
