from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import Response
from contextlib import asynccontextmanager

from database import get_connection

import requests
import uuid
import threading
import time
import hashlib
import shutil
from datetime import datetime


# ============================================================
# CONFIGURATION
# ============================================================

CHUNK_SIZE = 1024 * 1024  # 1 MB

STORAGE_NODES = [
    "http://127.0.0.1:8001",
    "http://127.0.0.1:8002",
    "http://127.0.0.1:8003"
]

REPLICATION_FACTOR = 2

HEARTBEAT_INTERVAL = 5
HEARTBEAT_TIMEOUT = 2
INTEGRITY_CHECK_INTERVAL = 30

# Level 3 reliability settings
REQUEST_RETRIES = 3
RETRY_DELAY = 0.5
MIN_FREE_SPACE_BUFFER = 50 * 1024 * 1024  # 50 MB safety buffer


# ============================================================
# NODE STATUS
# ============================================================

NODE_STATUS = {
    node: {
        "status": "UNKNOWN",
        "last_heartbeat": None,
        "storage_directory": None,
        "total_space": None,
        "used_space": None,
        "free_space": None
    }
    for node in STORAGE_NODES
}

NODE_STATUS_LOCK = threading.Lock()


# ============================================================
# RECOVERY CONTROL
# ============================================================

RECOVERY_LOCK = threading.Lock()

RECOVERIES_IN_PROGRESS = set()

HEARTBEAT_STOP_EVENT = threading.Event()


# ============================================================
# GET HEALTHY NODES
# ============================================================

def get_healthy_nodes(required_space=0):
    """Return UP nodes with enough reported free disk space."""

    minimum_free = required_space + MIN_FREE_SPACE_BUFFER

    with NODE_STATUS_LOCK:
        healthy_nodes = []

        for node in STORAGE_NODES:
            status = NODE_STATUS[node]

            if status["status"] != "UP":
                continue

            free_space = status.get("free_space")

            # If an older node does not expose capacity information,
            # keep it eligible so the rest of the DFS remains usable.
            if free_space is not None and free_space < minimum_free:
                print(
                    f"[CAPACITY] Skipping {node}: "
                    f"{free_space} bytes free, need {minimum_free}."
                )
                continue

            healthy_nodes.append(node)

    return healthy_nodes


def request_with_retry(method, url, retries=REQUEST_RETRIES, **kwargs):
    """Retry temporary network/5xx failures a few times."""

    last_error = None

    for attempt in range(1, retries + 1):
        try:
            response = requests.request(method, url, **kwargs)

            if response.status_code >= 500 and attempt < retries:
                print(
                    f"[RETRY] {method} {url} returned "
                    f"HTTP {response.status_code}; "
                    f"retry {attempt + 1}/{retries}"
                )
                time.sleep(RETRY_DELAY * attempt)
                continue

            return response

        except requests.RequestException as exc:
            last_error = exc

            if attempt >= retries:
                raise

            print(
                f"[RETRY] {method} {url} failed: {exc}; "
                f"retry {attempt + 1}/{retries}"
            )
            time.sleep(RETRY_DELAY * attempt)

    raise last_error or requests.RequestException(
        f"Request failed: {method} {url}"
    )


def cleanup_uploaded_chunks(stored_replicas):
    """Remove already-stored chunks after a failed upload."""

    for node_url, chunk_id in reversed(stored_replicas):
        try:
            response = request_with_retry(
                "DELETE",
                f"{node_url}/chunk/{chunk_id}",
                timeout=15
            )

            if response.status_code == 200:
                print(
                    f"[ROLLBACK] Removed {chunk_id} from {node_url}"
                )
            else:
                print(
                    f"[ROLLBACK] Cleanup returned HTTP "
                    f"{response.status_code} for {chunk_id} on {node_url}"
                )
        except requests.RequestException as exc:
            print(
                f"[ROLLBACK] Could not remove {chunk_id} "
                f"from {node_url}: {exc}"
            )


def calculate_checksum(data: bytes) -> str:
    """Return SHA-256 checksum for a bytes object."""
    return hashlib.sha256(data).hexdigest()


def repair_corrupted_replica(
    chunk_id: str,
    bad_node: str,
    chunk_data: bytes,
    expected_checksum: str
) -> bool:
    """
    Repair a live replica whose contents failed checksum
    verification by overwriting it with a verified copy.
    """

    # Never attempt to repair a node that is currently down.
    # Normal node-failure re-replication handles that case.
    with NODE_STATUS_LOCK:
        node_status = NODE_STATUS.get(
            bad_node,
            {}
        ).get("status")

    if node_status != "UP":
        print(
            f"Skipping repair of {bad_node} for {chunk_id}: "
            f"node status is {node_status}."
        )
        return False

    # Verify the source data one more time before sending it.
    actual_checksum = calculate_checksum(chunk_data)

    if expected_checksum and actual_checksum != expected_checksum:
        print(
            f"REFUSING REPAIR for {chunk_id}: source checksum "
            f"does not match expected checksum. "
            f"Expected {expected_checksum}, got {actual_checksum}."
        )
        return False

    try:
        response = request_with_retry("POST", 
            f"{bad_node}/store/{chunk_id}",
            files={
                "file": (
                    chunk_id,
                    chunk_data,
                    "application/octet-stream"
                )
            },
            timeout=30
        )

        response.raise_for_status()

        # Read the repaired replica back and verify it.
        verify_response = request_with_retry("GET", 
            f"{bad_node}/chunk/{chunk_id}/download",
            timeout=30
        )

        verify_response.raise_for_status()

        repaired_checksum = calculate_checksum(
            verify_response.content
        )

        if expected_checksum and repaired_checksum != expected_checksum:
            print(
                f"REPAIR VERIFICATION FAILED for {chunk_id} "
                f"on {bad_node}. "
                f"Expected {expected_checksum}, "
                f"got {repaired_checksum}."
            )
            return False

        print(
            f"CORRUPTED REPLICA REPAIRED: {chunk_id} "
            f"on {bad_node}"
        )
        return True

    except requests.RequestException as e:
        print(
            f"Failed to repair {chunk_id} on {bad_node}: {e}"
        )
        return False



# ============================================================
# PERIODIC DATA INTEGRITY MONITOR
# ============================================================

def check_data_integrity():
    """
    Periodically verify every known replica that has checksum
    metadata.

    For each replica:
    1. Skip nodes that are currently DOWN.
    2. Download the stored chunk.
    3. Calculate SHA-256.
    4. Compare it with the checksum stored in PostgreSQL.
    5. If corrupted, find another healthy verified replica.
    6. Repair the corrupted replica automatically.
    """

    while not HEARTBEAT_STOP_EVENT.is_set():

        conn = None
        cursor = None

        try:
            conn = get_connection()
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT
                    c.chunk_id,
                    c.checksum,
                    r.node_url
                FROM chunks c
                INNER JOIN chunk_replicas r
                    ON c.chunk_id = r.chunk_id
                WHERE c.checksum IS NOT NULL
                ORDER BY c.file_id, c.chunk_index, r.node_url
                """
            )

            rows = cursor.fetchall()

            print()
            print(
                f"[INTEGRITY] Checking {len(rows)} replicas..."
            )

            for chunk_id, expected_checksum, node_url in rows:

                # ------------------------------------------------
                # Only check nodes currently UP
                # ------------------------------------------------

                with NODE_STATUS_LOCK:
                    status = NODE_STATUS.get(
                        node_url,
                        {}
                    ).get("status")

                if status != "UP":
                    print(
                        f"[INTEGRITY] Skipping {chunk_id} "
                        f"on {node_url}: node status is {status}"
                    )
                    continue

                # ------------------------------------------------
                # Read replica
                # ------------------------------------------------

                try:
                    response = request_with_retry("GET", 
                        f"{node_url}/chunk/"
                        f"{chunk_id}/download",
                        timeout=20
                    )

                    response.raise_for_status()
                    chunk_data = response.content

                except requests.RequestException as e:
                    print(
                        f"[INTEGRITY] Could not read "
                        f"{chunk_id} from {node_url}: {e}"
                    )
                    continue

                # ------------------------------------------------
                # Verify checksum
                # ------------------------------------------------

                actual_checksum = calculate_checksum(chunk_data)

                if actual_checksum == expected_checksum:
                    print(
                        f"[INTEGRITY] OK: {chunk_id} "
                        f"on {node_url}"
                    )
                    continue

                # ------------------------------------------------
                # Corruption found
                # ------------------------------------------------

                print()
                print(
                    "[INTEGRITY] CORRUPTION DETECTED"
                )
                print(
                    f"Chunk    : {chunk_id}"
                )
                print(
                    f"Node     : {node_url}"
                )
                print(
                    f"Expected : {expected_checksum}"
                )
                print(
                    f"Actual   : {actual_checksum}"
                )

                # ------------------------------------------------
                # Find another healthy source node.  IMPORTANT:
                # the primary node is stored in `chunks.node_url`,
                # while additional replicas are stored in
                # `chunk_replicas`.  We must consider BOTH.
                # ------------------------------------------------

                source_node = None
                source_data = None

                source_conn = None
                source_cursor = None

                try:
                    source_conn = get_connection()
                    source_cursor = source_conn.cursor()

                    source_cursor.execute(
                        """
                        SELECT node_url
                        FROM (
                            SELECT node_url
                            FROM chunks
                            WHERE chunk_id = %s

                            UNION

                            SELECT node_url
                            FROM chunk_replicas
                            WHERE chunk_id = %s
                        ) AS candidates
                        WHERE node_url <> %s
                        ORDER BY node_url
                        """,
                        (
                            chunk_id,
                            chunk_id,
                            node_url
                        )
                    )

                    source_rows = source_cursor.fetchall()

                finally:
                    if source_cursor:
                        source_cursor.close()
                    if source_conn:
                        source_conn.close()

                for (candidate_node,) in source_rows:

                    with NODE_STATUS_LOCK:
                        candidate_status = NODE_STATUS.get(
                            candidate_node,
                            {}
                        ).get("status")

                    if candidate_status != "UP":
                        continue

                    try:
                        source_response = request_with_retry("GET", 
                            f"{candidate_node}/chunk/"
                            f"{chunk_id}/download",
                            timeout=30
                        )

                        source_response.raise_for_status()
                        candidate_data = source_response.content

                        candidate_checksum = calculate_checksum(
                            candidate_data
                        )

                        if candidate_checksum != expected_checksum:
                            print(
                                f"[INTEGRITY] Source replica "
                                f"{candidate_node} is also corrupted "
                                f"for {chunk_id}. Skipping source."
                            )
                            continue

                        source_node = candidate_node
                        source_data = candidate_data

                        print(
                            f"[INTEGRITY] Verified source "
                            f"{chunk_id} from {candidate_node}"
                        )

                        break

                    except requests.RequestException as e:
                        print(
                            f"[INTEGRITY] Could not read source "
                            f"{chunk_id} from {candidate_node}: {e}"
                        )

                if source_node is None or source_data is None:
                    print(
                        f"[INTEGRITY] No healthy verified source "
                        f"available to repair {chunk_id} on {node_url}"
                    )
                    continue

                # ------------------------------------------------
                # Repair corrupted replica
                # ------------------------------------------------

                repaired = repair_corrupted_replica(
                    chunk_id=chunk_id,
                    bad_node=node_url,
                    chunk_data=source_data,
                    expected_checksum=expected_checksum
                )

                if repaired:
                    print(
                        f"[INTEGRITY] Repair successful for "
                        f"{chunk_id} on {node_url}"
                    )
                else:
                    print(
                        f"[INTEGRITY] Repair failed for "
                        f"{chunk_id} on {node_url}"
                    )

        except Exception as e:
            print(
                f"[INTEGRITY] Monitor error: {e}"
            )

        finally:
            if cursor:
                cursor.close()

            if conn:
                conn.close()

        # Wait using the same stop event used by heartbeat so shutdown
        # interrupts the wait immediately.
        HEARTBEAT_STOP_EVENT.wait(
            INTEGRITY_CHECK_INTERVAL
        )


# ============================================================
# RE-REPLICATION
# ============================================================

def recover_failed_node(failed_node):
    """
    Restore replication for chunks that lost a replica
    because a storage node failed.
    """

    # --------------------------------------------------------
    # Prevent duplicate recovery jobs
    # --------------------------------------------------------

    with RECOVERY_LOCK:

        if failed_node in RECOVERIES_IN_PROGRESS:

            print(
                f"Recovery already running for {failed_node}"
            )

            return

        RECOVERIES_IN_PROGRESS.add(failed_node)

    conn = None
    cursor = None

    try:

        print()
        print("=" * 60)
        print(
            f"STARTING RE-REPLICATION FOR {failed_node}"
        )
        print("=" * 60)

        # ----------------------------------------------------
        # Get healthy nodes
        # ----------------------------------------------------

        healthy_nodes = get_healthy_nodes()

        print(
            f"Healthy nodes available: {healthy_nodes}"
        )

        if not healthy_nodes:

            print(
                "No healthy nodes available."
            )

            return

        # ----------------------------------------------------
        # Database connection
        # ----------------------------------------------------

        conn = get_connection()
        cursor = conn.cursor()

        # ----------------------------------------------------
        # Find chunks that had failed node
        # ----------------------------------------------------

        cursor.execute(
            """
            SELECT DISTINCT
                c.chunk_id,
                c.node_url,
                c.checksum
            FROM chunks c
            JOIN chunk_replicas r
                ON c.chunk_id = r.chunk_id
            WHERE r.node_url = %s
            ORDER BY c.chunk_id
            """,
            (failed_node,)
        )

        affected_chunks = cursor.fetchall()

        print(
            f"Found {len(affected_chunks)} affected chunks."
        )

        # ----------------------------------------------------
        # Process each affected chunk
        # ----------------------------------------------------

        for chunk_id, primary_node, expected_checksum in affected_chunks:

            print()
            print(
                f"Processing chunk: {chunk_id}"
            )

            # ------------------------------------------------
            # Find all current replica nodes
            # ------------------------------------------------

            cursor.execute(
                """
                SELECT node_url
                FROM chunk_replicas
                WHERE chunk_id = %s
                """,
                (chunk_id,)
            )

            replica_rows = cursor.fetchall()

            replica_nodes = [
                row[0]
                for row in replica_rows
            ]

            print(
                f"Current replicas: {replica_nodes}"
            )

            # ------------------------------------------------
            # Find healthy surviving replicas
            # ------------------------------------------------

            healthy_replicas = []

            for node_url in replica_nodes:

                if node_url == failed_node:
                    continue

                with NODE_STATUS_LOCK:

                    node_status = NODE_STATUS.get(
                        node_url,
                        {}
                    ).get(
                        "status"
                    )

                if node_status == "UP":

                    healthy_replicas.append(
                        node_url
                    )

            print(
                f"Healthy surviving replicas: "
                f"{healthy_replicas}"
            )

            # ------------------------------------------------
            # Remove failed node from replica metadata
            #
            # The node is currently DOWN, so keeping it in
            # replica metadata would count a dead copy.
            # ------------------------------------------------

            cursor.execute(
                """
                DELETE FROM chunk_replicas
                WHERE chunk_id = %s
                AND node_url = %s
                """,
                (
                    chunk_id,
                    failed_node
                )
            )

            # ------------------------------------------------
            # If we have no surviving copy, recovery is
            # impossible.
            # ------------------------------------------------

            if not healthy_replicas:

                print(
                    f"WARNING: No surviving replica for "
                    f"{chunk_id}"
                )

                continue

            # ------------------------------------------------
            # How many additional replicas do we need?
            # ------------------------------------------------

            replicas_needed = (
                REPLICATION_FACTOR
                - len(healthy_replicas)
            )

            if replicas_needed <= 0:

                print(
                    f"{chunk_id}: replication already healthy."
                )

                continue

            # ------------------------------------------------
            # Candidate nodes:
            # healthy nodes that don't already contain
            # this chunk
            # ------------------------------------------------

            candidate_nodes = [
                node
                for node in healthy_nodes
                if node not in replica_nodes
                and node != failed_node
            ]

            print(
                f"Candidate recovery nodes: "
                f"{candidate_nodes}"
            )

            if not candidate_nodes:

                print(
                    f"WARNING: No candidate node available "
                    f"for {chunk_id}"
                )

                continue

            # ------------------------------------------------
            # Select a healthy source replica and verify its
            # checksum before using it for recovery.
            # ------------------------------------------------

            source_node = None
            chunk_data = None

            for candidate_source in healthy_replicas:

                try:

                    response = request_with_retry("GET", 
                        f"{candidate_source}/chunk/"
                        f"{chunk_id}/download",
                        timeout=30
                    )

                    response.raise_for_status()

                    candidate_data = response.content

                    if expected_checksum:

                        actual_checksum = calculate_checksum(candidate_data)

                        if actual_checksum != expected_checksum:
                            print(
                                f"CHECKSUM MISMATCH for {chunk_id} "
                                f"from {candidate_source}. "
                                f"Expected {expected_checksum}, "
                                f"got {actual_checksum}."
                            )
                            continue

                    source_node = candidate_source
                    chunk_data = candidate_data

                    print(
                        f"Downloaded verified {chunk_id} "
                        f"from {candidate_source}"
                    )
                    break

                except requests.RequestException as e:

                    print(
                        f"Could not download {chunk_id} "
                        f"from {candidate_source}: {e}"
                    )

            if source_node is None:

                print(
                    f"ERROR: No healthy verified source available "
                    f"for {chunk_id}."
                )

                continue

            # ------------------------------------------------
            # Create new replica(s)
            # ------------------------------------------------

            successful_replications = 0

            for target_node in candidate_nodes:

                if (
                    successful_replications
                    >= replicas_needed
                ):
                    break

                try:

                    response = request_with_retry("POST", 
                        f"{target_node}/store/"
                        f"{chunk_id}",
                        files={
                            "file": (
                                chunk_id,
                                chunk_data,
                                "application/octet-stream"
                            )
                        },
                        timeout=30
                    )

                    response.raise_for_status()

                    print(
                        f"Re-replicated {chunk_id}"
                    )

                    print(
                        f"Source : {source_node}"
                    )

                    print(
                        f"Target : {target_node}"
                    )

                    # ----------------------------------------
                    # Save new replica metadata
                    # ----------------------------------------

                    cursor.execute(
                        """
                        INSERT INTO chunk_replicas
                        (chunk_id, node_url)
                        VALUES (%s, %s)
                        ON CONFLICT
                        (chunk_id, node_url)
                        DO NOTHING
                        """,
                        (
                            chunk_id,
                            target_node
                        )
                    )

                    successful_replications += 1

                    # ----------------------------------------
                    # If failed node was primary,
                    # promote the new node
                    # ----------------------------------------

                    if primary_node == failed_node:

                        cursor.execute(
                            """
                            UPDATE chunks
                            SET node_url = %s
                            WHERE chunk_id = %s
                            """,
                            (
                                target_node,
                                chunk_id
                            )
                        )

                        primary_node = target_node

                        print(
                            f"New primary for {chunk_id}: "
                            f"{target_node}"
                        )

                except requests.RequestException as e:

                    print(
                        f"Failed to re-replicate "
                        f"{chunk_id} to {target_node}"
                    )

                    print(
                        f"Reason: {e}"
                    )

            # ------------------------------------------------
            # Report replication state
            # ------------------------------------------------

            remaining_replication = (
                len(healthy_replicas)
                + successful_replications
            )

            print(
                f"{chunk_id}: "
                f"{remaining_replication}/"
                f"{REPLICATION_FACTOR} healthy replicas"
            )

        # ----------------------------------------------------
        # Commit database changes
        # ----------------------------------------------------

        conn.commit()

        print()
        print(
            f"RE-REPLICATION COMPLETED FOR "
            f"{failed_node}"
        )

        print("=" * 60)
        print()

    except Exception as e:

        if conn:

            conn.rollback()

        print()
        print(
            f"RE-REPLICATION ERROR "
            f"FOR {failed_node}"
        )

        print(
            f"Reason: {e}"
        )

    finally:

        if cursor:

            cursor.close()

        if conn:

            conn.close()

        with RECOVERY_LOCK:

            RECOVERIES_IN_PROGRESS.discard(
                failed_node
            )



# ============================================================
# NODE REJOIN / REBALANCING
# ============================================================

def rebalance_rejoined_node(rejoined_node):
    """
    Rebalance replicas after a storage node changes
    from DOWN -> UP.

    The same round-robin placement strategy used during upload
    is used here to restore the desired replica distribution.
    """

    # Prevent duplicate recovery/rebalance jobs
    with RECOVERY_LOCK:

        if rejoined_node in RECOVERIES_IN_PROGRESS:
            print(
                f"Rebalance already running for {rejoined_node}"
            )
            return

        RECOVERIES_IN_PROGRESS.add(rejoined_node)

    conn = None
    cursor = None

    try:

        print()
        print("=" * 60)
        print(
            f"NODE REJOIN DETECTED: {rejoined_node}"
        )
        print(
            "STARTING REBALANCING"
        )
        print("=" * 60)

        # ----------------------------------------------------
        # Verify the node is still UP
        # ----------------------------------------------------

        with NODE_STATUS_LOCK:

            node_status = NODE_STATUS[
                rejoined_node
            ]["status"]

        if node_status != "UP":

            print(
                f"{rejoined_node} is no longer UP. "
                "Skipping rebalance."
            )

            return

        # ----------------------------------------------------
        # Get all currently healthy nodes
        # ----------------------------------------------------

        healthy_nodes = get_healthy_nodes()

        if len(healthy_nodes) < REPLICATION_FACTOR:

            print(
                "Not enough healthy nodes for rebalancing."
            )

            return

        # ----------------------------------------------------
        # Database
        # ----------------------------------------------------

        conn = get_connection()
        cursor = conn.cursor()

        # ----------------------------------------------------
        # Get every chunk
        # ----------------------------------------------------

        cursor.execute(
            """
            SELECT DISTINCT
               c.file_id,
               c.chunk_index,
               c.chunk_id,
               c.node_url,
               c.checksum
            FROM chunks c
            INNER JOIN chunk_replicas r
                ON c.chunk_id = r.chunk_id
            ORDER BY c.file_id, c.chunk_index
            """
        )

        chunk_rows = cursor.fetchall()

        print(
            f"Checking {len(chunk_rows)} chunks."
        )

        # ----------------------------------------------------
        # Process each chunk
        # ----------------------------------------------------

        for (
            file_id,
            chunk_index,
            chunk_id,
            primary_node,
            expected_checksum
        ) in chunk_rows:

            print()
            print(
                f"Processing chunk {chunk_index}: {chunk_id}"
            )

            # -----------------------------------------------
            # Desired placement using the same round-robin
            # strategy as upload
            # -----------------------------------------------

            primary_index = (
                chunk_index % len(healthy_nodes)
            )

            desired_nodes = []

            for replica_number in range(
                REPLICATION_FACTOR
            ):

                node_index = (
                    primary_index + replica_number
                ) % len(healthy_nodes)

                node_url = healthy_nodes[node_index]

                if node_url not in desired_nodes:
                    desired_nodes.append(node_url)

            print(
                f"Desired replicas: {desired_nodes}"
            )

            # -----------------------------------------------
            # Current replica placement
            # -----------------------------------------------

            cursor.execute(
                """
                SELECT node_url
                FROM chunk_replicas
                WHERE chunk_id = %s
                """,
                (chunk_id,)
            )

            current_rows = cursor.fetchall()

            current_nodes = [
                row[0]
                for row in current_rows
            ]

            print(
                f"Current replicas: {current_nodes}"
            )

            # -----------------------------------------------
            # Find healthy current replicas
            # -----------------------------------------------

            healthy_current_nodes = []

            for node_url in current_nodes:

                with NODE_STATUS_LOCK:

                    status = NODE_STATUS.get(
                        node_url,
                        {}
                    ).get("status")

                if status == "UP":

                    healthy_current_nodes.append(
                        node_url
                    )

            # -----------------------------------------------
            # Find a healthy source copy
            # -----------------------------------------------

            source_node = None

            # Prefer a current healthy replica
            for node_url in healthy_current_nodes:

                if node_url != rejoined_node:
                    source_node = node_url
                    break

            if source_node is None:

                # The rejoined node may already contain the
                # chunk. It can be used only if it is a copy.
                if rejoined_node in healthy_current_nodes:
                    source_node = rejoined_node

            # -----------------------------------------------
            # Add missing desired replicas
            # -----------------------------------------------

            missing_nodes = [
                node
                for node in desired_nodes
                if node not in current_nodes
            ]

            print(
                f"Missing replicas: {missing_nodes}"
            )

            if missing_nodes:

                if source_node is None:

                    print(
                        f"No healthy source available "
                        f"for {chunk_id}. "
                        "Skipping."
                    )

                    continue

                verified_source = None
                chunk_data = None

                for candidate_source in healthy_current_nodes:

                    if candidate_source == rejoined_node:
                        continue

                    try:

                        response = request_with_retry("GET", 
                            f"{candidate_source}/chunk/"
                            f"{chunk_id}/download",
                            timeout=30
                        )

                        response.raise_for_status()

                        candidate_data = response.content

                        if expected_checksum:

                            actual_checksum = calculate_checksum(candidate_data)

                            if actual_checksum != expected_checksum:
                                print(
                                    f"CHECKSUM MISMATCH for {chunk_id} "
                                    f"from {candidate_source}. "
                                    f"Expected {expected_checksum}, "
                                    f"got {actual_checksum}."
                                )
                                continue

                        verified_source = candidate_source
                        chunk_data = candidate_data

                        print(
                            f"Downloaded verified {chunk_id} "
                            f"from {candidate_source}"
                        )
                        break

                    except requests.RequestException as e:

                        print(
                            f"Could not download {chunk_id} "
                            f"from {candidate_source}: {e}"
                        )

                if verified_source is None:

                    print(
                        f"No healthy verified source available "
                        f"for {chunk_id}. Skipping."
                    )

                    continue

                for target_node in missing_nodes:

                    # Never copy to a node that has gone down
                    with NODE_STATUS_LOCK:

                        target_is_up = (
                            NODE_STATUS.get(
                                target_node,
                                {}
                            ).get("status") == "UP"
                        )

                    if not target_is_up:

                        print(
                            f"Target {target_node} is no longer UP. "
                            f"Skipping {chunk_id}."
                        )

                        continue

                    try:

                        response = request_with_retry("POST", 
                            f"{target_node}/store/"
                            f"{chunk_id}",
                            files={
                                "file": (
                                    chunk_id,
                                    chunk_data,
                                    "application/octet-stream"
                                )
                            },
                            timeout=30
                        )

                        response.raise_for_status()

                        print(
                            f"Rejoined replica created:"
                        )
                        print(
                            f"{source_node} -> "
                            f"{target_node}"
                        )

                        cursor.execute(
                            """
                            INSERT INTO chunk_replicas
                            (
                                chunk_id,
                                node_url
                            )
                            VALUES (%s, %s)
                            ON CONFLICT
                            (
                                chunk_id,
                                node_url
                            )
                            DO NOTHING
                            """,
                            (
                                chunk_id,
                                target_node
                            )
                        )

                    except requests.RequestException as e:

                        print(
                            f"Failed to create replica "
                            f"on {target_node}: {e}"
                        )

            # -----------------------------------------------
            # Refresh replica list after additions
            # -----------------------------------------------

            cursor.execute(
                """
                SELECT node_url
                FROM chunk_replicas
                WHERE chunk_id = %s
                """,
                (chunk_id,)
            )

            updated_rows = cursor.fetchall()

            updated_nodes = [
                row[0]
                for row in updated_rows
            ]

            # -----------------------------------------------
            # Remove replicas that are not part of the
            # desired placement.
            #
            # We only remove extras if doing so leaves at
            # least REPLICATION_FACTOR copies.
            # -----------------------------------------------

            extra_nodes = [
                node
                for node in updated_nodes
                if node not in desired_nodes
            ]

            for extra_node in extra_nodes:

                if (
                    len(updated_nodes)
                    <= REPLICATION_FACTOR
                ):
                    break

                try:

                    response = request_with_retry("DELETE", 
                        f"{extra_node}/chunk/"
                        f"{chunk_id}",
                        timeout=30
                    )

                    if response.status_code == 200:

                        print(
                            f"Removed old replica:"
                        )

                        print(
                            f"{chunk_id} "
                            f"from "
                            f"{extra_node}"
                        )

                        cursor.execute(
                            """
                            DELETE FROM chunk_replicas
                            WHERE chunk_id = %s
                            AND node_url = %s
                            """,
                            (
                                chunk_id,
                                extra_node
                            )
                        )

                        updated_nodes.remove(
                            extra_node
                        )

                except requests.RequestException as e:

                    print(
                        f"Could not remove old replica "
                        f"from {extra_node}: {e}"
                    )

            # -----------------------------------------------
            # Update primary node to match desired placement
            # -----------------------------------------------

            if desired_nodes:

                desired_primary = desired_nodes[0]

                # Primary must be a healthy node
                with NODE_STATUS_LOCK:

                    primary_is_up = (
                        NODE_STATUS.get(
                            desired_primary,
                            {}
                        ).get("status") == "UP"
                    )

                if (
                    primary_is_up
                    and primary_node != desired_primary
                ):

                    cursor.execute(
                        """
                        UPDATE chunks
                        SET node_url = %s
                        WHERE chunk_id = %s
                        """,
                        (
                            desired_primary,
                            chunk_id
                        )
                    )

                    print(
                        f"New primary for {chunk_id}: "
                        f"{desired_primary}"
                    )

            # -----------------------------------------------
            # Final state report
            # -----------------------------------------------

            cursor.execute(
                """
                SELECT node_url
                FROM chunk_replicas
                WHERE chunk_id = %s
                ORDER BY node_url
                """,
                (chunk_id,)
            )

            final_rows = cursor.fetchall()

            final_nodes = [
                row[0]
                for row in final_rows
            ]

            print(
                f"Final replicas: {final_nodes}"
            )

        # ----------------------------------------------------
        # Commit
        # ----------------------------------------------------

        conn.commit()

        print()
        print(
            f"REBALANCING COMPLETED FOR "
            f"{rejoined_node}"
        )

        print("=" * 60)
        print()

    except Exception as e:

        if conn:
            conn.rollback()

        print()
        print(
            f"REBALANCING ERROR FOR "
            f"{rejoined_node}"
        )
        print(
            f"Reason: {e}"
        )

    finally:

        if cursor:
            cursor.close()

        if conn:
            conn.close()

        with RECOVERY_LOCK:

            RECOVERIES_IN_PROGRESS.discard(
                rejoined_node
            )



# ============================================================
# HEARTBEAT MONITOR
# ============================================================

def check_nodes():
    """
    Continuously monitor all storage nodes.

    Detects:
        UP/UNKNOWN -> DOWN
        DOWN -> UP
    """

    while not HEARTBEAT_STOP_EVENT.is_set():

        failed_nodes = []
        rejoined_nodes = []

        # ----------------------------------------------------
        # Check every node first
        # ----------------------------------------------------

        for node_url in STORAGE_NODES:

            try:

                response = request_with_retry(
                    "GET",
                    f"{node_url}/health",
                    retries=1,
                    timeout=HEARTBEAT_TIMEOUT
                )

                with NODE_STATUS_LOCK:

                    previous_status = (
                        NODE_STATUS[node_url]["status"]
                    )

                    if response.status_code == 200:

                        NODE_STATUS[node_url][
                            "status"
                        ] = "UP"

                        NODE_STATUS[node_url][
                            "last_heartbeat"
                        ] = datetime.now().isoformat()

                        try:
                            health_data = response.json()
                            storage_directory = health_data.get(
                                "storage_directory"
                            )

                            if storage_directory:
                                total_space, used_space, free_space = (
                                    shutil.disk_usage(storage_directory)
                                )

                                NODE_STATUS[node_url][
                                    "storage_directory"
                                ] = storage_directory
                                NODE_STATUS[node_url][
                                    "total_space"
                                ] = total_space
                                NODE_STATUS[node_url][
                                    "used_space"
                                ] = used_space
                                NODE_STATUS[node_url][
                                    "free_space"
                                ] = free_space
                        except (ValueError, OSError) as capacity_error:
                            print(
                                f"[CAPACITY] Could not inspect {node_url}: "
                                f"{capacity_error}"
                            )

                    else:

                        NODE_STATUS[node_url][
                            "status"
                        ] = "DOWN"

                    current_status = (
                        NODE_STATUS[node_url]["status"]
                    )

                    # ----------------------------------------
                    # Detect UP -> DOWN
                    # ----------------------------------------

                    if (
                        current_status == "DOWN"
                        and previous_status != "DOWN"
                    ):

                        failed_nodes.append(
                            node_url
                        )

                    # ----------------------------------------
                    # Detect DOWN -> UP
                    # ----------------------------------------

                    if (
                        current_status == "UP"
                        and previous_status == "DOWN"
                    ):

                        rejoined_nodes.append(
                            node_url
                        )

            except requests.RequestException:

                with NODE_STATUS_LOCK:

                    previous_status = (
                        NODE_STATUS[node_url]["status"]
                    )

                    NODE_STATUS[node_url][
                        "status"
                    ] = "DOWN"

                    if previous_status != "DOWN":

                        failed_nodes.append(
                            node_url
                        )

        # ----------------------------------------------------
        # Start failure recovery
        # ----------------------------------------------------

        for failed_node in failed_nodes:

            print()
            print(
                f"NODE FAILURE DETECTED: "
                f"{failed_node}"
            )

            recovery_thread = threading.Thread(
                target=recover_failed_node,
                args=(failed_node,),
                daemon=True
            )

            recovery_thread.start()

        # ----------------------------------------------------
        # Start node-rejoin rebalancing
        # ----------------------------------------------------

        for rejoined_node in rejoined_nodes:

            print()
            print(
                f"NODE REJOIN DETECTED: "
                f"{rejoined_node}"
            )

            rejoin_thread = threading.Thread(
                target=rebalance_rejoined_node,
                args=(rejoined_node,),
                daemon=True
            )

            rejoin_thread.start()

        # ----------------------------------------------------
        # Wait before next heartbeat cycle
        # ----------------------------------------------------

        HEARTBEAT_STOP_EVENT.wait(
            HEARTBEAT_INTERVAL
        )


# ============================================================
# FASTAPI LIFESPAN
# ============================================================


@asynccontextmanager
async def lifespan(app: FastAPI):

    print(
        "Starting DFS Master Server..."
    )

    HEARTBEAT_STOP_EVENT.clear()

    heartbeat_thread = threading.Thread(
        target=check_nodes,
        daemon=True
    )

    heartbeat_thread.start()

    integrity_thread = threading.Thread(
        target=check_data_integrity,
        daemon=True
    )

    integrity_thread.start()

    print(
        "Heartbeat monitor started."
    )
    print(
        "Integrity monitor started."
    )

    yield

    print(
        "Stopping heartbeat and integrity monitors..."
    )

    HEARTBEAT_STOP_EVENT.set()

    heartbeat_thread.join(
        timeout=3
    )

    integrity_thread.join(
        timeout=3
    )

    print(
        "DFS Master Server stopped."
    )


app = FastAPI(
    title="DFS Master Server",
    lifespan=lifespan
)


# ============================================================
# ROOT
# ============================================================

@app.get("/")
def root():

    return {
        "message": "DFS Master Server is running"
    }


# ============================================================
# MASTER HEALTH
# ============================================================

@app.get("/health")
def health():

    return {
        "status": "ok"
    }


# ============================================================
# NODE STATUS
# ============================================================

@app.get("/nodes")
def get_nodes():

    with NODE_STATUS_LOCK:

        return {
            node: status.copy()
            for node, status in NODE_STATUS.items()
        }


# ============================================================
# UPLOAD
# ============================================================

@app.post("/upload")
async def upload_file(
    file: UploadFile = File(...)
):

    # --------------------------------------------------------
    # Read file
    # --------------------------------------------------------

    content = await file.read()

    file_size = len(content)

    if file_size == 0:

        raise HTTPException(
            status_code=400,
            detail="Cannot upload an empty file"
        )

    # --------------------------------------------------------
    # Find healthy nodes
    # --------------------------------------------------------

    healthy_nodes = get_healthy_nodes()

    if len(healthy_nodes) < REPLICATION_FACTOR:

        raise HTTPException(
            status_code=503,
            detail=(
                "Not enough healthy storage nodes. "
                f"Required: {REPLICATION_FACTOR}, "
                f"Available: {len(healthy_nodes)}"
            )
        )

    # --------------------------------------------------------
    # Split into chunks
    # --------------------------------------------------------

    chunks = []

    for i in range(
        0,
        file_size,
        CHUNK_SIZE
    ):

        chunk_data = content[
            i:i + CHUNK_SIZE
        ]

        chunks.append(
            chunk_data
        )

    # --------------------------------------------------------
    # Database
    # --------------------------------------------------------

    conn = get_connection()
    cursor = conn.cursor()

    try:

        # ----------------------------------------------------
        # Save file metadata
        # ----------------------------------------------------

        cursor.execute(
            """
            INSERT INTO files
            (
                filename,
                file_size,
                total_chunks
            )
            VALUES (%s, %s, %s)
            RETURNING id
            """,
            (
                file.filename,
                file_size,
                len(chunks)
            )
        )

        file_id = cursor.fetchone()[0]

        chunk_checksums = []
        stored_replicas = []

        # ----------------------------------------------------
        # Process chunks
        # ----------------------------------------------------

        for index, chunk_data in enumerate(
            chunks
        ):

            chunk_id = (
                f"{file_id}_"
                f"{index}_"
                f"{uuid.uuid4().hex}"
            )

            chunk_checksum = calculate_checksum(chunk_data)

            chunk_checksums.append({
                "chunk_index": index,
                "chunk_id": chunk_id,
                "checksum": chunk_checksum
            })

            print(
                f"Chunk {index} checksum: {chunk_checksum}"
            )

            # -----------------------------------------------
            # Only use nodes with enough free space
            # -----------------------------------------------

            capacity_nodes = get_healthy_nodes(
                required_space=len(chunk_data)
            )

            if len(capacity_nodes) < REPLICATION_FACTOR:
                raise HTTPException(
                    status_code=507,
                    detail=(
                        f"Not enough storage capacity for chunk {index}. "
                        f"Need {REPLICATION_FACTOR} nodes with enough free space."
                    )
                )

            # -----------------------------------------------
            # Primary node selection
            # -----------------------------------------------

            primary_index = (
                index
                % len(capacity_nodes)
            )

            selected_nodes = []

            # -----------------------------------------------
            # Replica selection
            # -----------------------------------------------

            for replica_number in range(
                REPLICATION_FACTOR
            ):

                node_index = (
                    primary_index
                    + replica_number
                ) % len(capacity_nodes)

                node_url = (
                    capacity_nodes[node_index]
                )

                if (
                    node_url
                    not in selected_nodes
                ):

                    selected_nodes.append(
                        node_url
                    )

            print(
                f"Chunk {index} -> "
                f"{selected_nodes}"
            )

            # -----------------------------------------------
            # Store on every replica
            # -----------------------------------------------

            for node_url in selected_nodes:

                try:

                    response = request_with_retry("POST", 
                        f"{node_url}/store/"
                        f"{chunk_id}",
                        files={
                            "file": (
                                chunk_id,
                                chunk_data,
                                "application/octet-stream"
                            )
                        },
                        timeout=30
                    )

                    response.raise_for_status()

                    print(
                        f"Stored {chunk_id} "
                        f"on {node_url}"
                    )

                    stored_replicas.append(
                        (node_url, chunk_id)
                    )

                    with NODE_STATUS_LOCK:
                        if NODE_STATUS[node_url].get("free_space") is not None:
                            NODE_STATUS[node_url]["free_space"] = max(
                                0,
                                NODE_STATUS[node_url]["free_space"]
                                - len(chunk_data)
                            )

                except requests.RequestException as e:

                    print(
                        f"Failed to store "
                        f"{chunk_id} on "
                        f"{node_url}: {e}"
                    )

                    raise HTTPException(
                        status_code=503,
                        detail=(
                            f"Could not store "
                            f"chunk {chunk_id} "
                            f"on {node_url}"
                        )
                    )

                # -------------------------------------------
                # Replica metadata
                # -------------------------------------------

                cursor.execute(
                    """
                    INSERT INTO chunk_replicas
                    (
                        chunk_id,
                        node_url
                    )
                    VALUES (%s, %s)
                    ON CONFLICT
                    (
                        chunk_id,
                        node_url
                    )
                    DO NOTHING
                    """,
                    (
                        chunk_id,
                        node_url
                    )
                )

            # -----------------------------------------------
            # Primary chunk metadata
            # -----------------------------------------------

            primary_node = (
                selected_nodes[0]
            )

            cursor.execute(
                """
                INSERT INTO chunks
                (
                    file_id,
                    chunk_index,
                    chunk_id,
                    node_url,
                    chunk_size,
                    checksum
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    file_id,
                    index,
                    chunk_id,
                    primary_node,
                    len(chunk_data),
                    chunk_checksum
                )
            )

        # ----------------------------------------------------
        # Commit
        # ----------------------------------------------------

        conn.commit()

    except HTTPException:

        conn.rollback()
        cleanup_uploaded_chunks(stored_replicas)

        raise

    except Exception as e:

        conn.rollback()
        cleanup_uploaded_chunks(stored_replicas)

        print(
            "Upload failed:",
            e
        )

        raise HTTPException(
            status_code=500,
            detail=(
                f"Upload failed: {str(e)}"
            )
        )

    finally:

        cursor.close()
        conn.close()

    return {
        "message": "File uploaded successfully",
        "file_id": file_id,
        "filename": file.filename,
        "file_size": file_size,
        "total_chunks": len(chunks),
        "replication_factor": REPLICATION_FACTOR,
        "chunks": chunk_checksums
    }


# ============================================================
# DOWNLOAD
# ============================================================

@app.get("/download/{filename}")
def download_file(
    filename: str
):

    conn = get_connection()
    cursor = conn.cursor()

    try:

        # ----------------------------------------------------
        # Find latest file
        # ----------------------------------------------------

        cursor.execute(
            """
            SELECT id
            FROM files
            WHERE filename = %s
            ORDER BY id DESC
            LIMIT 1
            """,
            (filename,)
        )

        file_row = cursor.fetchone()

        if file_row is None:

            raise HTTPException(
                status_code=404,
                detail="File not found"
            )

        file_id = file_row[0]

        # ----------------------------------------------------
        # Find chunks in correct order
        # ----------------------------------------------------

        cursor.execute(
            """
            SELECT
                chunk_id,
                node_url,
                checksum
            FROM chunks
            WHERE file_id = %s
            ORDER BY chunk_index
            """,
            (file_id,)
        )

        chunk_rows = (
            cursor.fetchall()
        )

    finally:

        cursor.close()
        conn.close()

    if not chunk_rows:

        raise HTTPException(
            status_code=404,
            detail="No chunks found for this file"
        )

    # --------------------------------------------------------
    # Reconstruct file
    # --------------------------------------------------------

    file_data = bytearray()

    for (
        chunk_id,
        primary_node,
        expected_checksum
    ) in chunk_rows:

        # ----------------------------------------------------
        # Primary first
        # ----------------------------------------------------

        nodes_to_try = [
            primary_node
        ]

        # ----------------------------------------------------
        # Get replicas
        # ----------------------------------------------------

        conn = get_connection()
        cursor = conn.cursor()

        try:

            cursor.execute(
                """
                SELECT node_url
                FROM chunk_replicas
                WHERE chunk_id = %s
                AND node_url <> %s
                """,
                (
                    chunk_id,
                    primary_node
                )
            )

            replica_rows = (
                cursor.fetchall()
            )

            for (
                replica_url,
            ) in replica_rows:

                if (
                    replica_url
                    not in nodes_to_try
                ):

                    nodes_to_try.append(
                        replica_url
                    )

        finally:

            cursor.close()
            conn.close()

        # ----------------------------------------------------
        # Try each node and verify integrity
        # ----------------------------------------------------

        chunk_downloaded = False
        corrupted_nodes = []
        verified_source_node = None
        verified_chunk_data = None

        for node_url in nodes_to_try:

            try:

                response = request_with_retry("GET", 
                    f"{node_url}/chunk/"
                    f"{chunk_id}/download",
                    timeout=30
                )

                response.raise_for_status()

                chunk_data = response.content

                if expected_checksum:

                    actual_checksum = calculate_checksum(chunk_data)

                    if actual_checksum != expected_checksum:

                        print(
                            f"CHECKSUM MISMATCH: {chunk_id} "
                            f"from {node_url}. "
                            f"Expected {expected_checksum}, "
                            f"got {actual_checksum}."
                        )

                        corrupted_nodes.append(node_url)

                        # Try the next replica instead of using
                        # corrupted data.
                        continue

                    print(
                        f"Checksum verified: {chunk_id} "
                        f"from {node_url}"
                    )

                # Legacy chunks without checksum metadata are
                # still accepted for backward compatibility.
                file_data.extend(chunk_data)

                verified_source_node = node_url
                verified_chunk_data = chunk_data
                chunk_downloaded = True

                print(
                    f"Downloaded verified chunk "
                    f"{chunk_id} from {node_url}"
                )

                break

            except requests.RequestException as e:

                print(
                    f"Failed to get "
                    f"{chunk_id} "
                    f"from "
                    f"{node_url}: "
                    f"{e}"
                )

        if chunk_downloaded and expected_checksum and verified_chunk_data is not None:

            # Any live replica that failed checksum verification is
            # repaired from the verified source copy.
            for bad_node in corrupted_nodes:

                if bad_node == verified_source_node:
                    continue

                repair_corrupted_replica(
                    chunk_id=chunk_id,
                    bad_node=bad_node,
                    chunk_data=verified_chunk_data,
                    expected_checksum=expected_checksum
                )

        if not chunk_downloaded:

            raise HTTPException(
                status_code=503,
                detail=(
                    f"Chunk {chunk_id} "
                    f"is unavailable"
                )
            )

    return Response(
        content=bytes(file_data),
        media_type="application/octet-stream",
        headers={
            "Content-Disposition":
            f'attachment; filename="{filename}"'
        }
    )


# ============================================================
# LIST FILES
# ============================================================

@app.get("/files")
def list_files():

    conn = get_connection()
    cursor = conn.cursor()

    try:

        cursor.execute(
            """
            SELECT
                id,
                filename,
                file_size,
                total_chunks,
                created_at
            FROM files
            ORDER BY id
            """
        )

        rows = cursor.fetchall()

    finally:

        cursor.close()
        conn.close()

    return [
        {
            "file_id": row[0],
            "filename": row[1],
            "file_size": row[2],
            "total_chunks": row[3],
            "created_at": row[4]
        }
        for row in rows
    ]


# ============================================================
# DELETE FILE
# ============================================================

@app.delete("/file/{filename}")
def delete_file(
    filename: str
):

    conn = get_connection()
    cursor = conn.cursor()

    try:

        # ----------------------------------------------------
        # Find latest file
        # ----------------------------------------------------

        cursor.execute(
            """
            SELECT id
            FROM files
            WHERE filename = %s
            ORDER BY id DESC
            LIMIT 1
            """,
            (filename,)
        )

        file_row = cursor.fetchone()

        if file_row is None:

            raise HTTPException(
                status_code=404,
                detail="File not found"
            )

        file_id = file_row[0]

        # ----------------------------------------------------
        # Get chunks
        # ----------------------------------------------------

        cursor.execute(
            """
            SELECT chunk_id
            FROM chunks
            WHERE file_id = %s
            ORDER BY chunk_index
            """,
            (file_id,)
        )

        chunk_rows = (
            cursor.fetchall()
        )

        deleted_replicas = 0

        # ----------------------------------------------------
        # Delete physical replicas
        # ----------------------------------------------------

        for (
            chunk_id,
        ) in chunk_rows:

            cursor.execute(
                """
                SELECT node_url
                FROM chunk_replicas
                WHERE chunk_id = %s
                """,
                (chunk_id,)
            )

            replica_rows = (
                cursor.fetchall()
            )

            for (
                node_url,
            ) in replica_rows:

                try:

                    response = request_with_retry("DELETE", 
                        f"{node_url}/chunk/"
                        f"{chunk_id}",
                        timeout=30
                    )

                    if response.status_code == 200:

                        deleted_replicas += 1

                except requests.RequestException as e:

                    print(
                        f"Failed to delete "
                        f"{chunk_id} "
                        f"from "
                        f"{node_url}: "
                        f"{e}"
                    )

            # -----------------------------------------------
            # Delete replica metadata
            # -----------------------------------------------

            cursor.execute(
                """
                DELETE FROM chunk_replicas
                WHERE chunk_id = %s
                """,
                (chunk_id,)
            )

        # ----------------------------------------------------
        # Delete chunk metadata
        # ----------------------------------------------------

        cursor.execute(
            """
            DELETE FROM chunks
            WHERE file_id = %s
            """,
            (file_id,)
        )

        # ----------------------------------------------------
        # Delete file metadata
        # ----------------------------------------------------

        cursor.execute(
            """
            DELETE FROM files
            WHERE id = %s
            """,
            (file_id,)
        )

        conn.commit()

    except HTTPException:

        conn.rollback()

        raise

    except Exception as e:

        conn.rollback()

        raise HTTPException(
            status_code=500,
            detail=(
                f"Delete failed: {str(e)}"
            )
        )

    finally:

        cursor.close()
        conn.close()

    return {
        "message": "File deleted successfully",
        "filename": filename,
        "file_id": file_id,
        "deleted_replicas": deleted_replicas
    }