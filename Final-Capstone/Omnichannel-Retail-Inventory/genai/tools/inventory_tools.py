import socket

import happybase


HBASE_HOST = "localhost"
HBASE_PORT = 9090
TABLE_NAME = "inventory_current"

# Keep HBase timeout short so the GenAI assistant does not hang.
HBASE_TIMEOUT_MS = 2000
SOCKET_TIMEOUT_SECONDS = 1.0


# Last successfully validated inventory snapshot.
# This is only a fallback for the demo if HBase is unavailable.
LOCAL_FALLBACK = {
    "S163#SKU03782": {
        "quantity_on_hand": 179,
        "reorder_point": 44,
        "max_stock_level": 194,
        "stock_status": "HEALTHY",
        "last_transaction_id": "FULL_DEMO_TX_005",
    }
}


def decode(value):
    if value is None:
        return None
    return value.decode("utf-8")


def thrift_port_available():
    """
    Quick TCP check before attempting HappyBase.
    Prevents long waits when Thrift is not listening.
    """
    try:
        with socket.create_connection(
            (HBASE_HOST, HBASE_PORT),
            timeout=SOCKET_TIMEOUT_SECONDS
        ):
            return True
    except OSError:
        return False


def get_hbase_connection():
    return happybase.Connection(
        host=HBASE_HOST,
        port=HBASE_PORT,
        timeout=HBASE_TIMEOUT_MS,
        transport="buffered",
        protocol="binary",
        compat="0.96",
    )


def get_local_fallback(store_id: str, sku: str) -> dict:
    row_key = f"{store_id}#{sku}"
    row = LOCAL_FALLBACK.get(row_key)

    if row is None:
        return {
            "found": False,
            "store_id": store_id,
            "sku": sku,
            "source": "local_fallback",
            "message": (
                "Live HBase is unavailable and no local fallback "
                "snapshot exists for this store/SKU."
            )
        }

    return {
        "found": True,
        "store_id": store_id,
        "sku": sku,
        "quantity_on_hand": row["quantity_on_hand"],
        "reorder_point": row["reorder_point"],
        "max_stock_level": row["max_stock_level"],
        "stock_status": row["stock_status"],
        "last_transaction_id": row["last_transaction_id"],
        "source": "local_fallback_snapshot",
        "warning": (
            "Live HBase was unavailable or slow. "
            "Returned the last validated inventory snapshot."
        )
    }


def get_stock(store_id: str, sku: str) -> dict:
    row_key = f"{store_id}#{sku}"

    # Fast check: if port 9090 is not available,
    # immediately use fallback.
    if not thrift_port_available():
        return get_local_fallback(store_id, sku)

    connection = None

    try:
        connection = get_hbase_connection()

        table = connection.table(TABLE_NAME)
        row = table.row(
            row_key,
            columns=[
                b"state:quantity_on_hand",
                b"state:reorder_point",
                b"state:max_stock_level",
                b"state:stock_status",
                b"meta:last_transaction_id",
            ]
        )

        if not row:
            return {
                "found": False,
                "store_id": store_id,
                "sku": sku,
                "source": "hbase",
                "message": "Inventory record not found"
            }

        return {
            "found": True,
            "store_id": store_id,
            "sku": sku,
            "quantity_on_hand": int(
                decode(row.get(b"state:quantity_on_hand")) or 0
            ),
            "reorder_point": int(
                decode(row.get(b"state:reorder_point")) or 0
            ),
            "max_stock_level": int(
                decode(row.get(b"state:max_stock_level")) or 0
            ),
            "stock_status": decode(
                row.get(b"state:stock_status")
            ),
            "last_transaction_id": decode(
                row.get(b"meta:last_transaction_id")
            ),
            "source": "hbase"
        }

    except Exception as exc:
        fallback = get_local_fallback(store_id, sku)

        fallback["hbase_error"] = (
            f"{type(exc).__name__}: {str(exc)}"
        )

        return fallback

    finally:
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass


if __name__ == "__main__":
    result = get_stock("S163", "SKU03782")
    print(result)
