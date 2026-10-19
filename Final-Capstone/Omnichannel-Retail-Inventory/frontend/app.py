import sys
from pathlib import Path

import streamlit as st

# ---------------------------------------------------------
# Project paths
# ---------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
GENAI_DIR = PROJECT_ROOT / "genai"

if str(GENAI_DIR) not in sys.path:
    sys.path.insert(0, str(GENAI_DIR))

from tools.inventory_tools import get_stock


# ---------------------------------------------------------
# Page configuration
# ---------------------------------------------------------
st.set_page_config(
    page_title="Omnichannel Retail Operations Portal",
    page_icon="🛒",
    layout="wide",
)


# ---------------------------------------------------------
# Simple professional styling
# ---------------------------------------------------------
st.markdown(
    """
    <style>
        .main-title {
            font-size: 34px;
            font-weight: 700;
            margin-bottom: 0px;
        }

        .subtitle {
            color: #666;
            margin-top: 0px;
            margin-bottom: 25px;
        }

        .status-healthy {
            background-color: #e8f5e9;
            padding: 12px;
            border-radius: 10px;
            font-weight: 600;
        }

        .status-low {
            background-color: #fff3cd;
            padding: 12px;
            border-radius: 10px;
            font-weight: 600;
        }

        .status-critical {
            background-color: #f8d7da;
            padding: 12px;
            border-radius: 10px;
            font-weight: 600;
        }
    </style>
    """,
    unsafe_allow_html=True,
)


# ---------------------------------------------------------
# Header
# ---------------------------------------------------------
st.markdown(
    '<div class="main-title">🛒 Omnichannel Retail Operations Portal</div>',
    unsafe_allow_html=True,
)

st.markdown(
    '<div class="subtitle">'
    'Real-Time Inventory • Forecasting • Replenishment • AI'
    '</div>',
    unsafe_allow_html=True,
)


# ---------------------------------------------------------
# Tabs
# ---------------------------------------------------------
overview_tab, inventory_tab, ai_tab = st.tabs(
    [
        "📊 Overview",
        "📦 Inventory Lookup",
        "🤖 AI Assistant",
    ]
)


# =========================================================
# TAB 1 - OVERVIEW
# =========================================================
with overview_tab:

    st.subheader("Retail Network Overview")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Stores", "201")

    with col2:
        st.metric("Products / SKUs", "5,000")

    with col3:
        st.metric("Inventory Records", "471,000")

    col4, col5, col6 = st.columns(3)

    with col4:
        st.metric("Replenishment Recommendations", "26,545")

    with col5:
        st.metric("Draft Purchase Orders", "14,398")

    with col6:
        st.metric("Forecast MAPE", "16.71%")

    st.divider()

    st.subheader("Real-Time Inventory Architecture")

    st.markdown(
        """
        ### POS → Kafka → Spark Streaming → HBase → Inventory / AI

        **Batch & Analytics Flow**

        MySQL / Supplier Files → HDFS → Spark → Silver → Gold  
        → Forecasting → Replenishment → Power BI

        **Operational Layer**

        Real-time POS events are processed through Kafka and Spark,
        while HBase maintains the latest store-SKU inventory state.
        """
    )

    st.success(
        "Stock synchronization acceptance test: 29.499 seconds — PASS"
    )


# =========================================================
# TAB 2 - INVENTORY LOOKUP
# =========================================================
with inventory_tab:

    st.subheader("Real-Time Inventory Lookup")

    st.write(
        "Retrieve the latest inventory state for a store and SKU."
    )

    input_col, result_col = st.columns([1, 2])

    with input_col:

        store_id = st.text_input(
            "Store ID",
            value="S163",
            key="inventory_store",
        )

        sku = st.text_input(
            "SKU",
            value="SKU03782",
            key="inventory_sku",
        )

        check_inventory = st.button(
            "🔍 Check Inventory",
            type="primary",
            use_container_width=True,
        )

    with result_col:

        if check_inventory:

            try:
                result = get_stock(
                    store_id.strip(),
                    sku.strip(),
                )

                if result.get("found"):

                    st.subheader("Inventory Status")

                    c1, c2, c3 = st.columns(3)

                    c1.metric(
                        "Quantity On Hand",
                        result.get("quantity_on_hand", "N/A"),
                    )

                    c2.metric(
                        "Reorder Point",
                        result.get("reorder_point", "N/A"),
                    )

                    c3.metric(
                        "Maximum Stock",
                        result.get("max_stock_level", "N/A"),
                    )

                    status = result.get(
                        "stock_status",
                        "UNKNOWN",
                    )

                    if status == "HEALTHY":
                        st.markdown(
                            f'<div class="status-healthy">'
                            f'Status: {status}</div>',
                            unsafe_allow_html=True,
                        )

                    elif status == "LOW":
                        st.markdown(
                            f'<div class="status-low">'
                            f'Status: {status}</div>',
                            unsafe_allow_html=True,
                        )

                    else:
                        st.markdown(
                            f'<div class="status-critical">'
                            f'Status: {status}</div>',
                            unsafe_allow_html=True,
                        )

                    st.write(
                        "**Last Transaction:**",
                        result.get(
                            "last_transaction_id",
                            "N/A",
                        ),
                    )

                    source = result.get(
                        "source",
                        "unknown",
                    )

                    st.caption(
                        f"Data source: {source}"
                    )

                    if result.get("warning"):
                        st.warning(
                            result["warning"]
                        )

                else:

                    st.warning(
                        result.get(
                            "message",
                            "Inventory record not found.",
                        )
                    )

            except Exception as exc:

                st.error(
                    f"Inventory lookup failed: {exc}"
                )

        else:

            st.info(
                "Enter a Store ID and SKU, then click "
                "'Check Inventory'."
            )


# =========================================================
# TAB 3 - AI ASSISTANT
# =========================================================
with ai_tab:

    st.subheader("AI Retail Inventory Assistant")

    st.write(
        "Ask inventory questions, create draft purchase orders, "
        "or ask questions about retail operations."
    )

    if "messages" not in st.session_state:
        st.session_state.messages = []

    if "previous_response_id" not in st.session_state:
        st.session_state.previous_response_id = None

    if st.button("🗑️ Clear Conversation"):

        st.session_state.messages = []
        st.session_state.previous_response_id = None
        st.rerun()

    for message in st.session_state.messages:

        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    user_question = st.chat_input(
        "Ask about inventory, replenishment, forecasting, or create a draft PO..."
    )

    if user_question:

        st.session_state.messages.append(
            {
                "role": "user",
                "content": user_question,
            }
        )

        with st.chat_message("user"):
            st.markdown(user_question)

        with st.chat_message("assistant"):

            with st.spinner("Thinking..."):

                try:
                    from agent import process_turn

                    response = process_turn(
                        user_question,
                        previous_response_id=(
                            st.session_state.previous_response_id
                        ),
                    )

                    assistant_text = response.output_text

                    st.session_state.previous_response_id = (
                        response.id
                    )

                    st.markdown(assistant_text)

                    st.session_state.messages.append(
                        {
                            "role": "assistant",
                            "content": assistant_text,
                        }
                    )

                except Exception as exc:

                    st.error(
                        "AI Assistant error: "
                        f"{str(exc)}"
                    )
