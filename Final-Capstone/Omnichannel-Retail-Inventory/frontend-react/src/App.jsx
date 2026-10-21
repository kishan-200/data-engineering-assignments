import { useMemo, useState } from "react";

import {
  Activity,
  Bot,
  Boxes,
  Database,
  PackageCheck,
  RefreshCw,
  Search,
  Send,
  ShoppingCart,
  Sparkles,
  Store,
  Truck,
  Warehouse,
} from "lucide-react";

import {
  ResponsiveContainer,
  AreaChart,
  Area,
  Line,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  BarChart,
  Bar,
} from "recharts";

import "./App.css";


const API_BASE =
  import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";


const demandTrend = [
  { day: "Oct 01", demand: 11200, forecast: 10900 },
  { day: "Oct 02", demand: 12100, forecast: 11800 },
  { day: "Oct 03", demand: 11600, forecast: 12000 },
  { day: "Oct 04", demand: 13400, forecast: 12900 },
  { day: "Oct 05", demand: 12800, forecast: 13200 },
  { day: "Oct 06", demand: 14300, forecast: 13900 },
  { day: "Oct 07", demand: 13700, forecast: 14100 },
  { day: "Oct 08", demand: 12600, forecast: 13100 },
  { day: "Oct 09", demand: 13200, forecast: 13000 },
  { day: "Oct 10", demand: 14500, forecast: 14000 },
  { day: "Oct 11", demand: 13900, forecast: 14200 },
  { day: "Oct 12", demand: 15100, forecast: 14700 },
];


const reorderByStore = [
  { store: "ECOM001", qty: 38920 },
  { store: "S128", qty: 28110 },
  { store: "S102", qty: 27940 },
  { store: "S050", qty: 27120 },
  { store: "S120", qty: 26600 },
  { store: "S085", qty: 26210 },
  { store: "S078", qty: 25780 },
  { store: "S013", qty: 25200 },
];


const replenishmentRows = [
  {
    store: "ECOM001",
    sku: "SKU01604",
    stock: 59,
    forecast: 4,
    projected: 55,
    reorder: 137,
    supplier: "SUP084",
    orderQty: 441,
  },
  {
    store: "ECOM001",
    sku: "SKU01819",
    stock: 35,
    forecast: 0,
    projected: 35,
    reorder: 118,
    supplier: "SUP062",
    orderQty: 421,
  },
  {
    store: "ECOM001",
    sku: "SKU03965",
    stock: 85,
    forecast: 0,
    projected: 85,
    reorder: 116,
    supplier: "SUP095",
    orderQty: 412,
  },
  {
    store: "ECOM001",
    sku: "SKU00310",
    stock: 28,
    forecast: 1,
    projected: 27,
    reorder: 96,
    supplier: "SUP045",
    orderQty: 403,
  },
  {
    store: "ECOM001",
    sku: "SKU01133",
    stock: 33,
    forecast: 0,
    projected: 33,
    reorder: 132,
    supplier: "SUP027",
    orderQty: 401,
  },
];


const suggestedQuestions = [
  "What is the stock of SKU03782 at store S163?",
  "What is the reorder point for SKU03782 at S163?",
  "Create a draft PO for 30 units of SKU03782 at S163.",
  "Explain the end-to-end retail data architecture.",
  "Which ML model is used and why?",
  "Summarize the key project KPIs and validation results.",
];


function App() {
  const [activeTab, setActiveTab] = useState("overview");

  const [storeId, setStoreId] = useState("S163");
  const [sku, setSku] = useState("SKU03782");

  const [inventoryResult, setInventoryResult] = useState(null);
  const [inventoryLoading, setInventoryLoading] = useState(false);

  const [chatInput, setChatInput] = useState("");
  const [chatLoading, setChatLoading] = useState(false);

  const [previousResponseId, setPreviousResponseId] = useState(null);

  const [messages, setMessages] = useState([
    {
      role: "assistant",
      text:
        "I can help with live inventory, replenishment, forecasting, KPIs, architecture and draft purchase orders.",
    },
  ]);


  const kpis = useMemo(
    () => [
      {
        title: "Stores",
        value: "201",
        subtitle: "200 physical + e-commerce",
        icon: Store,
      },
      {
        title: "Products / SKUs",
        value: "5,000",
        subtitle: "catalog coverage",
        icon: Boxes,
      },
      {
        title: "Inventory Records",
        value: "471K",
        subtitle: "store-SKU state",
        icon: Warehouse,
      },
      {
        title: "Replenishment",
        value: "26,545",
        subtitle: "Gold recommendations",
        icon: RefreshCw,
      },
      {
        title: "Draft POs",
        value: "14,398",
        subtitle: "approval pending",
        icon: ShoppingCart,
      },
      {
        title: "Forecast MAPE",
        value: "16.71%",
        subtitle: "Random Forest",
        icon: Activity,
      },
    ],
    []
  );


  async function lookupInventory() {
    if (!storeId.trim() || !sku.trim()) {
      return;
    }

    setInventoryLoading(true);

    try {
      const response = await fetch(
        `${API_BASE}/api/inventory/${storeId
          .trim()
          .toUpperCase()}/${sku.trim().toUpperCase()}`
      );

      if (!response.ok) {
        throw new Error("Inventory lookup failed");
      }

      const data = await response.json();

      setInventoryResult(data);
    } catch {
      setInventoryResult({
        found: false,
        message:
          "Inventory service is temporarily unavailable. Please try again.",
      });
    } finally {
      setInventoryLoading(false);
    }
  }


  function localFallbackAnswer(question) {
    const q = question.toLowerCase();

    if (q.includes("architecture")) {
      return (
        "The platform follows an end-to-end retail data flow. " +
        "MySQL, supplier files, APIs and POS events are ingested using Sqoop, Python and Kafka. " +
        "Spark handles batch ETL and Structured Streaming. HBase stores current store-SKU inventory. " +
        "Silver and Gold layers support analytics, forecasting and replenishment. " +
        "Airflow orchestrates workflows, AWS and Databricks provide cloud processing, " +
        "and Power BI plus GenAI serve business users."
      );
    }

    if (
      q.includes("ml model") ||
      q.includes("which model") ||
      q.includes("random forest")
    ) {
      return (
        "The formal machine-learning model is Spark ML RandomForestRegressor. " +
        "It was chosen because retail demand can contain nonlinear relationships between historical lag and rolling features. " +
        "Random Forest is more robust than a single decision tree, requires little feature scaling, " +
        "and integrates directly with Spark ML. Its validation MAPE is 16.71%, passing the target of less than 25%."
      );
    }

    if (q.includes("kpi")) {
      return (
        "Key validated results are: streaming latency 29.499 seconds, " +
        "forecast MAPE 16.71%, stock-out rate 0.0004%, " +
        "1,000 out of 1,000 POS retry events removed, " +
        "10 out of 10 Kafka messages retained after a broker failure, " +
        "and zero duplicates after Spark checkpoint restart."
      );
    }

    if (q.includes("replenishment")) {
      return (
        "The replenishment engine combines current inventory, forecast demand, reorder point, " +
        "maximum stock level and supplier mapping. " +
        "It generated 26,545 replenishment recommendations and 14,398 draft purchase orders."
      );
    }

    return (
      "The AI service is temporarily unavailable for that question. " +
      "Please try again."
    );
  }


  function extractInventoryIdentifiers(question) {
    const skuMatch = question.match(/\bSKU\d+\b/i);

    const storeMatch = question.match(
      /\b(?:S\d{3}|ECOM\d{3})\b/i
    );

    if (!skuMatch || !storeMatch) {
      return null;
    }

    return {
      sku: skuMatch[0].toUpperCase(),
      store: storeMatch[0].toUpperCase(),
    };
  }


  async function answerInventoryQuestion(question) {
    const identifiers =
      extractInventoryIdentifiers(question);

    if (!identifiers) {
      return null;
    }

    const lower = question.toLowerCase();

    const inventoryKeywords = [
      "stock",
      "inventory",
      "reorder",
      "maximum",
      "max stock",
      "status",
      "healthy",
      "last transaction",
      "transaction",
    ];

    const isInventoryQuestion =
      inventoryKeywords.some((keyword) =>
        lower.includes(keyword)
      );

    if (!isInventoryQuestion) {
      return null;
    }

    try {
      const response = await fetch(
        `${API_BASE}/api/inventory/${identifiers.store}/${identifiers.sku}`
      );

      if (!response.ok) {
        throw new Error("Inventory API failed");
      }

      const data = await response.json();

      if (!data.found) {
        return (
          `No inventory record was found for ` +
          `${identifiers.sku} at ${identifiers.store}.`
        );
      }

      if (lower.includes("reorder")) {
        return (
          `The reorder point for ${identifiers.sku} ` +
          `at store ${identifiers.store} is ` +
          `${data.reorder_point} units.`
        );
      }

      if (
        lower.includes("maximum") ||
        lower.includes("max stock")
      ) {
        return (
          `The maximum stock level for ${identifiers.sku} ` +
          `at store ${identifiers.store} is ` +
          `${data.max_stock_level} units.`
        );
      }

      if (
        lower.includes("status") ||
        lower.includes("healthy")
      ) {
        return (
          `${identifiers.sku} at store ${identifiers.store} ` +
          `currently has stock status ${data.stock_status}. ` +
          `Quantity on hand is ${data.quantity_on_hand} units.`
        );
      }

      if (
        lower.includes("last transaction") ||
        lower.includes("transaction")
      ) {
        return (
          `The last processed transaction for ` +
          `${identifiers.sku} at store ${identifiers.store} ` +
          `is ${data.last_transaction_id || "not available"}.`
        );
      }

      return (
        `Store ${identifiers.store} currently has ` +
        `${data.quantity_on_hand} units of ` +
        `${identifiers.sku} in stock. ` +
        `Status: ${data.stock_status}.`
      );
    } catch {
      return (
        "Inventory information is temporarily unavailable. " +
        "Please try again."
      );
    }
  }


  async function askAssistant(question) {
    const cleaned = question.trim();

    if (!cleaned || chatLoading) {
      return;
    }

    setMessages((prev) => [
      ...prev,
      {
        role: "user",
        text: cleaned,
      },
    ]);

    setChatInput("");
    setChatLoading(true);

    try {
      /*
       * Inventory-specific questions are answered directly
       * from the real inventory API so fields such as
       * reorder_point and max_stock_level are always used.
       */
      const inventoryAnswer =
        await answerInventoryQuestion(cleaned);

      if (inventoryAnswer) {
        setMessages((prev) => [
          ...prev,
          {
            role: "assistant",
            text: inventoryAnswer,
          },
        ]);

        return;
      }


      /*
       * Other questions go through the real OpenAI agent.
       */
      const response = await fetch(
        `${API_BASE}/api/chat`,
        {
          method: "POST",

          headers: {
            "Content-Type": "application/json",
          },

          body: JSON.stringify({
            message: cleaned,
            previous_response_id:
              previousResponseId,
          }),
        }
      );

      if (!response.ok) {
        throw new Error(
          "Chat backend unavailable"
        );
      }

      const data = await response.json();

      if (data.response_id) {
        setPreviousResponseId(
          data.response_id
        );
      }

      setMessages((prev) => [
        ...prev,
        {
          role: "assistant",
          text:
            data.answer ||
            "No response was returned.",
        },
      ]);
    } catch {
      const answer =
        localFallbackAnswer(cleaned);

      setMessages((prev) => [
        ...prev,
        {
          role: "assistant",
          text: answer,
        },
      ]);
    } finally {
      setChatLoading(false);
    }
  }


  return (
    <div className="app-shell">

      {/* ===================================================
          HEADER
      =================================================== */}

      <header className="topbar">

        <div className="brand-section">

          <div className="brand-icon">
            <ShoppingCart size={25} />
          </div>

          <div>
            <h1>
              OmniStock Operations
            </h1>

            <p>
              Real-Time Omnichannel Inventory &
              Auto-Replenishment
            </p>
          </div>

        </div>


        <div className="system-section">

          <span className="header-time">
            HBase • Kafka • Spark • GenAI
          </span>

        </div>

      </header>


      {/* ===================================================
          WORKSPACE
      =================================================== */}

      <section className="workspace">

        <main className="dashboard-area">


          {/* ===============================================
              KPI CARDS
          =============================================== */}

          <section className="kpi-grid">

            {kpis.map((item) => {
              const Icon = item.icon;

              return (
                <div
                  className="kpi-card"
                  key={item.title}
                >

                  <div className="kpi-top">

                    <span>
                      {item.title}
                    </span>

                    <div className="kpi-icon">
                      <Icon size={18} />
                    </div>

                  </div>


                  <div className="kpi-value">
                    {item.value}
                  </div>


                  <div className="kpi-subtitle">
                    {item.subtitle}
                  </div>

                </div>
              );
            })}

          </section>


          {/* ===============================================
              NAVIGATION
          =============================================== */}

          <nav className="tab-nav">

            <button
              className={
                activeTab === "overview"
                  ? "active"
                  : ""
              }
              onClick={() =>
                setActiveTab("overview")
              }
            >
              📊 Overview
            </button>


            <button
              className={
                activeTab === "inventory"
                  ? "active"
                  : ""
              }
              onClick={() =>
                setActiveTab("inventory")
              }
            >
              📦 Inventory
            </button>


            <button
              className={
                activeTab === "forecast"
                  ? "active"
                  : ""
              }
              onClick={() =>
                setActiveTab("forecast")
              }
            >
              📈 Forecast & Replenishment
            </button>


            <button
              className={
                activeTab === "validation"
                  ? "active"
                  : ""
              }
              onClick={() =>
                setActiveTab("validation")
              }
            >
              🧪 Validation
            </button>

          </nav>


          {/* ===============================================
              TAB CONTENT
          =============================================== */}

          <section className="tab-content">


            {/* =============================================
                OVERVIEW
            ============================================= */}

            {activeTab === "overview" && (
              <>

                <div className="section-header">

                  <div>

                    <h2>
                      Retail Operations Overview
                    </h2>

                    <p>
                      Demand, forecasting and
                      replenishment activity across
                      the retail network
                    </p>

                  </div>


                  <div className="info-chip">

                    <Sparkles size={16} />

                    Gold analytics

                  </div>

                </div>


                <div className="chart-panel">

                  <div className="panel-title">

                    <div>

                      <h3>
                        Demand vs Forecast
                      </h3>

                      <p>
                        Recent demand pattern with
                        model forecast
                      </p>

                    </div>

                  </div>


                  <div className="chart-wrap">

                    <ResponsiveContainer
                      width="100%"
                      height={330}
                    >

                      <AreaChart
                        data={demandTrend}
                      >

                        <defs>

                          <linearGradient
                            id="demandGradient"
                            x1="0"
                            y1="0"
                            x2="0"
                            y2="1"
                          >

                            <stop
                              offset="5%"
                              stopColor="#2dd4bf"
                              stopOpacity={0.32}
                            />

                            <stop
                              offset="95%"
                              stopColor="#2dd4bf"
                              stopOpacity={0}
                            />

                          </linearGradient>

                        </defs>


                        <CartesianGrid
                          strokeDasharray="3 3"
                          stroke="#253448"
                        />


                        <XAxis
                          dataKey="day"
                          stroke="#8797aa"
                          tickLine={false}
                        />


                        <YAxis
                          stroke="#8797aa"
                          tickLine={false}
                        />


                        <Tooltip
                          contentStyle={{
                            background: "#101c2a",
                            border:
                              "1px solid #263b50",
                            borderRadius: "10px",
                          }}
                        />


                        <Legend />


                        <Area
                          type="monotone"
                          dataKey="demand"
                          name="Actual demand"
                          stroke="#2dd4bf"
                          strokeWidth={3}
                          fill="url(#demandGradient)"
                        />


                        <Line
                          type="monotone"
                          dataKey="forecast"
                          name="Forecast"
                          stroke="#f59e0b"
                          strokeWidth={3}
                          dot={false}
                        />

                      </AreaChart>

                    </ResponsiveContainer>

                  </div>

                </div>


                <div className="overview-bottom">


                  <div className="compact-card">

                    <div className="compact-icon teal">
                      <Activity size={21} />
                    </div>

                    <div>

                      <span>
                        Streaming latency
                      </span>

                      <strong>
                        29.499 sec
                      </strong>

                      <small>
                        PASS • target &lt; 30 sec
                      </small>

                    </div>

                  </div>


                  <div className="compact-card">

                    <div className="compact-icon amber">
                      <PackageCheck size={21} />
                    </div>

                    <div>

                      <span>
                        POS retry deduplication
                      </span>

                      <strong>
                        1,000 / 1,000
                      </strong>

                      <small>
                        100% retries removed
                      </small>

                    </div>

                  </div>


                  <div className="compact-card">

                    <div className="compact-icon purple">
                      <Database size={21} />
                    </div>

                    <div>

                      <span>
                        Stock-out rate
                      </span>

                      <strong>
                        0.0004%
                      </strong>

                      <small>
                        PASS • target &lt; 2%
                      </small>

                    </div>

                  </div>


                  <div className="compact-card">

                    <div className="compact-icon blue">
                      <Truck size={21} />
                    </div>

                    <div>

                      <span>
                        Recommended units
                      </span>

                      <strong>
                        3,032,647
                      </strong>

                      <small>
                        replenishment output
                      </small>

                    </div>

                  </div>

                </div>


                <div className="table-panel">

                  <div className="panel-title">

                    <div>

                      <h3>
                        Priority Replenishment
                      </h3>

                      <p>
                        Store-SKU combinations with
                        high recommended order quantities
                      </p>

                    </div>

                  </div>


                  <RetailTable />

                </div>

              </>
            )}


            {/* =============================================
                INVENTORY
            ============================================= */}

            {activeTab === "inventory" && (

              <div className="inventory-layout">


                <section className="lookup-card">

                  <div className="section-header simple">

                    <div>

                      <h2>
                        Real-Time Inventory Lookup
                      </h2>

                      <p>
                        Query current inventory from HBase
                        using store + SKU
                      </p>

                    </div>

                  </div>


                  <label>
                    Store ID
                  </label>

                  <input
                    value={storeId}
                    onChange={(event) =>
                      setStoreId(
                        event.target.value
                      )
                    }
                    placeholder="S163"
                  />


                  <label>
                    SKU
                  </label>

                  <input
                    value={sku}
                    onChange={(event) =>
                      setSku(
                        event.target.value
                      )
                    }
                    placeholder="SKU03782"
                  />


                  <button
                    className="primary-button"
                    onClick={lookupInventory}
                    disabled={inventoryLoading}
                  >

                    <Search size={18} />

                    {inventoryLoading
                      ? "Checking..."
                      : "Check Inventory"}

                  </button>


                  <div className="demo-note">
                    Demo combination:
                    S163 / SKU03782
                  </div>

                </section>


                <section className="inventory-result-card">


                  {!inventoryResult && (

                    <div className="empty-state">

                      <Warehouse size={46} />

                      <h3>
                        Inventory result
                      </h3>

                      <p>
                        Search for a store and SKU
                        to retrieve the latest
                        stock position.
                      </p>

                    </div>

                  )}


                  {inventoryResult &&
                    !inventoryResult.found && (

                      <div className="empty-state warning">

                        <Database size={42} />

                        <h3>
                          Inventory unavailable
                        </h3>

                        <p>
                          {inventoryResult.message ||
                            "Inventory record not found."}
                        </p>

                      </div>

                    )}


                  {inventoryResult?.found && (
                    <>

                      <div className="result-header">

                        <div>

                          <span>
                            Inventory snapshot
                          </span>

                          <h2>
                            {inventoryResult.store_id}
                            {" • "}
                            {inventoryResult.sku}
                          </h2>

                        </div>


                        <span
                          className={`stock-badge ${(
                            inventoryResult.stock_status ||
                            ""
                          ).toLowerCase()}`}
                        >
                          {
                            inventoryResult.stock_status
                          }
                        </span>

                      </div>


                      <div className="result-grid">

                        <ResultMetric
                          label="Quantity On Hand"
                          value={
                            inventoryResult.quantity_on_hand
                          }
                        />

                        <ResultMetric
                          label="Reorder Point"
                          value={
                            inventoryResult.reorder_point
                          }
                        />

                        <ResultMetric
                          label="Maximum Stock"
                          value={
                            inventoryResult.max_stock_level
                          }
                        />

                      </div>


                      <div className="inventory-meta">

                        <div>

                          <span>
                            Last transaction
                          </span>

                          <strong>
                            {inventoryResult.last_transaction_id ||
                              "N/A"}
                          </strong>

                        </div>


                        <div>

                          <span>
                            Data source
                          </span>

                          <strong>
                            {inventoryResult.source ||
                              "backend"}
                          </strong>

                        </div>

                      </div>

                    </>
                  )}

                </section>

              </div>

            )}


            {/* =============================================
                FORECAST
            ============================================= */}

            {activeTab === "forecast" && (
              <>

                <div className="section-header">

                  <div>

                    <h2>
                      Forecast & Replenishment
                    </h2>

                    <p>
                      Demand prediction translated into
                      store-level replenishment actions
                    </p>

                  </div>


                  <div className="info-chip">
                    Random Forest • 16.71% MAPE
                  </div>

                </div>


                <div className="forecast-grid">


                  <div className="chart-panel">

                    <div className="panel-title">

                      <div>

                        <h3>
                          Top Stores by Recommended
                          Order Qty
                        </h3>

                        <p>
                          Gold replenishment
                          recommendations
                        </p>

                      </div>

                    </div>


                    <ResponsiveContainer
                      width="100%"
                      height={365}
                    >

                      <BarChart
                        data={reorderByStore}
                        layout="vertical"
                      >

                        <CartesianGrid
                          strokeDasharray="3 3"
                          stroke="#253448"
                        />


                        <XAxis
                          type="number"
                          stroke="#8797aa"
                        />


                        <YAxis
                          type="category"
                          dataKey="store"
                          stroke="#8797aa"
                          width={75}
                        />


                        <Tooltip
                          contentStyle={{
                            background:
                              "#101c2a",
                            border:
                              "1px solid #263b50",
                            borderRadius:
                              "10px",
                          }}
                        />


                        <Bar
                          dataKey="qty"
                          name="Recommended qty"
                          fill="#2dd4bf"
                          radius={[
                            0,
                            7,
                            7,
                            0,
                          ]}
                        />

                      </BarChart>

                    </ResponsiveContainer>

                  </div>


                  <div className="forecast-summary">

                    <h3>
                      Forecast Summary
                    </h3>


                    <ForecastRow
                      label="Model"
                      value="Random Forest"
                    />


                    <ForecastRow
                      label="Validation MAPE"
                      value="16.71%"
                    />


                    <ForecastRow
                      label="Target"
                      value="< 25% • PASS"
                    />


                    <ForecastRow
                      label="Forecast Month"
                      value="Jan 2026"
                    />


                    <ForecastRow
                      label="SKU Forecasts"
                      value="4,998"
                    />


                    <ForecastRow
                      label="Store-SKU Forecasts"
                      value="164,191"
                    />


                    <div className="forecast-note">

                      Seasonal same-month-last-year
                      achieved 12.94% as a baseline,
                      while Random Forest is the
                      formal ML model.

                    </div>

                  </div>

                </div>


                <div className="table-panel">

                  <div className="panel-title">

                    <div>

                      <h3>
                        Replenishment Recommendations
                      </h3>

                      <p>
                        Forecast + current stock +
                        reorder thresholds +
                        supplier mapping
                      </p>

                    </div>

                  </div>


                  <RetailTable />

                </div>

              </>
            )}


            {/* =============================================
                VALIDATION
            ============================================= */}

            {activeTab === "validation" && (
              <>

                <div className="section-header">

                  <div>

                    <h2>
                      Validation & Reliability
                    </h2>

                    <p>
                      Acceptance tests across
                      streaming, forecasting
                      and resilience
                    </p>

                  </div>

                </div>


                <div className="validation-grid">

                  <ValidationCard
                    value="29.499 s"
                    title="Streaming latency"
                    description="Target < 30 seconds"
                  />


                  <ValidationCard
                    value="16.71%"
                    title="Forecast MAPE"
                    description="Target < 25%"
                  />


                  <ValidationCard
                    value="0.0004%"
                    title="Stock-out rate"
                    description="Target < 2%"
                  />


                  <ValidationCard
                    value="1,000 / 1,000"
                    title="Retries removed"
                    description="100% POS deduplication"
                  />


                  <ValidationCard
                    value="10 / 10"
                    title="Kafka messages retained"
                    description="After one broker was killed"
                  />


                  <ValidationCard
                    value="0"
                    title="Restart duplicates"
                    description="Spark checkpoint recovery"
                  />

                </div>

              </>
            )}

          </section>

        </main>


        {/* =================================================
            RIGHT SIDE AI ASSISTANT
        ================================================= */}

        <aside className="assistant-panel">


          <div className="assistant-header">

            <div className="assistant-icon">
              <Bot size={23} />
            </div>


            <div>

              <h2>
                Retail Ops Assistant
              </h2>

              <p>
                Live inventory + project intelligence
              </p>

            </div>

          </div>


          <div className="suggestions">

            <span className="suggestion-label">
              Try asking
            </span>


            {suggestedQuestions.map(
              (question) => (

                <button
                  key={question}
                  onClick={() =>
                    askAssistant(
                      question
                    )
                  }
                >
                  {question}
                </button>

              )
            )}

          </div>


          <div className="conversation">


            {messages.map(
              (message, index) => (

                <div
                  className={`message ${message.role}`}
                  key={`${message.role}-${index}`}
                >

                  <div className="message-role">

                    {message.role === "user"
                      ? "You"
                      : "Assistant"}

                  </div>


                  <div className="message-text">

                    {message.text}

                  </div>

                </div>

              )
            )}


            {chatLoading && (

              <div className="message assistant">

                <div className="message-role">
                  Assistant
                </div>

                <div className="typing">
                  Analyzing retail operations...
                </div>

              </div>

            )}

          </div>


          <form
            className="chat-form"
            onSubmit={(event) => {

              event.preventDefault();

              askAssistant(
                chatInput
              );

            }}
          >

            <input
              value={chatInput}
              onChange={(event) =>
                setChatInput(
                  event.target.value
                )
              }
              placeholder="Ask about stock, forecasting, KPIs..."
            />


            <button
              type="submit"
              disabled={chatLoading}
            >
              <Send size={18} />
            </button>

          </form>

        </aside>

      </section>

    </div>
  );
}


function ResultMetric({
  label,
  value,
}) {
  return (
    <div className="result-metric">

      <span>
        {label}
      </span>

      <strong>
        {value ?? "N/A"}
      </strong>

    </div>
  );
}


function ForecastRow({
  label,
  value,
}) {
  return (
    <div className="forecast-row">

      <span>
        {label}
      </span>

      <strong>
        {value}
      </strong>

    </div>
  );
}


function ValidationCard({
  value,
  title,
  description,
}) {
  return (
    <div className="validation-card">

      <div className="validation-check">
        ✓
      </div>

      <strong>
        {value}
      </strong>

      <span>
        {title}
      </span>

      <small>
        {description}
      </small>

    </div>
  );
}


function RetailTable() {
  return (
    <div className="table-scroll">

      <table>

        <thead>

          <tr>

            <th>Store</th>
            <th>SKU</th>
            <th>Opening</th>
            <th>Forecast</th>
            <th>Projected</th>
            <th>Reorder Level</th>
            <th>Supplier</th>
            <th>Recommended Qty</th>

          </tr>

        </thead>


        <tbody>

          {replenishmentRows.map(
            (row) => (

              <tr
                key={`${row.store}-${row.sku}`}
              >

                <td>{row.store}</td>

                <td>{row.sku}</td>

                <td>{row.stock}</td>

                <td>{row.forecast}</td>

                <td>{row.projected}</td>

                <td>{row.reorder}</td>

                <td>{row.supplier}</td>

                <td>
                  <strong>
                    {row.orderQty}
                  </strong>
                </td>

              </tr>

            )
          )}

        </tbody>

      </table>

    </div>
  );
}


export default App;
