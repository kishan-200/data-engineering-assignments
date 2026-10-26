"""Run the 10 compact ML/GenAI demonstrations in sequence."""
import subprocess, sys, os
scripts = [
    "p1_iot_anomaly.py", "p2_fraud_segmentation.py", "p3_demand_forecasting.py",
    "p4_support_intent_router.py", "p5_basic_healthcare_rag.py", "p6_hybrid_legal_rag.py",
    "p7_financial_analyst_agent.py", "p8_multi_agent_devsecops.py",
    "p9_qlora_claims_scaffold.py", "p10_agentic_rag_capstone.py"
]
os.makedirs("results", exist_ok=True)
for script in scripts:
    print("\\n" + "="*72 + "\\nRUNNING " + script + "\\n" + "="*72, flush=True)
    result = subprocess.run([sys.executable, os.path.join("src", script)])
    if result.returncode:
        print(f"WARNING: {script} exited with code {result.returncode}. Continuing.", flush=True)
print("\\nGenerating additional assignment graphs, tables, and evaluation evidence...", flush=True)
subprocess.run([sys.executable, os.path.join("src", "generate_assignment_evidence.py")])
print("\\nAll demo scripts attempted. Inspect results/ and any warnings above.")
