import os,json,time
os.makedirs("results",exist_ok=True)
tickets=[
 "Cannot access API; authentication returns 401",
 "Please explain the invoice and refund my duplicate charge",
 "Could you add a dark mode feature?",
 "Our service is down and all users are affected",
 "How do I change my billing plan?"
]
def route(text):
 t=text.lower()
 if any(w in t for w in ["down","401","error","cannot access","bug"]): category="technical"; priority="high" if any(w in t for w in ["down","all users"]) else "medium"
 elif any(w in t for w in ["invoice","refund","billing","charge","plan"]): category="billing"; priority="medium"
 else: category="feature_request"; priority="low"
 return {"category":category,"priority":priority,"draft_response":f"Thanks for contacting support. Your {category.replace('_',' ')} request has been recorded; our team will review it."}
start=time.perf_counter(); rows=[{"ticket":t,"result":route(t)} for t in tickets]; latency=(time.perf_counter()-start)/len(tickets)
out={"mode":"deterministic fallback; no external LLM/API required","average_latency_seconds":latency,"examples":rows,"note":"For actual LLM benchmarking, configure an API provider and compare zero-shot/few-shot prompts."}
open("results/p4_ticket_routing.json","w").write(json.dumps(out,indent=2)); print(json.dumps(out,indent=2))
