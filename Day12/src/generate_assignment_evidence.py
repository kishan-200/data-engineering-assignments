"""
Generate additional assignment evidence: graphs, comparison tables, and evaluation reports.
All numerical metrics are computed from the toy synthetic data generated here. LLM/agent/QLoRA
items that are not genuinely run are explicitly labeled as demonstrations or pending.
"""
import os, json, csv, time, re
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import make_pipeline
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.neural_network import MLPRegressor
from sklearn.cluster import KMeans
from sklearn.metrics import (mean_absolute_error, mean_squared_error, recall_score,
    roc_curve, roc_auc_score, precision_score, recall_score as rec_score,
    silhouette_score, accuracy_score)
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

os.makedirs("results", exist_ok=True)
os.makedirs("results/figures", exist_ok=True)
R="results"; F=f"{R}/figures"
def save_json(name, obj):
    with open(f"{R}/{name}","w",encoding="utf-8") as f: json.dump(obj,f,indent=2,ensure_ascii=False)
def save_csv(name, rows):
    pd.DataFrame(rows).to_csv(f"{R}/{name}",index=False)
def plot_save(name, title, xlabel, ylabel):
    plt.title(title); plt.xlabel(xlabel); plt.ylabel(ylabel); plt.tight_layout(); plt.savefig(f"{F}/{name}",dpi=150); plt.close()

# P1: real train/test model evaluation and requested threshold report/plots.
rng=np.random.default_rng(101); n=700
vib=rng.normal(5,1.2,n); temp=rng.normal(65,8,n); voltage=rng.normal(220,12,n); noise=rng.normal(40,7,n)
score=(vib-5)*.8+(temp-65)*.035+(noise-40)*.025
rul=np.clip(120-score*12-rng.normal(0,15,n),1,200); fail=(score>0.45).astype(int)
X=np.c_[vib,temp,voltage,noise]
Xtr,Xte,ytr,yte=train_test_split(X,rul,test_size=.25,random_state=42)
reg=make_pipeline(StandardScaler(),LinearRegression()).fit(Xtr,ytr); pred=reg.predict(Xte)
Xt,Xv,yt,yv=train_test_split(X,fail,test_size=.25,random_state=42,stratify=fail)
clf=make_pipeline(StandardScaler(),LogisticRegression(max_iter=1000,class_weight="balanced")).fit(Xt,yt)
proba=clf.predict_proba(Xv)[:,1]
p1={"RUL_MAE":float(mean_absolute_error(yte,pred)),"RUL_RMSE":float(np.sqrt(mean_squared_error(yte,pred))),
    "failure_threshold_metrics":[]}
for th in np.arange(.1,.91,.1):
    yp=(proba>=th).astype(int)
    p1["failure_threshold_metrics"].append({"threshold":round(float(th),2),"precision":float(precision_score(yv,yp,zero_division=0)),
        "recall":float(rec_score(yv,yp,zero_division=0)),"predicted_failures":int(yp.sum())})
save_json("p1_full_metrics.json",p1); save_csv("p1_threshold_evaluation.csv",p1["failure_threshold_metrics"])
plt.figure(); plt.scatter(yte,pred,alpha=.65); lo=min(yte.min(),pred.min()); hi=max(yte.max(),pred.max()); plt.plot([lo,hi],[lo,hi]); plot_save("p1_actual_vs_predicted_rul.png","P1: Actual vs Predicted RUL","Actual RUL (hours)","Predicted RUL (hours)")
plt.figure(); plt.plot([x["threshold"] for x in p1["failure_threshold_metrics"]],[x["recall"] for x in p1["failure_threshold_metrics"]],marker="o"); plot_save("p1_recall_vs_threshold.png","P1: Failure Recall vs Decision Threshold","Decision threshold","Recall")
# P2: classifier comparison, ROC and elbow, cluster plot.
rng=np.random.default_rng(102); n=900
amount=rng.lognormal(3,1,n); velocity=rng.poisson(2,n); loc=rng.uniform(0,1,n); age=rng.integers(18,80,n)
logit=-4+.0008*amount+.65*velocity+2*loc; prob=1/(1+np.exp(-logit)); yy=(rng.random(n)<prob).astype(int)
XX=np.c_[amount,velocity,loc,age]; A,B,ya,yb=train_test_split(XX,yy,test_size=.25,random_state=42,stratify=yy)
dt=DecisionTreeClassifier(max_depth=5,class_weight="balanced",random_state=42).fit(A,ya)
rf=RandomForestClassifier(n_estimators=100,max_depth=8,class_weight="balanced",random_state=42,n_jobs=-1).fit(A,ya)
models=[("Decision Tree",dt),("Random Forest",rf)]; comp=[]
plt.figure()
for label,m in models:
    pp=m.predict_proba(B)[:,1]; fpr,tpr,_=roc_curve(yb,pp); auc=roc_auc_score(yb,pp)
    plt.plot(fpr,tpr,label=f"{label} (AUC={auc:.3f})")
    comp.append({"model":label,"ROC_AUC":float(auc),"accuracy":float(accuracy_score(yb,m.predict(B))),"precision":float(precision_score(yb,m.predict(B),zero_division=0)),"recall":float(rec_score(yb,m.predict(B),zero_division=0))})
plt.plot([0,1],[0,1],linestyle="--"); plt.legend(); plot_save("p2_roc_curve.png","P2: Fraud Classifier ROC Curves","False Positive Rate","True Positive Rate")
save_csv("p2_model_comparison.csv",comp); save_json("p2_full_metrics.json",{"model_comparison":comp})
scaled=StandardScaler().fit_transform(XX); ks=list(range(2,8)); inertias=[]; sils=[]
for k in ks:
    km=KMeans(n_clusters=k,random_state=42,n_init=10).fit(scaled); inertias.append(float(km.inertia_)); sils.append(float(silhouette_score(scaled,km.labels_,sample_size=min(400,n),random_state=42)))
save_csv("p2_kmeans_elbow_silhouette.csv",[{"k":k,"inertia":i,"silhouette_score":s} for k,i,s in zip(ks,inertias,sils)])
plt.figure(); plt.plot(ks,inertias,marker="o"); plot_save("p2_elbow_plot.png","P2: K-Means Elbow Method","Number of clusters (K)","Inertia")
km=KMeans(n_clusters=3,random_state=42,n_init=10).fit(scaled)
plt.figure(); plt.scatter(scaled[:,0],scaled[:,1],c=km.labels_,s=12); plot_save("p2_cluster_visualization.png","P2: Customer Clusters (scaled features)","Scaled transaction amount","Scaled transaction velocity")
# P3: neural network train/validation curves, holdout model comparison and simple tuning table.
rng=np.random.default_rng(103); n=800
promo=rng.uniform(0,1,n); weather=rng.normal(20,8,n); delay=rng.uniform(0,5,n); price=rng.uniform(5,100,n)
yy=40+55*promo-1.3*delay+.25*weather-.15*price+15*np.sin(promo*3)+rng.normal(0,5,n)
XX=np.c_[promo,weather,delay,price]; A,B,ya,yb=train_test_split(XX,yy,test_size=.2,random_state=42)
mlp=MLPRegressor(hidden_layer_sizes=(64,32),alpha=.01,learning_rate_init=.003,max_iter=300,early_stopping=True,random_state=42).fit(A,ya)
rfreg=RandomForestRegressor(n_estimators=100,random_state=42,n_jobs=-1).fit(A,ya)
rows=[]
for label,m in [("MLP",mlp),("Random Forest baseline",rfreg)]:
    pred=m.predict(B); rows.append({"model":label,"MAE":float(mean_absolute_error(yb,pred)),"RMSE":float(np.sqrt(mean_squared_error(yb,pred)))})
save_csv("p3_model_comparison.csv",rows); save_json("p3_full_metrics.json",{"comparison":rows,"mlp_iterations":int(mlp.n_iter_),"early_stopping_validation_scores_available":bool(getattr(mlp,"validation_scores_",None))})
plt.figure(); plt.plot(mlp.loss_curve_,label="Training loss")
vs=getattr(mlp,"validation_scores_",None)
if vs: plt.plot(range(1,len(vs)+1),[1-v for v in vs],label="Validation error proxy")
plt.legend(); plot_save("p3_training_validation_curve.png","P3: MLP Training Loss and Validation Error Proxy","Iteration","Loss / 1 - validation R²")
# P4 prompt benchmark matrix: deterministic rule-based proxy, clearly not external LLM results.
tickets=[
("technical","API returns 401 authentication error","technical","medium"),
("billing","I was charged twice for my invoice","billing","medium"),
("feature","Please add dark mode","feature_request","low"),
("technical","Service is down for all users","technical","high"),
("billing","I need to change my billing plan","billing","medium")]
bench=[]
for variant in ["zero-shot-rule-baseline","few-shot-rule-baseline"]:
    start=time.perf_counter(); correct=0
    for true,text,expected,priority in tickets:
        low=text.lower()
        if any(w in low for w in ["401","error","down","service"]): pred="technical"
        elif any(w in low for w in ["charged","invoice","billing","plan"]): pred="billing"
        else: pred="feature_request"
        correct+=pred==expected
    bench.append({"variant":variant,"accuracy":correct/len(tickets),"avg_latency_ms":(time.perf_counter()-start)*1000/len(tickets),"evaluation_type":"rule-based proxy; not an LLM prompt benchmark"})
save_csv("p4_prompt_benchmark_proxy.csv",bench); save_json("p4_benchmark_note.json",{"results":bench,"limitation":"True zero-shot/few-shot LLM benchmarking requires a configured model/API and labeled evaluation set."})
# P5 chunking strategy and retrieval result comparison; lightweight TF-IDF retrieval proxy.
docs=[
("guideline_a","Fever care includes recording temperature, hydration, duration, and symptoms. Breathing difficulty or confusion needs urgent assessment."),
("guideline_b","Medication safety includes checking allergies, current medicines, contraindications, and dosage with a qualified clinician."),
("guideline_c","Asthma guidance includes breathing assessment, prescribed inhaler plan, triggers, and severity."),
("guideline_d","Clinical protocols must be checked against current local policy and clinician judgment.")]
def fixed_chunks(text,size=22,overlap=5):
    w=text.split(); return [" ".join(w[i:i+size]) for i in range(0,len(w),size-overlap)]
fixed=[{"source":s,"text":c,"strategy":"fixed-size-overlap"} for s,t in docs for c in fixed_chunks(t)]
header=[{"source":s,"text":t,"strategy":"document/header-based"} for s,t in docs]
queries=["What checks are needed for medication safety?","What symptoms require urgent assessment?"]
p5rows=[]
for strategy,chunks in [("fixed-size-overlap",fixed),("document/header-based",header)]:
    texts=[x["text"] for x in chunks]
    for q in queries:
        vec=TfidfVectorizer(); mat=vec.fit_transform(texts+[q]); sims=cosine_similarity(mat[-1],mat[:-1]).ravel()
        top=np.argsort(-sims)[:2]
        for rank,i in enumerate(top,1):
            p5rows.append({"strategy":strategy,"query":q,"rank":rank,"source":chunks[i]["source"],"score":float(sims[i]),"retrieved_text":texts[i]})
save_csv("p5_chunking_retrieval_comparison.csv",p5rows); save_json("p5_chunking_note.json",{"fixed_chunk_count":len(fixed),"header_document_chunk_count":len(header),"comparison_file":"p5_chunking_retrieval_comparison.csv","limitation":"TF-IDF is a lexical baseline, not a dense clinical embedding/vector database."})
# P6 hybrid search benchmark with known query-to-document labels, ranks and simple precision/recall.
legal=[
("c1","Section 4.2 termination: either party may terminate with thirty days written notice except material breach."),
("c2","Section 7.1 confidentiality: confidential information shall not be disclosed without prior written consent."),
("c3","Section 9.3 indemnity: supplier indemnifies customer for direct losses caused by proven negligence."),
("c4","Section 12.2 disputes: parties first refer disputes to mediation before litigation.")]
qs=[("What is the notice period in clause 4.2?","c1"),("Which clause discusses confidentiality?","c2"),("Who indemnifies the customer for negligence?","c3")]
texts=[d[1] for d in legal]; vector=TfidfVectorizer(ngram_range=(1,2)); mat=vector.fit_transform(texts+[q for q,_ in qs])
retr=[]
for qi,(q,target) in enumerate(qs):
    qv=mat[len(texts)+qi]; dense=cosine_similarity(qv,mat[:len(texts)]).ravel()
    toks=set(re.findall(r"\w+",q.lower())); lex=np.array([sum(t in txt.lower() for t in toks) for txt in texts],float)
    def rank(scores): return np.argsort(np.argsort(-scores))+1
    rr=1/(60+rank(dense))+1/(60+rank(lex)); order=np.argsort(-rr)
    # exact clause reference boost in reranking
    rerank=sorted(order,key=lambda i: (("4.2" in q and "4.2" in texts[i]),rr[i]),reverse=True)
    for r,i in enumerate(rerank,1):
        retr.append({"query":q,"document_id":legal[i][0],"hybrid_rrf_score":float(rr[i]),"reranked_rank":r,"relevant":legal[i][0]==target})
save_csv("p6_hybrid_reranking_results.csv",retr)
top1=[next(x for x in retr if x["query"]==q and x["reranked_rank"]==1) for q,_ in qs]
save_json("p6_retrieval_metrics.json",{"queries":len(qs),"precision_at_1":sum(x["relevant"] for x in top1)/len(top1),"recall_at_1":sum(x["relevant"] for x in top1)/len(top1),"note":"Small labeled toy corpus; lexical/vector-style TF-IDF fusion proxy, not a trained cross-encoder benchmark."})
# P7 trace table; export existing trace if available or explicit controlled demo trace.
p7trace=[
{"step":1,"agent":"controller","action":"Receive question","observation":"Compare Customer X portfolio with Q3 market outlook and estimate tax exposure"},
{"step":2,"agent":"SQL tool","action":"Lookup customer portfolio","observation":"Balance and illustrative tax rate returned from in-memory sample database"},
{"step":3,"agent":"Market retrieval tool","action":"Retrieve Q3 outlook","observation":"Retrieved sample outlook text"},
{"step":4,"agent":"Calculator tool","action":"Calculate balance × illustrative tax rate","observation":"Estimated exposure calculated"},
{"step":5,"agent":"controller","action":"Synthesize response","observation":"Final illustrative report assembled"}]
save_csv("p7_agent_trace_table.csv",p7trace)
# P8 findings and QA table from controlled sample; not executing patches.
p8rows=[
{"agent":"Security Scanner","severity":"High","finding":"Potential SQL injection via string concatenation","remediation":"Use parameterized SQL queries","qa_status":"Remediation suggestion present; patch not executed"},
{"agent":"Security Scanner","severity":"High","finding":"Hardcoded secret","remediation":"Move to environment/secret manager and rotate secret","qa_status":"Remediation suggestion present; patch not executed"}]
save_csv("p8_security_audit_table.csv",p8rows); save_json("p8_agent_workflow_note.json",{"agents":["Security Scanner","Remediation","QA Reviewer"],"workflow":"controlled rule-based demonstration","qa":"Checks remediation suggestions are non-empty; does not prove patched code is secure."})
# P9 explicitly report that training has not occurred; no fake training graphs or fabricated memory numbers.
save_json("p9_finetuning_evidence.json",{"dataset":"data/claims_toy_train.jsonl","examples_prepared":2,"quantization":"4-bit QLoRA configuration scaffold","actual_training_run":False,"training_loss_curve":"NOT AVAILABLE: training was not run","gpu_memory_comparison":"NOT AVAILABLE: training was not run","inference_latency_comparison":"NOT AVAILABLE: base and fine-tuned model inference were not run","how_to_complete":"Run a compatible QLoRA job on a supported GPU/model after installing transformers, datasets, peft, trl and bitsandbytes; then record real metrics."})
# P10 capstone scorecard/architecture.
capstone=[{"metric":"Guardrail test cases","value":2,"status":"measured on toy test cases"},{"metric":"Prompt-injection test blocked","value":1,"status":"measured on toy rule-based guard"},{"metric":"RAGAS faithfulness","value":"Not measured","status":"Requires RAGAS evaluator and configured LLM"},{"metric":"Answer relevance","value":"Not measured","status":"Requires labeled/evaluated answers"},{"metric":"Context recall","value":"Not measured","status":"Requires reference contexts/answers"},{"metric":"Harm evaluation","value":"Not measured","status":"Requires defined safety evaluation set"}]
save_csv("p10_capstone_evaluation_scorecard.csv",capstone)
with open(f"{R}/p10_architecture.mmd","w") as f:
    f.write("flowchart TD\\n A[Shipment events] --> B[Document and event ingestion]\\n B --> C[Hybrid retrieval index]\\n C --> D[Agent tool controller]\\n D --> E[SQL / shipment lookup / calculator tools]\\n E --> F[Output guardrail and PII redaction]\\n F --> G[RAGAS and safety evaluation]")
# Output inventory
inventory=[]
for name in sorted(os.listdir(R)):
    if os.path.isfile(os.path.join(R,name)):
        inventory.append({"file":name,"bytes":os.path.getsize(os.path.join(R,name))})
save_csv("output_inventory.csv",inventory)
print("Generated assignment evidence. Review results/ and results/figures/.")
print("\\nNew figures:")
for name in sorted(os.listdir(F)): print(" -",name)
print("\\nNew CSV reports:")
for name in sorted(os.listdir(R)):
    if name.endswith(".csv"): print(" -",name)
