import os,json,re
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
os.makedirs("results",exist_ok=True)
docs=[
("guideline_a","For fever, record temperature, hydration status, duration, and associated symptoms. Seek urgent assessment for breathing difficulty or confusion."),
("guideline_b","Medication safety requires checking allergies, current medicines, contraindications, and dosage instructions with a qualified clinician."),
("guideline_c","For asthma symptoms, assess breathing, prescribed inhaler plan, triggers, and severity. Emergency symptoms require immediate medical attention."),
("guideline_d","Clinical protocols should be reviewed against current local policy and the treating clinician's judgment.")
]
def chunks(text,size=28,overlap=6):
 words=text.split(); return [" ".join(words[i:i+size]) for i in range(0,len(words),size-overlap)]
chunks_all=[{"source":name,"text":c} for name,txt in docs for c in chunks(txt)]
texts=[x["text"] for x in chunks_all]; vec=TfidfVectorizer().fit_transform(texts)
queries=["What should be checked for medication safety?","Which symptoms may need urgent assessment?"]
results=[]
for q in queries:
 sims=cosine_similarity(TfidfVectorizer(vocabulary=None).fit_transform([q]+texts))[0][1:] if False else None
 # fit one shared vocabulary so query and documents are comparable
 v=TfidfVectorizer(); mat=v.fit_transform(texts+[q]); scores=cosine_similarity(mat[-1],mat[:-1]).ravel(); idx=scores.argsort()[::-1][:2]
 results.append({"query":q,"retrieved":[{"source":chunks_all[i]["source"],"text":texts[i],"score":float(scores[i])} for i in idx]})
out={"chunk_count":len(chunks_all),"strategy":"fixed-size word chunks with overlap; TF-IDF retrieval baseline (not a clinical decision system)","queries":results}
open("results/p5_rag_retrieval.json","w").write(json.dumps(out,indent=2)); print(json.dumps(out,indent=2))
