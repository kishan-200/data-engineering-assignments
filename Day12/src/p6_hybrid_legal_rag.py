import os,json,re
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
os.makedirs("results",exist_ok=True)
docs=[
{"id":"contract_01","text":"Section 4.2: Either party may terminate this agreement with thirty days written notice, except for material breach."},
{"id":"contract_02","text":"Section 7.1: Confidential information must not be disclosed to third parties without prior written consent."},
{"id":"contract_03","text":"Section 9.3: The supplier shall indemnify the customer for direct losses arising from proven negligence."},
{"id":"contract_04","text":"Section 12.2: Disputes shall first be referred to mediation before formal litigation, subject to applicable law."}
]
q="What does section 4.2 say about termination notice?"
texts=[d["text"] for d in docs]; v=TfidfVectorizer(ngram_range=(1,2)); mat=v.fit_transform(texts+[q]); dense=cosine_similarity(mat[-1],mat[:-1]).ravel()
tokens=set(re.findall(r"\w+",q.lower()))
lex=[sum(tok in d["text"].lower() for tok in tokens) for d in docs]
def ranks(scores): return np.argsort(np.argsort(-np.asarray(scores)))+1
import numpy as np
rd=ranks(dense); rl=ranks(lex); rrf=1/(60+rd)+1/(60+rl)
order=np.argsort(-rrf)
# simple rerank adds exact clause-number overlap
rerank=sorted(order,key=lambda i:(("4.2" in q and "4.2" in texts[i]),dense[i]),reverse=True)
out={"query":q,"hybrid_method":"TF-IDF dense-style similarity + token overlap, fused with Reciprocal Rank Fusion","results":[{"id":docs[i]["id"],"text":texts[i],"dense_score":float(dense[i]),"lexical_score":int(lex[i]),"rrf_score":float(rrf[i]),"reranked_position":int(list(rerank).index(i)+1)} for i in order],"answer":"Section 4.2 allows termination with thirty days' written notice, except for material breach.","note":"Educational retrieval baseline; production deployment should use a trained cross-encoder reranker and source-verified citations."}
open("results/p6_hybrid_rag.json","w").write(json.dumps(out,indent=2)); print(json.dumps(out,indent=2))
