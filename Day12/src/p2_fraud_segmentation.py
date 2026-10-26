import os,json
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import roc_auc_score,classification_report,silhouette_score
os.makedirs("results",exist_ok=True)
rng=np.random.default_rng(12); n=900
amount=rng.lognormal(3,1,n); velocity=rng.poisson(2,n); location_delta=rng.uniform(0,1,n); age=rng.integers(18,80,n)
logit=-4+0.0008*amount+0.65*velocity+2.0*location_delta
p=1/(1+np.exp(-logit)); y=(rng.random(n)<p).astype(int)
X=np.c_[amount,velocity,location_delta,age]
Xa,Xb,ya,yb=train_test_split(X,y,test_size=.25,random_state=42,stratify=y)
dt=DecisionTreeClassifier(max_depth=5,class_weight="balanced",random_state=42).fit(Xa,ya)
rf=RandomForestClassifier(n_estimators=100,max_depth=8,class_weight="balanced",random_state=42,n_jobs=-1).fit(Xa,ya)
metrics={"decision_tree_roc_auc":float(roc_auc_score(yb,dt.predict_proba(Xb)[:,1])),"random_forest_roc_auc":float(roc_auc_score(yb,rf.predict_proba(Xb)[:,1]))}
profiles=StandardScaler().fit_transform(np.c_[amount,velocity,location_delta,age])
km=KMeans(n_clusters=3,random_state=42,n_init=10).fit(profiles)
metrics["kmeans_silhouette_sample"]=float(silhouette_score(profiles,km.labels_,sample_size=min(400,len(profiles)),random_state=42))
metrics["cluster_sizes"]={str(int(k)):int(v) for k,v in zip(*np.unique(km.labels_,return_counts=True))}
open("results/p2_metrics.json","w").write(json.dumps(metrics,indent=2)); print(metrics)
