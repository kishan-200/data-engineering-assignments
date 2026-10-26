import os, json
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, PolynomialFeatures
from sklearn.pipeline import make_pipeline
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, recall_score
os.makedirs("results", exist_ok=True)
rng=np.random.default_rng(11); n=700
vib=rng.normal(5,1.2,n); temp=rng.normal(65,8,n); voltage=rng.normal(220,12,n); noise=rng.normal(40,7,n)
risk_score=(vib-5)*.8+(temp-65)*.035+(noise-40)*.025
rul=np.clip(120-risk_score*12-rng.normal(0,15,n),1,200)
failure=(risk_score>0.45).astype(int)
X=np.c_[vib,temp,voltage,noise]
Xtr,Xte,ytr,yte=train_test_split(X,rul,test_size=.25,random_state=42)
reg=make_pipeline(PolynomialFeatures(2,include_bias=False),StandardScaler(),LinearRegression()).fit(Xtr,ytr)
pred=reg.predict(Xte)
Xtr2,Xte2,yt,yv=train_test_split(X,failure,test_size=.25,random_state=42,stratify=failure)
clf=make_pipeline(StandardScaler(),LogisticRegression(max_iter=1000,class_weight="balanced")).fit(Xtr2,yt)
proba=clf.predict_proba(Xte2)[:,1]; threshold=.4; cls=(proba>=threshold).astype(int)
out={"rul_MAE":float(mean_absolute_error(yte,pred)),"rul_RMSE":float(np.sqrt(mean_squared_error(yte,pred))),"failure_recall_at_threshold_0.4":float(recall_score(yv,cls,zero_division=0)),"threshold":threshold}
open("results/p1_metrics.json","w").write(json.dumps(out,indent=2)); print(out)
