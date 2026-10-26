import os,json
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.neural_network import MLPRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error,mean_squared_error
os.makedirs("results",exist_ok=True)
rng=np.random.default_rng(13); n=800
promo=rng.uniform(0,1,n); weather=rng.normal(20,8,n); delay=rng.uniform(0,5,n); price=rng.uniform(5,100,n)
y=40+55*promo-1.3*delay+0.25*weather-0.15*price+15*np.sin(promo*3)+rng.normal(0,5,n)
X=np.c_[promo,weather,delay,price]
Xa,Xb,ya,yb=train_test_split(X,y,test_size=.2,random_state=42)
mlp=MLPRegressor(hidden_layer_sizes=(64,32),activation="relu",alpha=.01,learning_rate_init=.003,max_iter=250,early_stopping=True,random_state=42).fit(Xa,ya)
rf=RandomForestRegressor(n_estimators=100,random_state=42,n_jobs=-1).fit(Xa,ya)
def score(m): return {"MAE":float(mean_absolute_error(yb,m.predict(Xb))),"RMSE":float(np.sqrt(mean_squared_error(yb,m.predict(Xb))))}
out={"MLP":score(mlp),"RandomForest_baseline":score(rf),"MLP_iterations":int(mlp.n_iter_)}
open("results/p3_metrics.json","w").write(json.dumps(out,indent=2)); print(out)
try:
 import matplotlib.pyplot as plt
 plt.figure(); plt.plot(mlp.loss_curve_); plt.title("MLP training loss"); plt.xlabel("Iteration"); plt.ylabel("Loss"); plt.tight_layout(); plt.savefig("results/p3_loss_curve.png"); plt.close()
except Exception as e: print("Plot skipped:",e)
