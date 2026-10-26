import os,json
os.makedirs("results",exist_ok=True)
examples=[
{"system":"You are an insurance claims assistant. Summarize evidence and flag uncertainty; do not invent facts.","user":"A pipe burst damaged kitchen cabinets. Photos and repair estimate are attached. Policy has a water-damage clause.","assistant":"Claim type: water damage. Evidence noted: photos and repair estimate. Review policy exclusions and verify cause before settlement."},
{"system":"You are an insurance claims assistant. Summarize evidence and flag uncertainty; do not invent facts.","user":"Vehicle bumper damaged in a parking lot. No witness details supplied.","assistant":"Claim type: vehicle damage. Evidence is incomplete because witness details are not supplied. Request supporting photographs and incident documentation."}
]
with open("data/claims_toy_train.jsonl","w") as f:
 for ex in examples: f.write(json.dumps(ex)+"\\n")
out={"training_examples_created":len(examples),"dataset_path":"data/claims_toy_train.jsonl","qlora_config":{"load_in_4bit":True,"lora_r":8,"lora_alpha":16,"target_modules":["q_proj","v_proj"],"epochs":1,"max_seq_length":512},"status":"Preparation/configuration scaffold completed; actual model fine-tuning NOT run by this lightweight script.","requirements_for_real_run":["CUDA GPU with sufficient memory","approved accessible base model and license","transformers, datasets, peft, trl, bitsandbytes (CUDA/Linux compatibility)"],"evaluation_plan":["training loss curve","held-out claim classification/summary quality","inference latency","GPU memory comparison"]}
open("results/p9_qlora_status.json","w").write(json.dumps(out,indent=2)); print(json.dumps(out,indent=2))
