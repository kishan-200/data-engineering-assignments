import os,json,re
os.makedirs("results",exist_ok=True)
code='''def get_user(query):
    sql = "SELECT * FROM users WHERE name = '" + query + "'"
    return db.execute(sql)
password = "hardcoded-secret"
'''
trace=[]
def scanner(code):
 findings=[]
 if re.search(r'["\']\s*\+\s*\w+',code) or "SELECT * FROM users WHERE name = '" in code: findings.append({"severity":"high","issue":"Potential SQL injection via string concatenation","line_hint":"query construction","fix":"Use parameterized SQL queries."})
 if "hardcoded-secret" in code: findings.append({"severity":"high","issue":"Hardcoded secret","line_hint":"password assignment","fix":"Load secret from environment/secret manager; rotate exposed credentials."})
 trace.append({"agent":"Security Scanner","findings":findings}); return findings
def remediation(findings):
 patches=[f["fix"] for f in findings]; trace.append({"agent":"Remediation","suggestions":patches}); return patches
def qa(findings,patches):
 ok=all(bool(p.strip()) for p in patches) and len(patches)==len(findings)
 trace.append({"agent":"QA Reviewer","validation_passed":ok,"checks":["one remediation per finding","non-empty remediation"]}); return ok
findings=scanner(code); patches=remediation(findings); passed=qa(findings,patches)
out={"findings":findings,"remediation_suggestions":patches,"qa_validation_passed":passed,"trace":trace,"note":"This is a rule-based demo, not a complete security scanner. Never execute untrusted patches automatically."}
open("results/p8_devsecops_audit.json","w").write(json.dumps(out,indent=2)); print(json.dumps(out,indent=2))
