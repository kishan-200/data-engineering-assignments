import os,json,sqlite3
os.makedirs("results",exist_ok=True)
db=sqlite3.connect(":memory:")
db.execute("create table portfolio(customer text, balance real, tax_rate real)")
db.executemany("insert into portfolio values(?,?,?)",[("Customer X",125000,0.22),("Customer Y",72000,0.18)])
market_docs=["Q3 outlook: analysts expect elevated volatility and advise scenario analysis rather than guaranteed returns."]
trace=[]
def sql_tool(customer):
 trace.append({"tool":"sql_tool","input":customer})
 row=db.execute("select balance,tax_rate from portfolio where customer=?",(customer,)).fetchone()
 return {"balance":row[0],"tax_rate":row[1]} if row else None
def market_rag_tool(query):
 trace.append({"tool":"market_rag_tool","input":query}); return market_docs[0]
def calculator_tool(balance,tax_rate):
 trace.append({"tool":"calculator_tool","input":{"balance":balance,"tax_rate":tax_rate}})
 return balance*tax_rate
customer="Customer X"; portfolio=sql_tool(customer); outlook=market_rag_tool("Q3 outlook"); estimated_tax=calculator_tool(portfolio["balance"],portfolio["tax_rate"])
out={"customer":customer,"portfolio":portfolio,"estimated_tax_amount":estimated_tax,"market_context":outlook,"trace":trace,"disclaimer":"Illustrative calculation only; not financial or tax advice."}
open("results/p7_agent_trace.json","w").write(json.dumps(out,indent=2)); print(json.dumps(out,indent=2))
