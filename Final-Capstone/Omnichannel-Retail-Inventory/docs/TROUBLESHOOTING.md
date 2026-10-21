\# Troubleshooting Guide



\## 1. HBase Inventory Lookup Fails



Check whether HBase Thrift is listening on port 9090.



On Linux:



```bash

ss -ltnp | grep 9090

```



Ensure HBase Master, RegionServer, and Thrift services are running.



\## 2. FastAPI Backend Not Reachable



Check:



http://localhost:8000/api/health



Start the backend if necessary:



```bash

python -m uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload

```



\## 3. React Dashboard Not Loading



Navigate to the frontend directory and run:



```bash

npm install

npm run dev

```



Open:



http://localhost:5173



\## 4. AI Assistant Not Responding



Verify that:



\- FastAPI is running.

\- The OpenAI API key is configured in the backend environment.

\- The frontend is connecting to the correct API endpoint.

\- The backend logs do not show authentication or connection errors.



Never expose API keys in React source files.



\## 5. Kafka Streaming Issues



Verify that:



\- The Kafka broker is running.

\- The expected Kafka topics exist.

\- The producer is publishing messages.

\- The Spark Structured Streaming consumer is running.



\## 6. Spark Processing Failures



Check:



\- Spark application logs

\- Input data paths

\- Checkpoint directory permissions

\- Executor memory availability

\- HDFS connectivity



\## 7. Inventory Service Unavailable



Ensure the following services are operational:



\- HDFS

\- HBase

\- HBase Thrift

\- FastAPI



\## 8. Final Verification



After resolving issues, confirm that the dashboard, inventory lookup, AI assistant, and backend APIs work as expected.



