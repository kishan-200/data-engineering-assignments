# React + Vite

This template provides a minimal setup to get React working in Vite with HMR and some Oxlint rules.

Currently, two official plugins are available:

* [@vitejs/plugin-react](https://github.com/vitejs/vite-plugin-react/blob/main/packages/plugin-react) uses [Oxc](https://oxc.rs)
* [@vitejs/plugin-react-swc](https://github.com/vitejs/vite-plugin-react/blob/main/packages/plugin-react-swc) uses [SWC](https://swc.rs/)

## React Compiler

The React Compiler is not enabled on this template because of its impact on dev \& build performances. To add it, see [this documentation](https://react.dev/learn/react-compiler/installation).

## Expanding the Oxlint configuration

If you are developing a production application, we recommend using TypeScript with type-aware lint rules enabled. Check out the [TS template](https://github.com/vitejs/vite/tree/main/packages/create-vite/template-react-ts) for information on how to integrate TypeScript and Oxlint's TypeScript related rules in your project.

\## Running the Dashboard



Start the backend services first.



1\. Start HDFS, HBase and HBase Thrift.

2\. Start the FastAPI backend on port 8000.

3\. Start the React frontend on port 5173.



\### Backend



```bash

cd \~/omnichannel-inventory

source .ui-venv/bin/activate

python -m uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload

```



\### Frontend



```bash

cd frontend-react

npm install

npm run dev

```



Open the dashboard at http://localhost:5173.

