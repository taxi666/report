# FastAPI Tracking API POC

## Run

```bash
.\.venv\Scripts\python.exe -m uvicorn app.main:app --reload
```

## Example

```bash
curl "http://127.0.0.1:8000/report?prod=pc_his&from=pc_web&json=1"
```

Response:

```json
{"status":"ok"}
```

## PowerShell

```powershell
.\run_uvicorn.ps1
```

## Local Kafka

Start the local broker after the workspace-local Java and Kafka binaries are downloaded:

```powershell
.\scripts\start_local_kafka.ps1
.\scripts\create_tracking_topic.ps1
.\.venv\Scripts\python.exe .\scripts\verify_kafka_delivery.py
```
