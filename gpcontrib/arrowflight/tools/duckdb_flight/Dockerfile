FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY tools/duckdb_flight/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY tools/duckdb_flight/duckdb_arrowflightd.py /app/duckdb_arrowflightd.py

EXPOSE 8815 8816

CMD ["python", "/app/duckdb_arrowflightd.py", "--host", "0.0.0.0", "--port", "8815", "--admin-host", "0.0.0.0", "--admin-port", "8816"]
