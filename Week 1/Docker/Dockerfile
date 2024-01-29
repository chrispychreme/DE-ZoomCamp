FROM python:3.11

RUN apt install wget
RUN pip install pyarrow pandas sqlalchemy psycopg2

WORKDIR /app
COPY ingest-data.py ingest-data.py

ENTRYPOINT [ "python", "ingest-data.py" ]