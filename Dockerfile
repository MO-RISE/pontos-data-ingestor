FROM python:3.11-bullseye as builder

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip3 install --no-cache -r requirements.txt

COPY main.py main.py
COPY streamz_nodes.py streamz_nodes.py

CMD ["python3", "main.py"]