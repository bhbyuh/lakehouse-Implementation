
FROM apache/spark:4.1.0 as jupyter-local
USER root
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
WORKDIR /app
EXPOSE 8888