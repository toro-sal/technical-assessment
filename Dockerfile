FROM apache/airflow:2.11.0

USER root
RUN apt-get update -y && apt-get install -y --no-install-recommends \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Asegura que /usr/local/bin y ~/.local/bin estén en PATH
ENV PATH="/home/airflow/.local/bin:/usr/local/bin:${PATH}"

USER airflow
ENV PIP_USER=false
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
ENV PIP_USER=true
