FROM python:3.9-slim-bullseye

WORKDIR /home

# System deps
# python:3.9-slim-bullseye is Debian 11 — openjdk-11-jre-headless is available here.
# Do NOT use python:3.9-slim (no version tag) — it now resolves to Trixie (Debian 13)
# where Java 11 was removed. Always pin the Debian version for reproducibility.
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    openjdk-11-jre-headless \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for PySpark
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Python deps — pinned to match namenode conda env
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

ENV PYTHONPATH=/home/src
ENV PYTHONUNBUFFERED=1

EXPOSE 5050 8501