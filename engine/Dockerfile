# Build  COMA and its dependancies from source
FROM openjdk:8-slim-bullseye AS build-stage-COMA

RUN apt-get update && apt-get -y --no-install-recommends install git patch dos2unix maven

WORKDIR /usr/local

COPY ./algorithms/coma/valentine.patch /usr/local

RUN git clone https://github.com/kPsarakis/COMA \
    && find . -type f -exec dos2unix {} \; \
    && patch -ruN -p1 -d  COMA < /usr/local/valentine.patch \
    && mvn -f COMA/pom.xml clean \
    && mvn -f COMA/pom.xml -Dmaven.test.skip=true package \
    && mv COMA/coma-engine/target/coma-engine-0.1-CE-SNAPSHOT-jar-with-dependencies.jar /usr/local/coma.jar

# Python dependancies builder
FROM python:3.9-slim-bullseye AS builder

RUN apt-get update && apt-get install -y --no-install-recommends gcc g++

COPY requirements.txt /var/local/schema-matching-system/

RUN pip3 install --upgrade pip \
    && pip3 install --prefix=/usr/local -r /var/local/schema-matching-system/requirements.txt \
    && python -m nltk.downloader -d /usr/local/share/nltk_data stopwords punkt wordnet

# Clean Python 3.9 env
FROM python:3.9-slim-bullseye AS clean-Python

# Create the final service
FROM openjdk:8-slim-bullseye
COPY --from=clean-Python / /

COPY --from=builder /usr/local/ /usr/local

RUN mkdir /usr/local/schema-matching-system \
    && groupadd flask \
    && useradd -m -d /usr/local/schema-matching-system/engine -g flask flask

WORKDIR /usr/local/schema-matching-system/engine

COPY --chown=flask . .

RUN mkdir algorithms/coma/artifact  \
    && chown flask:flask algorithms/coma/artifact

COPY --chown=flask --from=build-stage-COMA /usr/local/coma.jar algorithms/coma/artifact

ENV PYTHONPATH /usr/local/schema-matching-system

EXPOSE 5000

USER flask

CMD gunicorn -b 0.0.0.0:5000 app:app
