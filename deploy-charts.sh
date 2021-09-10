#!/usr/bin/env bash

helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo add cetic https://cetic.github.io/helm-charts
helm repo add ingress-nginx https://kubernetes.github.io/ingress-nginx

helm repo update

helm install --version v7.3.0 -f helm-config/minio-helm-values.yaml minio bitnami/minio
helm install --version v15.3.2 -f helm-config-minikube/redis-helm-values.yaml redis bitnami/redis
helm install --version v8.22.0 -f helm-config-minikube/rabbitmq-helm-values.yaml rabbitmq bitnami/rabbitmq
helm install --version v4.0.1 -f helm-config/nginx-helm-values.yaml nginx ingress-nginx/ingress-nginx
helm install --version v0.2.1 -f helm-config-minikube/postgres-helm-values.yaml postgres cetic/postgresql