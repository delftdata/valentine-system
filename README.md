# Valentine in Action: Matching Tabular Data at Scale

This repository contains the system implementation of [Valentine](https://delftdata.github.io/valentine/) 
with the addition of a holistic schema matching element. 

![](https://user-images.githubusercontent.com/23175739/111807004-34cd0980-88db-11eb-9dd7-599d3988c262.jpg)

## Install

### Install using docker-compose

To install docker-compose follow the [official instructions](https://docs.docker.com/compose/install/).
The next step is to build the required containers (client, engine) by running: 

```shell
docker-compose build
```

> NOTE: For easier setup of Minio create a folder in the projects root named `minio-volume` and add folders (buckets) 
> with the data you like in there to instantly load them to the system.

### Install in minikube

To install minikube follow the [official instructions](https://minikube.sigs.k8s.io/docs/start/).

> NOTE: You will also need to enable the ingress addon by running: `minikube addons enable ingress`

At first, you have two options, either pull the two required images (client, engine) build by us:

```shell
docker pull kpsarakis/schema-matching-engine:latest
docker pull kpsarakis/schema-matching-client:latest
```

or build them yourself by running

```shell
docker build -t kpsarakis/schema-matching-engine:latest ./engine
docker build -t kpsarakis/schema-matching-client:latest --build-arg REACT_APP_SERVER_ADDRESS=/api ./client
```

Then you need to install Helm by following the [official instructions](https://helm.sh/docs/intro/install/). 
Once installed run the following commands to deploy a Redis cluster, a Minio cluster and RAbbitMQ 
(tune their parameters to your liking by going in the `helm-config` folder in each respective file):

```shell
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update

helm install -f helm-config/minio-helm-values.yaml minio bitnami/minio
helm install -f helm-config/redis-helm-values.yaml redis bitnami/redis
helm install -f helm-config/rabbitmq-helm-values.yaml rabbitmq bitnami/rabbitmq
```

The final step is to deploy the client, server, celery worker, flower (celery cluster monitoring), and the ingress 
service. At first change the configurations of those deployments in the `k8s` folder (or leave the defaults) and then 
run the following command to apply them all:

```shell
cd k8s/
kubectl apply -f .
```

### Install in a kubernetes cluster 
Assuming that you have created a kubernetes cluster somewhere and have the `kubectl` command configured go in the 
`helm-config` and `k8s` folders to change the deployment configurations to match your use-case then run:

```shell
./deploy-charts.sh
cd k8s/
kubectl apply -f .
```

This will create all the system's components within the cluster and also add a nginx load balancer to handle incoming 
traffic.

## Run

### Run with docker-compose

To run the system with docker compose:

```shell
docker-compose up
```

then go to:

*   `localhost:3000` to access the UI.
*   `localhost:5555` to access the celery cluster monitoring tool Flower.
*   `localhost:5000` to access the systems api.
*   `localhost:9000` to access Minio.
*   `localhost:15672` to access RabbitMQ.

### Run with  minikube

To access the system deployed with Minikube at first get the IP by running:

```shell
minikube ip
```

then you can access the UI by going to that address, and the api by writing that address with the `/api` suffix.

If you want to access a specific service run the following command:

```shell
minikube service $(SERVICE_NAME)
```

e.g. for the flower service:

```shell
minikube service flower-service
```

> NOTE: The names can be found by running `kubectl get svc`

> NOTE: To access the services deployed by Helm use the instructions given after their deployment.

### Run with a kubernetes cluster 

The cluster case is similar to Minikube, you have to get the external IP of the nginx load balancer instead of 
Minikube's and access the UI and api in the same way. For the rest of the services follow either the Helm or 
your providers port-forwarding instructions.

## Repo structure

*   [`client`](https://github.com/kPsarakis/holistic-schema-matching-at-scale/tree/master/client) 
    Module containing the React implementation of the system's UI.
    
*   [`engine`](https://github.com/kPsarakis/holistic-schema-matching-at-scale/tree/master/engine) 
    Module containing the schema matching engine and the backend of the system. 
    
*   [`env_files`](https://github.com/kPsarakis/holistic-schema-matching-at-scale/tree/master/env_files) 
    Folder containing example env files for the docker-compose. 
    
*   [`helm-config`](https://github.com/delftdata/valentine-suite/tree/master/helm-config) 
    Folder containing the configuration of the redis, rabbitmq and ingress-nginx charts. 
    
*   [`k8s`](https://github.com/delftdata/valentine-suite/tree/master/k8s) 
    Folder containing the kubernetes deployments, apps and services for the client, server, celery worker, 
    flower (celery cluster monitoring), and the ingress service.
