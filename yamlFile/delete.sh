#!/bin/bash

kubectl delete -f flink-configuration-configmap.yaml
kubectl delete -f jobmanager-service.yaml
kubectl delete -f jobmanager-deployment.yaml
kubectl delete -f taskmanager-deployment.yaml
