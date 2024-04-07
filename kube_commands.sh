az login

az account set --subscription 4693832c-ac40-4623-80b9-79a0345fcfce

az acr login --name imperialswemlsspring2024
az aks get-credentials --resource-group imperial-swemls-spring-2024 --name imperial-swemls-spring-2024 --overwrite-existing

kubelogin convert-kubeconfig -l azurecli

kubectl --namespace=peace get pods
docker build --platform=linux/amd64 -t imperialswemlsspring2024.azurecr.io/coursework5-peace .
docker push imperialswemlsspring2024.azurecr.io/coursework5-peace

kubectl apply -f kubernetes.yaml

#For checking the prometheus metrics, open another terminal tab and paste the following command. Instead of aki-detection-66f7857596-98gr9 use the current pod name. Then open localhost:8000
kubectl -n peace port-forward aki-detection-66f7857596-98gr9 8000:8000
kubectl --namespace=peace get deployments

kubectl logs --namespace=peace -l app=aki-detection

# kubectl --namespace=peace delete deployment aki-detection
kubectl --namespace=peace get pods

kubectl -n peace exec --stdin --tty aki-detection-c8fb5848d-t9xm7 -- /bin/bash

kubectl -n peace delete deployment aki-detection 
kubectl -n peace delete pvc aki-detection-state
