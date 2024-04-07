docker build --platform=linux/amd64 -t imperialswemlsspring2024.azurecr.io/coursework5-peace .
docker push imperialswemlsspring2024.azurecr.io/coursework5-peace
kubectl --namespace=peace delete deployment aki-detection
kubectl apply -f kubernetes.yaml