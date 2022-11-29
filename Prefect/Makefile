build-dev:
   prefect deployment build ./run_analytics.py:run --name dev -q prod-agent --tag dev-elt --output dev-deployment.yaml
deploy-dev: build-dev
   prefect deployment apply dev-deployment.yaml
build-prod:
   prefect deployment build ./run_analytics.py:run --name prod -q prod-agent --tag prod-elt --output prod-deployment.yaml
deploy-prod: build-prod
   prefect deployment apply prod-deployment.yaml
run-dev: deploy
   prefect deployment run Analytics_Flow/dev
