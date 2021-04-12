## README for the Demo App

#### To run on MAC
`export DOCKER_KAFKA_HOST=$(ipconfig getifaddr en0)`

`docker-compose -f docker-compose-project.yml up` # for kafka

`docker-compose up` # for api

#### Install java and run the  apps inside main/java
`mvn exec:java -Dexec.mainClass="myapps.Claim"`

####  Application Endpoints
1. Swagger: `localhost/docs`
2. Kafka Admin UI: `localhost:8080/ui`

#### Generate Kafka Streams Topology
Cut and paste the output of System.out.println(topology) to the following website

`https://zz85.github.io/kafka-streams-viz/`