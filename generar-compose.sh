#!/bin/bash

# Validate input parameters
if [ "$#" -ne 2 ]; then
    echo "Uso: $0 <nombre_del_archivo> <cantidad_de_clientes>"
    exit 1
fi

OUTPUT_FILE=$1
CLIENT_COUNT=$2

# Create the Docker Compose file content
cat <<EOL > $OUTPUT_FILE
name: tp0
services:
  server:
    container_name: server
    image: server:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
    volumes:
      - ./server/config.ini:/config.ini
    networks:
      - testing_net
EOL

for i in $(seq 1 $CLIENT_COUNT); do
    cat <<EOL >> $OUTPUT_FILE
  client$i:
    container_name: client$i
    image: client:latest
    entrypoint: /client
    environment:
      - CLI_ID=$i
      - CLI_NAME="Theo"
      - CLI_SURNAME="Miguel"
      - CLI_DOCUMENT="44556677"
      - CLI_BIRTHDATE="2000-06-02"
      - CLI_NUMBER="1234567890"
    volumes:
      - ./client/config.yaml:/config.yaml
    networks:
      - testing_net
    depends_on:
      - server
EOL
done

cat <<EOL >> $OUTPUT_FILE
networks:
  testing_net:
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24
EOL

