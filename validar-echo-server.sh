#!/bin/bash

# CONTAINER_ID=$(docker ps -qf "name=server")
# CONTAINER_IP=$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $CONTAINER_ID)

# echo "Container ID: $CONTAINER_ID"
# echo "Container IP: $CONTAINER_IP"

MESSAGE="Hola mundo"

# Run nc with docker
RESULT=$(docker run --rm --network tp0_testing_net alpine:latest sh -c "echo '$MESSAGE' | nc -w 10 server 12345")

# Verifica si el servidor está respondiendo
if [ "$RESULT" = "$MESSAGE" ]; then
    echo "action: test_echo_server | result: success"
else
    echo "action: test_echo_server | result: failed"
fi
