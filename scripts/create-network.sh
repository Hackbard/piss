#!/bin/bash
# Create external Docker network for PISS project

NETWORK_NAME="piss-network"

if docker network ls | grep -q "$NETWORK_NAME"; then
    echo "Network '$NETWORK_NAME' already exists."
else
    echo "Creating external network '$NETWORK_NAME'..."
    docker network create "$NETWORK_NAME"
    echo "Network '$NETWORK_NAME' created successfully."
fi

echo "Network status:"
docker network inspect "$NETWORK_NAME" --format '{{.Name}}: {{.Scope}} ({{.Driver}})'




