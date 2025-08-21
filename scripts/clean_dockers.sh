#!/bin/bash

echo "Stopping all running containers..."
docker stop $(docker ps -q)

echo "Removing all containers..."
docker rm $(docker ps -aq)

echo "Pruning unused images, networks, and volumes..."
docker system prune -a --volumes -f

echo " All containers stopped, removed, and system pruned."
