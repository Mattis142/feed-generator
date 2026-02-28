#!/bin/bash
set -e

echo "🚀 Deploying BSKYALGO..."

# Pull latest code
git pull origin main

# Stop and remove old containers cleanly (avoids docker-compose v1 ContainerConfig bug)
sudo docker compose stop
sudo docker compose rm -f

# Rebuild images and start all services
sudo docker compose up --build -d

echo "✅ Deploy complete. Container status:"
sudo docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
