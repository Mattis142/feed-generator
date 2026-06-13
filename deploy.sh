#!/bin/bash
set -e

echo "🚀 Deploying BSKYALGO..."

# Pull latest code
git pull origin main

# Rebuild images and apply updates (Docker automatically restarts only changed containers)
sudo docker compose up --build -d --remove-orphans

echo "✅ Deploy complete. Container status:"
sudo docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
