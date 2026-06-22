#!/bin/bash
set -e

echo "🚀 Deploying BSKYALGO..."

# Pull latest code
git pull origin main

# Rebuild images and apply updates (Docker automatically restarts only changed containers)
sudo docker compose up --build -d --remove-orphans

echo "✅ Deploy complete. Container status:"
sudo docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'

# Re-publish feed generator record to Bluesky to avoid "could not resolve identity" errors
echo ""
echo "📡 Re-publishing feed generator record to Bluesky..."
sleep 3 # Give the API a moment to fully start
sudo docker exec bsky-api yarn ts-node scripts/publishFeedNonInteractive.ts
echo "✅ Feed generator record published."

# Clean up dangling images and build cache to prevent disk full errors
echo "🧹 Cleaning up old Docker images..."
sudo docker system prune -f
echo "✅ Cleanup complete."
