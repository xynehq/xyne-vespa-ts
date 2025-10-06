#!/bin/bash
# publish.sh
# Usage: ./publish.sh [patch|minor|major]
# Example: ./publish.sh patch

set -e 

# --- Validate input ---
if [ -z "$1" ]; then
  echo "Error: No version type provided."
  echo "Usage: ./publish.sh [patch|minor|major]"
  exit 1
fi

VERSION_TYPE=$1

if [[ "$VERSION_TYPE" != "patch" && "$VERSION_TYPE" != "minor" && "$VERSION_TYPE" != "major" ]]; then
  echo "Invalid version type: '$VERSION_TYPE'"
  echo "Please use one of: patch, minor, major"
  exit 1
fi

# --- Check for uncommitted changes ---
echo "Checking for uncommitted changes..."
if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "You have uncommitted changes. Please commit or stash them before publishing."
  exit 1
fi

# --- Bump version ---
echo "Bumping $VERSION_TYPE version..."
npm version $VERSION_TYPE

# --- Push changes ---
echo "Pushing commits and tags to origin/main..."
git push origin main --tags

# --- Publish package ---
echo "Publishing package to npm (public access)..."
npm publish --access public

echo "Publish completed successfully!"