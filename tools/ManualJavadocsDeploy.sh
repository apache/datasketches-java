#!/bin/bash
set -e

TAG_NAME="test"
# Build and Generate Javadoc
# POM is configured to output to target/site/apidocs
echo "mvn clean javadoc:javadoc"
mvn clean javadoc:javadoc

echo "git fetch origin gh-pages"
git fetch origin gh-pages

echo "Create worktree"
git worktree add ./gh-pages-dir origin/gh-pages
EXIT_CODE=0
(
  set -e
  TARGET_PATH="gh-pages-dir/docs/$TAG_NAME"
  mkdir -p "$TARGET_PATH"
  cp -a target/site/apidocs/. "$TARGET_PATH/"
  cd gh-pages-dir
  git add "docs/$TAG_NAME"
  
  if git diff --staged --quiet; then
    echo "No changes detected for Javadoc $TAG_NAME."
  else
    echo "Changes detected for Javadoc $TAG_NAME."
    git status
    git commit -m "Manual Javadoc deployment for tag $TAG_NAME"
    git push origin gh-pages
  fi
) || EXIT_CODE=$?

# Cleanup
echo "Cleaning up worktree..."
git worktree remove --force ./gh-pages-dir || true

# Final exit based on subshell success
exit $EXIT_CODE
if: success()
run: echo "Javadoc for $TAG_NAME is now live on gh-pages."