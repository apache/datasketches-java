#!/bin/bash
set -e

TAG_NAME="test"
# Build and Generate Javadoc
# POM is configured to output to target/site/apidocs
echo "ECHO: mvn clean javadoc:javadoc"
mvn clean javadoc:javadoc

echo "ECHO: git fetch origin gh-pages"
git fetch origin gh-pages

echo "ECHO: Create worktree"
git worktree add -B gh-pages ./gh-pages-dir origin/gh-pages
EXIT_CODE=0
(
  set -e
  TARGET_PATH="gh-pages-dir/docs/$TAG_NAME"
  mkdir -p "$TARGET_PATH"
  cp -a target/site/apidocs/. "$TARGET_PATH/"
  cd gh-pages-dir
  
  echo "ECHO: git pull origin gh-pages --rebase"
  git pull origin gh-pages --rebase

  echo "ECHO: git add docs/$TAG_NAME"
  git add "docs/$TAG_NAME"
  
  if git diff --staged --quiet; then
    echo "ECHO: No changes detected for Javadoc $TAG_NAME."
  else
    echo "ECHO: Changes detected for Javadoc $TAG_NAME."
    echo "ECHO: git status:"
    git status
    echo "ECHO: git commit ..."
    git commit -m "Manual Javadoc deployment for tag $TAG_NAME"
    echo "ECHO: git push origin gh-pages"
    git push origin gh-pages
  fi
) || EXIT_CODE=$?

# Cleanup
echo "ECHO: Cleaning up worktree..."
git worktree remove --force ./gh-pages-dir || true

# Check the exit code and report success or failure
if [ $EXIT_CODE -eq 0 ]; then
  echo "ECHO: Javadoc for $TAG_NAME is now live on gh-pages."
else
  echo "ECHO: Javadoc deployment failed for $TAG_NAME."
fi

# Final exit
exit $EXIT_CODE