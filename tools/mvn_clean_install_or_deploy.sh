#!/usr/bin/env bash
set -e

# Default goal to 'install' if no argument provided
# Otherwise specify 'deploy', in which case it also adds: -DskipTests" -Pnexus-jars
GOAL="${1:-install}"
shift 1 2>/dev/null || true

# Set goal-specific defaults
EXTRA_ARGS=()
if [ "$GOAL" = "deploy" ]; then
  EXTRA_ARGS=("-DskipTests" "-Pnexus-jars")
fi

# Safe Git metadata extraction with fallbacks for non-git builds
if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  BRANCH=$(git rev-parse --abbrev-ref HEAD 2>/dev/null)
  [ "$BRANCH" = "HEAD" ] && BRANCH=$(git name-rev --name-only HEAD 2>/dev/null)

  COMMIT=$(git rev-parse HEAD 2>/dev/null || echo "unknown")
  TIME=$(git log -1 --format=%cI 2>/dev/null || echo "unknown")
  EMAIL=$(git config user.email 2>/dev/null)
  [ -z "$EMAIL" ] && EMAIL="unknown"
else
  BRANCH="source-release-no-git"
  COMMIT="unknown"
  TIME="unknown"
  EMAIL="unknown"
fi

# Execute Maven clean + target goal with injected system properties
exec mvn clean "$GOAL" \
  "${EXTRA_ARGS[@]}" \
  -Dgit.branch="${BRANCH:-source-release-no-git}" \
  -Dgit.commit.id.full="${COMMIT:-unknown}" \
  -Dgit.commit.time="${TIME:-unknown}" \
  -Dgit.build.user.email="${EMAIL:-unknown}" \
  "$@"
