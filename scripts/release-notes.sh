#!/usr/bin/env bash
# release-notes.sh <tag> [previous-tag]
#
# Emits the release notes for a tag on stdout: the curated CHANGELOG section for
# that version, followed by the commits it actually contains.
#
# GitHub's own --generate-notes builds its list from merged PULL REQUESTS. This
# repo commits straight to main, so there were none to list and every release
# since v0.7.2 got a body consisting of one compare link and nothing else — the
# notes were not generated from commits, they were generated from nothing.
#
# Both halves matter. The changelog says why a release exists and what it was
# measured to do; the commit list says what is provably in it. Neither is a
# substitute for the other, and the commit list is the half that cannot drift,
# because git computes it.
set -euo pipefail

TAG="${1:?usage: release-notes.sh <tag> [previous-tag]}"
VERSION="${TAG#v}"
PREV="${2:-}"

if [ -z "$PREV" ]; then
  # The tag before this one, by version order rather than by date: releases get
  # cut out of order often enough that date order lies.
  PREV="$(git tag --list 'v*' --sort=-version:refname | grep -A1 -x -F "$TAG" | tail -n1 || true)"
  [ "$PREV" = "$TAG" ] && PREV=""
fi

# --- curated section from the changelog -------------------------------------
# Matches "## [0.8.8]" and range headings like "## [0.8.11] - [0.8.14]".
if [ -f CHANGELOG.md ]; then
  awk -v ver="$VERSION" '
    /^## \[/ {
      if (found) exit
      # Collect every version named in the heading, so a range heading is found
      # by each version it covers.
      line = $0
      n = 0
      while (match(line, /\[[0-9]+\.[0-9]+\.[0-9]+\]/)) {
        v = substr(line, RSTART + 1, RLENGTH - 2)
        if (v == ver) n = 1
        line = substr(line, RSTART + RLENGTH)
      }
      if (n) { found = 1; print; next }
    }
    found { print }
  ' CHANGELOG.md
fi

# --- what is provably in it --------------------------------------------------
echo
echo "### Commits"
echo
if [ -n "$PREV" ]; then
  git log --no-merges --pretty='- %s (`%h`)' "$PREV..$TAG"
  echo
  echo "**Full changelog**: https://github.com/redstone-md/moss/compare/${PREV}...${TAG}"
else
  git log --no-merges --pretty='- %s (`%h`)' "$TAG" | head -50
fi
