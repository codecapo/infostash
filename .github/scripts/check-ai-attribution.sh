#!/usr/bin/env bash
# Fails if text carries AI attribution: co-author trailers for AI tools or
# "generated with" footers. The person who submits a change is its author
# and has reviewed it; see "Using AI tools" in CONTRIBUTING.md.
#
# Reads the files given as arguments, or stdin. Works as a commit-msg hook:
#   ln -s ../../.github/scripts/check-ai-attribution.sh .git/hooks/commit-msg
set -euo pipefail

tools='claude|anthropic|openai|chatgpt|gpt-[0-9]|codex|copilot|cursor|gemini|devin|aider|windsurf|codeium'
patterns=(
  "^[[:space:]]*co-authored-by:.*(${tools})"
  "generated (with|by) .*(${tools})"
  "noreply@anthropic\.com"
)

text=$(cat "$@")
found=0
for p in "${patterns[@]}"; do
  if matches=$(grep -inE "$p" <<<"$text"); then
    echo "$matches"
    found=1
  fi
done

if [[ $found -eq 1 ]]; then
  echo "error: remove the AI attribution lines above." >&2
  echo "You are the author of what you submit; see 'Using AI tools' in CONTRIBUTING.md." >&2
  exit 1
fi
