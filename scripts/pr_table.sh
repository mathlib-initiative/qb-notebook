#!/usr/bin/env bash

# Generate a report of GitHub pull requests for the Mathlib Initiative weekly meeting.

# MacOS support
shopt -s expand_aliases
alias date="gdate"

# Get anchor date (default: today)
anchor="${1:-$(date +%Y-%m-%d)}"

# Find the most recent Sunday before or on anchor date
# Then calculate the week ranges
anchor_dow=$(date -d "$anchor" +%u)  # 1=Monday, 7=Sunday

# Calculate days to go back to reach last Sunday
if [ "$anchor_dow" -eq 7 ]; then
    days_to_sunday=0
else
    days_to_sunday=$anchor_dow
fi

# End of past week (Sunday)
past_week_end=$(date -d "$anchor - $days_to_sunday days" +%Y-%m-%d)
# Start of past week (Monday)
past_week_start=$(date -d "$past_week_end - 6 days" +%Y-%m-%d)

# End of week before (Sunday before past_week_start)
week_before_end=$(date -d "$past_week_start - 1 day" +%Y-%m-%d)
# Start of week before (Monday)
week_before_start=$(date -d "$week_before_end - 6 days" +%Y-%m-%d)

# Count commits (using day after end date for --until since it's exclusive)
past_week_until=$(date -d "$past_week_end + 1 day" +%Y-%m-%d)
week_before_until=$(date -d "$week_before_end + 1 day" +%Y-%m-%d)

# Past week counts
past_feat=$(git log --oneline --since="$past_week_start" --until="$past_week_until" | grep -c "^[a-f0-9]* feat" || true)
past_total=$(git log --oneline --since="$past_week_start" --until="$past_week_until" | wc -l)
past_nonfeat=$((past_total - past_feat))
past_closed=$(gh pr list --search "is:pr created:<$past_week_start closed:>$past_week_start" --limit 10000 --json id --jq length)
past_open=$(gh pr list --search "is:pr created:<$past_week_start is:open" --limit 10000 --json id --jq length)
past_prs=$((past_closed + past_open))

# Week before counts
before_feat=$(git log --oneline --since="$week_before_start" --until="$week_before_until" | grep -c "^[a-f0-9]* feat" || true)
before_total=$(git log --oneline --since="$week_before_start" --until="$week_before_until" | wc -l)
before_nonfeat=$((before_total - before_feat))
before_closed=$(gh pr list --search "is:pr created:<$week_before_start closed:>$week_before_start" --limit 10000 --json id --jq length)
before_open=$(gh pr list --search "is:pr created:<$week_before_start is:open" --limit 10000 --json id --jq length)
before_prs=$((before_closed + before_open))

# Calculate deltas
delta_feat=$((past_feat - before_feat))
delta_nonfeat=$((past_nonfeat - before_nonfeat))
delta_prs=$((past_prs - before_prs))

# Format deltas with sign
format_delta() {
    if [ "$1" -gt 0 ]; then
        echo "+$1"
    else
        echo "$1"
    fi
}

# Print header
echo "Week before: $week_before_start to $week_before_end"
echo "Past week:   $past_week_start to $past_week_end"
echo ""

# Print table
printf "%-15s | %11s | %11s | %5s\n" "" "past week" "week before" "delta"
# Doesn't work anymore, GH API only returns 1000 most recent PRs.
# printf "%-15s | %11d | %11d | %5s\n" "open PRs" "$past_prs" "$before_prs" "$(format_delta $delta_prs)"
printf "%-15s | %11d | %11d | %5s\n" "feat PRs" "$past_feat" "$before_feat" "$(format_delta $delta_feat)"
printf "%-15s | %11d | %11d | %5s\n" "non-feat PRs" "$past_nonfeat" "$before_nonfeat" "$(format_delta $delta_nonfeat)"
