#!/bin/bash
# Download only the parquet files for a given year from the supercomputer.
# Usage: ./download_year.sh [YEAR]     (defaults to the current year)

USERNAME="njpres3"
REMOTE_HOST="ssh.rc.byu.edu"
REMOTE_DIR="groups/grp_quant/database/production"
LOCAL_DIR="/Users/nathanpreslar/groups/grp_quant/database/production"

YEAR="${1:-$(date +%Y)}"

# Basic sanity check on the year argument
if ! [[ "$YEAR" =~ ^[0-9]{4}$ ]]; then
    echo "Error: YEAR must be a 4-digit year (got '$YEAR')"
    exit 1
fi

mkdir -p "$LOCAL_DIR"

echo "Downloading *_${YEAR}.parquet files from $REMOTE_HOST:$REMOTE_DIR to $LOCAL_DIR"

# --include='*/'                  -> descend into every subfolder (assets, factors, exposures, ...)
# --include="*_${YEAR}.parquet"   -> keep only files for the chosen year
# --exclude='*'                   -> skip everything else
# --prune-empty-dirs              -> don't create local folders that end up with no matches
rsync -av --prune-empty-dirs \
    --include='*/' \
    --include="*_${YEAR}.parquet" \
    --exclude='*' \
    "$USERNAME@$REMOTE_HOST:$REMOTE_DIR/" "$LOCAL_DIR/"

if [ $? -eq 0 ]; then
    echo "Download of $YEAR files completed successfully"
else
    echo "Error occurred during download"
    exit 1
fi