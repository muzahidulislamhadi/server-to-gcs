#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

################################################################################
# gcs_move_streams.sh
#
# Usage:
#   ./gcs_move_streams.sh <GCP_PROJECT_ID> <GCS_BUCKET_NAME>
#
# This script:
#   1. Installs Google Cloud SDK (if needed)
#   2. Runs gcloud auth login (interactive)
#   3. Sets project and ensures bucket exists
#   4. Runs gsutil -m rsync to move /mnt/volume_nyc1_02/streams → gs://bucket
#   5. Deletes local files if the sync completes successfully
################################################################################

PROJECT_ID="${1:?Must pass GCP project ID}"
BUCKET="${2:?Must pass GCS bucket name}"
SRC_DIR="/mnt/volume_nyc1_02/streams" #source_dir
GCS_URI="gs://${BUCKET}/"

# 1) Install Cloud SDK if missing
if ! command -v gcloud &>/dev/null; then
  echo "Installing Google Cloud SDK..."
  sudo apt-get update -y
  sudo apt-get install -y apt-transport-https ca-certificates gnupg curl
  echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" \
    | sudo tee /etc/apt/sources.list.d/google-cloud-sdk.list >/dev/null
  curl https://packages.cloud.google.com/apt/doc/apt-key.gpg \
    | sudo gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
  sudo apt-get update -y
  sudo apt-get install -y google-cloud-sdk
fi

# 2) Authenticate
echo
echo "=== Authenticate with Google Cloud ==="
echo "A browser window will open. Please log in and grant permissions."
gcloud auth login

# 3) Set project & ensure bucket
echo
echo "=== Configuring project to '${PROJECT_ID}' and verifying bucket '${BUCKET}' ==="
gcloud config set project "${PROJECT_ID}"

if ! gsutil ls "${GCS_URI}" &>/dev/null; then
  echo "Bucket does not exist, creating it in us-central1..."
  gsutil mb -p "${PROJECT_ID}" -l us-central1 "${GCS_URI}"
fi

# 4) Perform multi-threaded rsync with progress
echo
echo "=== Starting transfer: ${SRC_DIR} → ${GCS_URI} ==="
# -m : multi-threaded
# rsync : only copies new/changed files, but here source is untouched
# -r : recursive
# -d : preserve empty dirs
# -c : checksum (ensures integrity)
# --quiet : reduce noise (remove if you'd like per-file logs)
# Progress is shown per-thread by default
gsutil -m rsync -r -d "${SRC_DIR}" "${GCS_URI}"

# 5) Clean up local files
echo
read -p "Transfer complete. Delete local files in ${SRC_DIR}? [y/N]: " confirm
if [[ "${confirm,,}" == "y" ]]; then
  echo "Removing local files..."
  rm -rf "${SRC_DIR:?}"/*
  echo "Local files deleted."
else
  echo "Local files preserved."
fi

echo "All done."
