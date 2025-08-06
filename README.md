This Shell Script will move folders/files to Google Cloud Storage...

# 🚀 GCS Enterprise Transfer Tool

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Shell Script](https://img.shields.io/badge/Shell-Bash-green.svg)](https://www.gnu.org/software/bash/)
[![Platform](https://img.shields.io/badge/Platform-Linux%20%7C%20Ubuntu%20%7C%20CentOS%20%7C%20Debian-blue.svg)](#supported-platforms)
[![Version](https://img.shields.io/badge/Version-3.0.0-brightgreen.svg)](#changelog)

> **Enterprise-grade GCS file transfer script. Built for speed, resiliency, and automation in production environments.**

This Bash-based CLI tool enables fast, reliable, and monitored transfers to Google Cloud Storage using `gsutil` and `gcloud`. Designed for sysadmins, data engineers, and DevOps teams needing robust, resumable, and secure uploads with live feedback and alerting.

---

## ✅ Features

- 🔄 Auto-resume, retry logic, and SHA256 integrity checks
- ⚡ Parallel uploads (default: 100 concurrent)
- 📤 Filters by file size, extension, or modified date
- 📈 Real-time logs, metrics, and webhook/email alerts
- 🔒 Secure service account authentication (JSON or ADC)
- 🧰 Compatible with any Linux distribution (Bash ≥ 4.2)
- 🧪 Dry-run support, optional zipping, chunking & batching
- 🔧 Easily customizable `.env` for automation

---

## 🚀 Quick Start

```bash
# Clone the repo
git clone https://github.com/muzahidulislamhadi/server-to-gcs
.git
cd server-to-gcs

# Make script executable
chmod +x server-to-gcs.sh

# Configure environment variables
cp .env.sample .env
vim .env  # Set PROJECT_ID, BUCKET_NAME, SOURCE_PATH, etc.

# Run in automated mode
./gcs_enterprise_transfer.sh --automated
