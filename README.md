# wsrs-acquisition

WSR download + parse pipeline for **Atlas acquisition stores** (Kerr-McCauley and MikLin/Mulligan groups).

Runs independently from the main `wsrs` Atlas pipeline. Uses the same Macromatix portal and Supabase tables — stores are differentiated by store number.

---

## Repo structure

```
wsrs-acquisition/
├── wsr_download_acquisition.py        # Shared downloader — profile-aware (km / mm)
├── wsr_orchestrator_kerrmccauley.py   # Full pipeline orchestrator — Kerr-McCauley
├── wsr_orchestrator_miklim.py         # Full pipeline orchestrator — MikLin/Mulligan
├── process_wsr_ENHANCED.py            # Parser (copy from wsrs main repo — no changes needed)
├── requirements.txt                   # Same as wsrs main repo
└── .github/workflows/
    ├── wsr_pipeline_kerrmccauley.yml
    └── wsr_pipeline_miklim.yml
```

> **Note**: Copy `process_wsr_ENHANCED.py` and `requirements.txt` directly from the main `wsrs` repo — they need no modifications.

---

## GitHub Secrets required

Set these under **Settings → Secrets and variables → Actions** in this repo:

| Secret | Description |
|---|---|
| `KM_SITE_USERNAME` | Kerr-McCauley Macromatix username |
| `KM_SITE_PASSWORD` | Kerr-McCauley Macromatix password |
| `MM_SITE_USERNAME` | MikLin/Mulligan Macromatix username |
| `MM_SITE_PASSWORD` | MikLin/Mulligan Macromatix password |
| `SUPABASE_URL` | Same Supabase project as Atlas main |
| `SUPABASE_KEY` | Same Supabase project as Atlas main |
| `SLACK_WEB_HOOK_URL` | Slack webhook for notifications |
| `SENDER_EMAIL` | SMTP sender (for parser email alerts) |
| `SENDER_PASSWORD` | SMTP password |
| `RECIPIENT_EMAILS` | Comma-separated recipient list |
| `SMTP_SERVER` | e.g. `smtp.gmail.com` |
| `SMTP_PORT` | e.g. `587` |

---

## How profiles work

The shared download script (`wsr_download_acquisition.py`) uses a `WSR_PROFILE` environment variable to select credentials and directories:

| Profile | Credentials | Download dir | Browser data dir |
|---|---|---|---|
| `km` | `KM_SITE_USERNAME` / `KM_SITE_PASSWORD` | `wsr_downloads_km/` | `browser_data_km/` |
| `mm` | `MM_SITE_USERNAME` / `MM_SITE_PASSWORD` | `wsr_downloads_mm/` | `browser_data_mm/` |

Each orchestrator sets `os.environ['WSR_PROFILE']` **before** importing the downloader, so credentials resolve at import time. The workflows also set `WSR_PROFILE` as an env var for redundancy.

---

## Running locally

```bash
# Kerr-McCauley — automatic week
python wsr_orchestrator_kerrmccauley.py

# Kerr-McCauley — manual week override
python wsr_orchestrator_kerrmccauley.py --week 20 --year 2026

# MikLin/Mulligan — automatic week
python wsr_orchestrator_miklim.py

# Download only (no parse/audit), specific profile
python wsr_download_acquisition.py --profile km
python wsr_download_acquisition.py --profile mm --week 20 --year 2026
```

Copy a `.env` file with the relevant secrets before running locally.

---

## Data destination

Parsed data lands in the **same Supabase tables** as the Atlas main pipeline (`wsr_sales`, `wsr_labor`, `wsr_financial`, `wsr_inventory`). Acquisition stores are differentiated by their store numbers — no table changes needed.
