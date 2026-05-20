#!/usr/bin/env python3
"""
WSR Daily Orchestrator — Kerr-McCauley Acquisition Stores
Runs the complete WSR pipeline: download → parse → audit → notify

Processes BOTH current week AND prior week (same logic as Atlas main pipeline).

Profile: km (Kerr-McCauley)
Credentials: KM_SITE_USERNAME / KM_SITE_PASSWORD
Expected stores: KM_EXPECTED_STORES (set in GitHub Secrets)
Download dir: wsr_downloads_km/
"""

import os
import sys

# ---------------------------------------------------------------------------
# CRITICAL: Set profile BEFORE importing the downloader module so that all
# module-level globals in wsr_download_acquisition resolve to KM credentials.
# ---------------------------------------------------------------------------
os.environ['WSR_PROFILE'] = 'km'

import json
import asyncio
import subprocess
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import requests
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROFILE           = 'km'
PROFILE_LABEL     = 'Kerr-McCauley'
EXPECTED_STORES   = 4  # Kerr-McCauley group
SLACK_WEBHOOK_URL = os.getenv('SLACK_WEB_HOOK_URL')
DOWNLOAD_DIR      = Path('wsr_downloads_km')


class Colors:
    GREEN   = '\033[92m'
    YELLOW  = '\033[93m'
    RED     = '\033[91m'
    BLUE    = '\033[94m'
    CYAN    = '\033[96m'
    MAGENTA = '\033[95m'
    RESET   = '\033[0m'
    BOLD    = '\033[1m'


def fetch_all_stores_paginated(client, table_name: str, week_ending: str) -> set:
    """Fetch ALL unique store numbers using pagination (Supabase 1000-row limit)."""
    all_store_numbers = set()
    offset    = 0
    page_size = 1000

    while True:
        response = client.table(table_name)\
            .select('store_number')\
            .eq('week_ending', week_ending)\
            .range(offset, offset + page_size - 1)\
            .execute()

        if not response.data:
            break

        page_stores = set(row['store_number'] for row in response.data)
        all_store_numbers.update(page_stores)

        if len(response.data) < page_size:
            break

        offset += page_size

    return all_store_numbers


def calculate_prior_week(week: int, year: int) -> Tuple[int, int]:
    """Calculate prior week number and year, handling year boundaries."""
    if week == 1:
        return (52, year - 1)
    return (week - 1, year)


def get_current_week_from_date() -> Tuple[int, int]:
    """
    Calculate current JJ week number and year.

    Jimmy Johns weeks: Wednesday-Tuesday (week ends on Tuesday)
    - Wednesday: Use PRIOR week (ended yesterday, finalized overnight)
    - All other days: Use CURRENT week

    JJ fiscal weeks are offset by -1 from ISO weeks:
      ISO Week 2 = JJ Week 1, ISO Week 3 = JJ Week 2, etc.
    """
    today         = datetime.now()
    today_weekday = today.weekday()

    if today_weekday == 2:  # Wednesday
        week_end = today - timedelta(days=1)
    else:
        days_until_tuesday = (1 - today_weekday) % 7
        week_end = today if days_until_tuesday == 0 else today + timedelta(days=days_until_tuesday)

    iso_week    = week_end.isocalendar()[1]
    week_number = iso_week - 1
    year        = week_end.year

    if week_number == 0:
        week_number = 52
        year        = year - 1

    return week_number, year


class WSROrchestrator:
    def __init__(self, override_week: Optional[int] = None, override_year: Optional[int] = None):
        self.start_time    = datetime.now()
        self.override_week = override_week
        self.override_year = override_year
        self.results = {
            'current_week': {'download': None, 'parse': None, 'audit': None},
            'prior_week':   {'download': None, 'parse': None, 'audit': None},
            'errors': []
        }

    async def run_download_for_week(self, week: int, year: int, week_label: str) -> bool:
        """Run the WSR downloader for a specific week."""
        print(f"\n{Colors.BOLD}{'='*70}{Colors.RESET}")
        print(f"{Colors.BOLD}[{PROFILE_LABEL}] DOWNLOADING: {week_label}{Colors.RESET}")
        print(f"{Colors.CYAN}Week {week}, Year {year}{Colors.RESET}")
        print(f"{Colors.BOLD}{'='*70}{Colors.RESET}\n")

        try:
            # Import acquisition downloader (WSR_PROFILE already set to 'km')
            from wsr_download_acquisition import WSRDownloader
            import wsr_download_acquisition as downloader_module

            downloader_module.OVERRIDE_WEEK = week
            downloader_module.OVERRIDE_YEAR = year

            # Clear download directory
            if DOWNLOAD_DIR.exists():
                import shutil
                shutil.rmtree(DOWNLOAD_DIR)
                print(f"{Colors.YELLOW}[{PROFILE_LABEL}] Cleared download directory{Colors.RESET}")

            downloader = WSRDownloader()
            await downloader.run()

            if not DOWNLOAD_DIR.exists():
                raise Exception("Download directory not created")

            downloaded_files = list(DOWNLOAD_DIR.glob("*.xls*"))
            print(f"\n{Colors.GREEN}[{PROFILE_LABEL}] ✓ Download complete: {len(downloaded_files)} files{Colors.RESET}")
            return True

        except Exception as e:
            error_msg = f"[{PROFILE_LABEL}] Download failed for {week_label}: {str(e)}"
            print(f"\n{Colors.RED}✗ {error_msg}{Colors.RESET}")
            self.results['errors'].append(error_msg)
            return False

    def run_parser_for_week(self, week_label: str) -> bool:
        """Run the WSR parser for downloaded files."""
        print(f"\n{Colors.BOLD}{'='*70}{Colors.RESET}")
        print(f"{Colors.BOLD}[{PROFILE_LABEL}] PARSING: {week_label}{Colors.RESET}")
        print(f"{Colors.BOLD}{'='*70}{Colors.RESET}\n")

        try:
            cmd = [
                sys.executable,
                'process_wsr_ENHANCED.py',
                'process',
                str(DOWNLOAD_DIR),
                '--email'
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, check=False)

            if result.stdout:
                print(result.stdout)

            if result.returncode != 0:
                if result.stderr:
                    print(result.stderr)
                raise Exception(f"Parser exited with code {result.returncode}")

            print(f"\n{Colors.GREEN}[{PROFILE_LABEL}] ✓ Parsing complete for {week_label}{Colors.RESET}")
            return True

        except Exception as e:
            error_msg = f"[{PROFILE_LABEL}] Parsing failed for {week_label}: {str(e)}"
            print(f"\n{Colors.RED}✗ {error_msg}{Colors.RESET}")
            self.results['errors'].append(error_msg)
            return False

    def audit_database_for_week(self, week: int, year: int, week_label: str) -> Dict[str, Any]:
        """Audit the database to verify all stores have data for a specific week."""
        print(f"\n{Colors.BOLD}{'='*70}{Colors.RESET}")
        print(f"{Colors.BOLD}[{PROFILE_LABEL}] AUDITING: {week_label}{Colors.RESET}")
        print(f"{Colors.CYAN}Week {week}, Year {year}{Colors.RESET}")
        print(f"{Colors.BOLD}{'='*70}{Colors.RESET}\n")

        try:
            from supabase import create_client, Client

            supabase_url = os.getenv('SUPABASE_URL')
            supabase_key = os.getenv('SUPABASE_KEY')

            if not supabase_url or not supabase_key:
                raise Exception("Supabase credentials not found in environment")

            client: Client = create_client(supabase_url, supabase_key)

            year_start       = datetime(year, 1, 1)
            days_to_tuesday  = (1 - year_start.weekday()) % 7
            first_tuesday    = year_start + timedelta(days=days_to_tuesday)
            target_tuesday   = first_tuesday + timedelta(weeks=week - 1)
            week_ending      = target_tuesday.strftime('%Y-%m-%d')

            print(f"[{PROFILE_LABEL}] Checking data for week ending: {week_ending}")

            tables = ['wsr_sales', 'wsr_labor', 'wsr_financial', 'wsr_inventory']

            audit_results = {
                'week_ending':    week_ending,
                'tables':         {},
                'overall_passed': True
            }

            for table in tables:
                store_numbers = fetch_all_stores_paginated(client, table, week_ending)
                stores_found  = len(store_numbers)
                passed        = stores_found == EXPECTED_STORES

                audit_results['tables'][table] = {
                    'stores_found':  stores_found,
                    'expected':      EXPECTED_STORES,
                    'passed':        passed,
                    'missing_count': EXPECTED_STORES - stores_found
                }

                if not passed:
                    audit_results['overall_passed'] = False

                status = "✓" if passed else "✗"
                color  = Colors.GREEN if passed else Colors.YELLOW
                print(f"{color}[{PROFILE_LABEL}] {status} {table}: {stores_found}/{EXPECTED_STORES} stores{Colors.RESET}")

            return audit_results

        except Exception as e:
            error_msg = f"[{PROFILE_LABEL}] Audit failed for {week_label}: {str(e)}"
            print(f"\n{Colors.RED}✗ {error_msg}{Colors.RESET}")
            print(traceback.format_exc())
            self.results['errors'].append(error_msg)
            return {'error': error_msg, 'overall_passed': False}

    def send_slack_notification(self):
        """Send consolidated Slack notification with results from both weeks."""
        if not SLACK_WEBHOOK_URL:
            print(f"{Colors.YELLOW}⚠ No Slack webhook configured, skipping notification{Colors.RESET}")
            return

        print(f"\n{Colors.BOLD}Sending Slack notification...{Colors.RESET}")

        try:
            duration = (datetime.now() - self.start_time).total_seconds()

            current_week_ok = (
                self.results['current_week'].get('download', {}).get('success', False) and
                self.results['current_week'].get('parse',    {}).get('success', False)
            )
            prior_week_ok = (
                self.results['prior_week'].get('download', {}).get('success', False) and
                self.results['prior_week'].get('parse',    {}).get('success', False)
            )
            overall_ok = current_week_ok and prior_week_ok

            if overall_ok:
                emoji = ":white_check_mark:"
                color = "#36a64f"
                title = f"✓ [{PROFILE_LABEL}] WSR Pipeline Completed Successfully (Current + Prior Week)"
            elif current_week_ok or prior_week_ok:
                emoji = ":warning:"
                color = "#ff9900"
                title = f"⚠ [{PROFILE_LABEL}] WSR Pipeline Completed with Warnings"
            else:
                emoji = ":x:"
                color = "#ff0000"
                title = f"✗ [{PROFILE_LABEL}] WSR Pipeline Failed"

            fields = []

            # CURRENT WEEK
            fields.append({"title": "CURRENT WEEK", "value": "_" * 40, "short": False})

            current_download = self.results['current_week'].get('download', {})
            if current_download:
                if current_download.get('success'):
                    fields.append({"title": "1️⃣ Download (Current)", "value": f"✓ {current_download.get('files_downloaded', 0)} files downloaded", "short": True})
                else:
                    fields.append({"title": "1️⃣ Download (Current)", "value": f"✗ {current_download.get('error', 'Unknown error')}", "short": True})

            current_parse = self.results['current_week'].get('parse', {})
            if current_parse:
                if current_parse.get('success'):
                    fields.append({"title": "2️⃣ Parse (Current)", "value": "✓ Completed", "short": True})
                else:
                    fields.append({"title": "2️⃣ Parse (Current)", "value": f"✗ {current_parse.get('error', 'Unknown error')}", "short": True})

            current_audit = self.results['current_week'].get('audit')
            if current_audit:
                week_ending = current_audit.get('week_ending', 'Unknown')
                if current_audit.get('overall_passed'):
                    fields.append({"title": "3️⃣ Audit (Current)", "value": f"✓ All {EXPECTED_STORES} stores present\nWeek ending: {week_ending}", "short": False})
                else:
                    issues = []
                    for table, data in current_audit.get('tables', {}).items():
                        if not data.get('passed'):
                            shortage = data.get('expected', EXPECTED_STORES) - data.get('stores_found', 0)
                            issues.append(f"• {table}: {data.get('stores_found', 0)}/{data.get('expected', EXPECTED_STORES)} stores ({shortage} short)")
                    fields.append({"title": "3️⃣ Audit (Current)", "value": f"✗ Issues found\nWeek ending: {week_ending}\n" + "\n".join(issues), "short": False})

            # PRIOR WEEK
            fields.append({"title": "PRIOR WEEK (Cleanup)", "value": "_" * 40, "short": False})

            prior_download = self.results['prior_week'].get('download', {})
            if prior_download:
                if prior_download.get('success'):
                    fields.append({"title": "1️⃣ Download (Prior)", "value": f"✓ {prior_download.get('files_downloaded', 0)} files downloaded", "short": True})
                else:
                    fields.append({"title": "1️⃣ Download (Prior)", "value": f"✗ {prior_download.get('error', 'Unknown error')}", "short": True})

            prior_parse = self.results['prior_week'].get('parse', {})
            if prior_parse:
                if prior_parse.get('success'):
                    fields.append({"title": "2️⃣ Parse (Prior)", "value": "✓ Completed", "short": True})
                else:
                    fields.append({"title": "2️⃣ Parse (Prior)", "value": f"✗ {prior_parse.get('error', 'Unknown error')}", "short": True})

            prior_audit = self.results['prior_week'].get('audit')
            if prior_audit:
                week_ending = prior_audit.get('week_ending', 'Unknown')
                if prior_audit.get('overall_passed'):
                    fields.append({"title": "3️⃣ Audit (Prior)", "value": f"✓ All {EXPECTED_STORES} stores present\nWeek ending: {week_ending}", "short": False})
                else:
                    issues = []
                    for table, data in prior_audit.get('tables', {}).items():
                        if not data.get('passed'):
                            shortage = data.get('expected', EXPECTED_STORES) - data.get('stores_found', 0)
                            issues.append(f"• {table}: {data.get('stores_found', 0)}/{data.get('expected', EXPECTED_STORES)} stores ({shortage} short)")
                    fields.append({"title": "3️⃣ Audit (Prior)", "value": f"✗ Issues found\nWeek ending: {week_ending}\n" + "\n".join(issues), "short": False})

            fields.append({"title": "Duration",  "value": f"{duration:.1f}s", "short": True})
            fields.append({"title": "Completed", "value": datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC"), "short": True})

            payload = {
                "username":    "WSR Bot",
                "icon_emoji":  emoji,
                "attachments": [{
                    "color":  color,
                    "title":  title,
                    "fields": fields,
                    "footer": f"Atlas WSR Pipeline — {PROFILE_LABEL} Acquisition Stores",
                    "ts":     int(datetime.now().timestamp())
                }]
            }

            response = requests.post(SLACK_WEBHOOK_URL, json=payload, headers={'Content-Type': 'application/json'})

            if response.status_code == 200:
                print(f"{Colors.GREEN}✓ Slack notification sent{Colors.RESET}")
            else:
                print(f"{Colors.RED}✗ Slack notification failed: {response.status_code}{Colors.RESET}")

        except Exception as e:
            print(f"{Colors.RED}✗ Failed to send Slack notification: {e}{Colors.RESET}")
            print(traceback.format_exc())

    async def run(self):
        """Run the complete orchestration for both current and prior week."""
        print(f"\n{Colors.BOLD}{'='*70}{Colors.RESET}")
        print(f"{Colors.BOLD}WSR DAILY PIPELINE — {PROFILE_LABEL.upper()} ACQUISITION STORES{Colors.RESET}")
        print(f"{Colors.BOLD}{'='*70}{Colors.RESET}")
        print(f"Started: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Expected stores: {EXPECTED_STORES}")
        print(f"{Colors.BOLD}{'='*70}{Colors.RESET}\n")

        if self.override_week and self.override_year:
            current_week = self.override_week
            current_year = self.override_year
            print(f"{Colors.CYAN}Manual Override: Week {current_week}, Year {current_year}{Colors.RESET}")
        else:
            current_week, current_year = get_current_week_from_date()
            print(f"{Colors.CYAN}Automatic Mode: Week {current_week}, Year {current_year}{Colors.RESET}")

        prior_week, prior_year = calculate_prior_week(current_week, current_year)
        print(f"{Colors.CYAN}Prior Week: Week {prior_week}, Year {prior_year}{Colors.RESET}\n")

        # ── CURRENT WEEK ──────────────────────────────────────────────────────
        print(f"\n{Colors.BOLD}{Colors.MAGENTA}{'='*70}{Colors.RESET}")
        print(f"{Colors.BOLD}{Colors.MAGENTA}PROCESSING CURRENT WEEK{Colors.RESET}")
        print(f"{Colors.BOLD}{Colors.MAGENTA}{'='*70}{Colors.RESET}\n")

        download_ok = await self.run_download_for_week(current_week, current_year, "CURRENT WEEK")
        self.results['current_week']['download'] = {'success': download_ok, 'week': current_week, 'year': current_year}
        if download_ok:
            self.results['current_week']['download']['files_downloaded'] = len(list(DOWNLOAD_DIR.glob("*.xls*")))

        parse_ok = self.run_parser_for_week("CURRENT WEEK") if download_ok else False
        self.results['current_week']['parse'] = {'success': parse_ok} if download_ok else {'success': False, 'error': 'Skipped due to download failure'}

        if download_ok and parse_ok:
            self.results['current_week']['audit'] = self.audit_database_for_week(current_week, current_year, "CURRENT WEEK")

        # ── PRIOR WEEK ────────────────────────────────────────────────────────
        print(f"\n{Colors.BOLD}{Colors.MAGENTA}{'='*70}{Colors.RESET}")
        print(f"{Colors.BOLD}{Colors.MAGENTA}PROCESSING PRIOR WEEK (Data Cleanup){Colors.RESET}")
        print(f"{Colors.BOLD}{Colors.MAGENTA}{'='*70}{Colors.RESET}\n")

        download_ok_prior = await self.run_download_for_week(prior_week, prior_year, "PRIOR WEEK")
        self.results['prior_week']['download'] = {'success': download_ok_prior, 'week': prior_week, 'year': prior_year}
        if download_ok_prior:
            self.results['prior_week']['download']['files_downloaded'] = len(list(DOWNLOAD_DIR.glob("*.xls*")))

        parse_ok_prior = self.run_parser_for_week("PRIOR WEEK") if download_ok_prior else False
        self.results['prior_week']['parse'] = {'success': parse_ok_prior} if download_ok_prior else {'success': False, 'error': 'Skipped due to download failure'}

        if download_ok_prior and parse_ok_prior:
            self.results['prior_week']['audit'] = self.audit_database_for_week(prior_week, prior_year, "PRIOR WEEK")

        # ── NOTIFY & SUMMARY ─────────────────────────────────────────────────
        self.send_slack_notification()

        print(f"\n{Colors.BOLD}{'='*70}{Colors.RESET}")
        print(f"{Colors.BOLD}PIPELINE COMPLETE — {PROFILE_LABEL}{Colors.RESET}")
        print(f"{Colors.BOLD}{'='*70}{Colors.RESET}")
        duration = (datetime.now() - self.start_time).total_seconds()
        print(f"Total duration: {duration:.1f}s\n")

        current_ok = (
            self.results['current_week'].get('download', {}).get('success', False) and
            self.results['current_week'].get('parse',    {}).get('success', False)
        )
        prior_ok = (
            self.results['prior_week'].get('download', {}).get('success', False) and
            self.results['prior_week'].get('parse',    {}).get('success', False)
        )

        if current_ok and prior_ok:
            print(f"{Colors.GREEN}✓ Both weeks processed successfully{Colors.RESET}\n")
            return True
        elif current_ok or prior_ok:
            print(f"{Colors.YELLOW}⚠ Pipeline partially succeeded{Colors.RESET}\n")
            return True
        else:
            print(f"{Colors.RED}✗ Pipeline failed for both weeks{Colors.RESET}\n")
            for error in self.results['errors']:
                print(f"  • {error}")
            return False


async def main():
    """Entry point"""
    import argparse

    parser = argparse.ArgumentParser(description=f'WSR Daily Orchestrator — {PROFILE_LABEL}')
    parser.add_argument('--week', type=int, help='Week number (1-52)')
    parser.add_argument('--year', type=int, help='Year (e.g., 2025)')
    args = parser.parse_args()

    if (args.week and not args.year) or (args.year and not args.week):
        print(f"{Colors.RED}❌ Both --week and --year must be provided together{Colors.RESET}")
        sys.exit(1)

    if args.week and (args.week < 1 or args.week > 52):
        print(f"{Colors.RED}❌ Week must be between 1 and 52, got {args.week}{Colors.RESET}")
        sys.exit(1)

    try:
        orchestrator = WSROrchestrator(override_week=args.week, override_year=args.year)
        success = await orchestrator.run()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\n{Colors.RED}Fatal error: {e}{Colors.RESET}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
