#!/usr/bin/env python3
"""
WSR Historical Backfill Script
Iterates through a range of weeks, downloads WSR reports and parses them
into Supabase. Skips weeks that already have complete data (resume-safe).

Usage:
    python wsr_backfill.py --profile km --year 2024
    python wsr_backfill.py --profile mm --year 2025
    python wsr_backfill.py --profile km --start-week 1 --start-year 2024 --end-week 20 --end-year 2026
"""

import os
import sys
import asyncio
import argparse
import subprocess
import shutil
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple, List
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# BACKFILL SETTINGS
# ---------------------------------------------------------------------------
# Faster polling for historical data — reports generate near-instantly
# since they're not waiting on live POS data
BACKFILL_CHECK_INTERVAL      = 10   # seconds between checks (vs 30 for live)
BACKFILL_MAX_EMPTY_CHECKS    = 6    # max empty checks (vs 10 for live)

# How many consecutive parse failures before aborting the whole run
MAX_CONSECUTIVE_FAILURES     = 3


class Colors:
    GREEN   = '\033[92m'
    YELLOW  = '\033[93m'
    RED     = '\033[91m'
    CYAN    = '\033[96m'
    MAGENTA = '\033[95m'
    RESET   = '\033[0m'
    BOLD    = '\033[1m'


def get_week_ending_date(week: int, year: int) -> str:
    """
    Calculate the Tuesday week-ending date for a given JJ week + year.
    JJ fiscal weeks are offset -1 from ISO weeks:
      JJ Week 1 = ISO Week 2, etc.
    """
    year_start      = datetime(year, 1, 1)
    days_to_tuesday = (1 - year_start.weekday()) % 7
    first_tuesday   = year_start + timedelta(days=days_to_tuesday)
    target_tuesday  = first_tuesday + timedelta(weeks=week - 1)
    return target_tuesday.strftime('%Y-%m-%d')


def get_max_week_for_year(year: int) -> int:
    """Return the last JJ week number for a given year (52 for most years)."""
    # Check if week 53 exists by seeing if its date falls in the right year
    week_53_end = get_week_ending_date(53, year)
    if datetime.strptime(week_53_end, '%Y-%m-%d').year == year:
        return 53
    return 52


def get_current_week() -> Tuple[int, int]:
    """Return the current JJ week and year."""
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


def build_week_list(
    start_week: int, start_year: int,
    end_week: int,   end_year: int
) -> List[Tuple[int, int]]:
    """Build an ordered list of (week, year) tuples for the given range."""
    weeks = []
    year  = start_year
    week  = start_week

    while (year < end_year) or (year == end_year and week <= end_week):
        weeks.append((week, year))
        max_week = get_max_week_for_year(year)
        week += 1
        if week > max_week:
            week  = 1
            year += 1

    return weeks


def check_week_in_supabase(
    store_numbers: List[int],
    week: int,
    year: int,
    expected_stores: int
) -> bool:
    """
    Returns True if all expected stores already have data for this week
    in wsr_sales (used as the canonical presence check).
    """
    try:
        from supabase import create_client

        supabase_url = os.getenv('SUPABASE_URL')
        supabase_key = os.getenv('SUPABASE_KEY')
        if not supabase_url or not supabase_key:
            return False

        client      = create_client(supabase_url, supabase_key)
        week_ending = get_week_ending_date(week, year)

        response = client.table('wsr_sales')\
            .select('store_number')\
            .in_('store_number', store_numbers)\
            .eq('week_ending', week_ending)\
            .execute()

        stores_found = len(set(r['store_number'] for r in (response.data or [])))
        return stores_found >= expected_stores

    except Exception as e:
        print(f"{Colors.YELLOW}  ⚠ Could not check Supabase: {e}{Colors.RESET}")
        return False


async def run_download_for_week(
    profile: str,
    week: int,
    year: int,
    download_dir: Path
) -> bool:
    """Run the downloader for a specific week in backfill mode."""
    try:
        import wsr_download_acquisition as downloader_module
        from wsr_download_acquisition import WSRDownloader

        # Override polling constants for faster backfill
        downloader_module.CHECK_INTERVAL              = BACKFILL_CHECK_INTERVAL
        downloader_module.MAX_CONSECUTIVE_EMPTY_CHECKS = BACKFILL_MAX_EMPTY_CHECKS
        downloader_module.OVERRIDE_WEEK               = week
        downloader_module.OVERRIDE_YEAR               = year

        # Clear download dir
        if download_dir.exists():
            shutil.rmtree(download_dir)

        downloader = WSRDownloader()
        await downloader.run()

        files = list(download_dir.glob("*.xls*"))
        return len(files) > 0

    except Exception as e:
        print(f"{Colors.RED}  ✗ Download error: {e}{Colors.RESET}")
        return False


def run_parser(download_dir: Path) -> bool:
    """Run the WSR parser against the downloaded files."""
    try:
        result = subprocess.run(
            [sys.executable, 'process_wsr_ENHANCED.py', 'process', str(download_dir), '--email'],
            capture_output=True, text=True, check=False
        )
        if result.stdout:
            # Only print errors/warnings, not full verbose output
            for line in result.stdout.splitlines():
                if any(kw in line for kw in ['Error', 'Warning', 'ERROR', 'WARNING', 'Upserted', 'Skipped', 'Fatal']):
                    print(f"  {line}")
        if result.returncode != 0:
            if result.stderr:
                print(f"{Colors.RED}  {result.stderr[:300]}{Colors.RESET}")
            return False
        return True
    except Exception as e:
        print(f"{Colors.RED}  ✗ Parser error: {e}{Colors.RESET}")
        return False


async def run_backfill(
    profile:        str,
    start_week:     int,
    start_year:     int,
    end_week:       int,
    end_year:       int,
    store_numbers:  List[int],
    expected_stores: int,
    skip_existing:  bool = True,
    dry_run:        bool = False,
):
    """Main backfill loop."""
    weeks      = build_week_list(start_week, start_year, end_week, end_year)
    total      = len(weeks)
    profile_uc = profile.upper()

    if profile == 'mm':
        download_dir  = Path('wsr_downloads_mm')
        profile_label = 'MikLin/Mulligan'
        os.environ['WSR_PROFILE'] = 'mm'
    else:
        download_dir  = Path('wsr_downloads_km')
        profile_label = 'Kerr-McCauley'
        os.environ['WSR_PROFILE'] = 'km'

    print(f"\n{Colors.BOLD}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}WSR BACKFILL — {profile_label.upper()}{Colors.RESET}")
    print(f"{Colors.BOLD}{'='*70}{Colors.RESET}")
    print(f"Range  : Week {start_week}/{start_year} → Week {end_week}/{end_year}")
    print(f"Total  : {total} weeks")
    print(f"Stores : {store_numbers}")
    print(f"Mode   : {'DRY RUN' if dry_run else 'LIVE'}")
    print(f"Resume : {'enabled (skip existing)' if skip_existing else 'disabled (re-process all)'}")
    print(f"{Colors.BOLD}{'='*70}{Colors.RESET}\n")

    skipped        = 0
    succeeded      = 0
    failed         = 0
    failed_weeks   = []
    consecutive_failures = 0
    start_time     = datetime.now()

    for i, (week, year) in enumerate(weeks, 1):
        week_ending = get_week_ending_date(week, year)
        print(f"{Colors.CYAN}[{i}/{total}] Week {week}, {year}  (ending {week_ending}){Colors.RESET}", end='  ')

        # Resume: skip if already fully populated
        if skip_existing and not dry_run:
            if check_week_in_supabase(store_numbers, week, year, expected_stores):
                print(f"{Colors.GREEN}✓ already in DB — skipping{Colors.RESET}")
                skipped += 1
                continue

        if dry_run:
            print(f"{Colors.YELLOW}[dry-run] would process{Colors.RESET}")
            continue

        print()  # newline before download logs

        # Download
        download_ok = await run_download_for_week(profile, week, year, download_dir)
        if not download_ok:
            print(f"{Colors.RED}  ✗ Download failed — skipping parse{Colors.RESET}")
            failed += 1
            failed_weeks.append((week, year, 'download'))
            consecutive_failures += 1
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                print(f"\n{Colors.RED}❌ {MAX_CONSECUTIVE_FAILURES} consecutive failures — aborting backfill{Colors.RESET}")
                break
            continue

        consecutive_failures = 0

        # Parse
        parse_ok = run_parser(download_dir)
        if parse_ok:
            files = list(download_dir.glob("*.xls*"))
            print(f"  {Colors.GREEN}✓ Parsed {len(files)} files{Colors.RESET}")
            succeeded += 1
        else:
            print(f"  {Colors.RED}✗ Parse failed{Colors.RESET}")
            failed += 1
            failed_weeks.append((week, year, 'parse'))

        # Brief pause between weeks to avoid hammering the portal
        await asyncio.sleep(2)

    duration = (datetime.now() - start_time).total_seconds()

    print(f"\n{Colors.BOLD}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}BACKFILL COMPLETE — {profile_label}{Colors.RESET}")
    print(f"{Colors.BOLD}{'='*70}{Colors.RESET}")
    print(f"Duration  : {duration/60:.1f} minutes")
    print(f"Succeeded : {Colors.GREEN}{succeeded}{Colors.RESET}")
    print(f"Skipped   : {Colors.CYAN}{skipped}{Colors.RESET}  (already in DB)")
    print(f"Failed    : {Colors.RED}{failed}{Colors.RESET}")

    if failed_weeks:
        print(f"\n{Colors.YELLOW}Failed weeks:{Colors.RESET}")
        for w, y, stage in failed_weeks:
            print(f"  Week {w}/{y} — {stage}")

    print(f"{Colors.BOLD}{'='*70}{Colors.RESET}\n")
    return failed == 0


async def main():
    parser = argparse.ArgumentParser(
        description='WSR Historical Backfill',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full backfill for KM, 2024
  python wsr_backfill.py --profile km --year 2024

  # Full backfill for MM, all years
  python wsr_backfill.py --profile mm --year 2024
  python wsr_backfill.py --profile mm --year 2025
  python wsr_backfill.py --profile mm --year 2026

  # Custom range
  python wsr_backfill.py --profile km --start-week 1 --start-year 2024 --end-week 20 --end-year 2026

  # Dry run (shows what would be processed without downloading)
  python wsr_backfill.py --profile km --year 2024 --dry-run

  # Re-process all weeks even if already in DB
  python wsr_backfill.py --profile km --year 2024 --no-skip
        """
    )
    parser.add_argument('--profile',      choices=['km', 'mm'], required=True)
    parser.add_argument('--year',         type=int, help='Process all weeks for this year')
    parser.add_argument('--start-week',   type=int)
    parser.add_argument('--start-year',   type=int)
    parser.add_argument('--end-week',     type=int)
    parser.add_argument('--end-year',     type=int)
    parser.add_argument('--dry-run',      action='store_true', help='Show what would run without downloading')
    parser.add_argument('--no-skip',      action='store_true', help='Re-process weeks already in Supabase')
    args = parser.parse_args()

    # Resolve week range
    if args.year:
        start_week, start_year = 1, args.year
        max_week = get_max_week_for_year(args.year)
        cur_week, cur_year = get_current_week()
        if args.year == cur_year:
            end_week, end_year = cur_week, cur_year
        else:
            end_week, end_year = max_week, args.year
    elif all([args.start_week, args.start_year, args.end_week, args.end_year]):
        start_week, start_year = args.start_week, args.start_year
        end_week,   end_year   = args.end_week,   args.end_year
    else:
        parser.error("Provide either --year or all of --start-week --start-year --end-week --end-year")
        return

    # Store config per profile
    if args.profile == 'km':
        store_numbers   = [1340, 2357, 1563, 2646]
        expected_stores = 4
    else:
        # MM store numbers — update once known
        store_numbers   = []
        expected_stores = 8

    success = await run_backfill(
        profile         = args.profile,
        start_week      = start_week,
        start_year      = start_year,
        end_week        = end_week,
        end_year        = end_year,
        store_numbers   = store_numbers,
        expected_stores = expected_stores,
        skip_existing   = not args.no_skip,
        dry_run         = args.dry_run,
    )
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    asyncio.run(main())
