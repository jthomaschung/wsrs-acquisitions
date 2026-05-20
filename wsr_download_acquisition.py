import os
import asyncio
from datetime import datetime, timedelta
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
from dotenv import load_dotenv
import logging
import argparse

# Suppress Playwright's verbose debug output
os.environ['DEBUG'] = ''

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Suppress verbose logging from libraries
logging.getLogger('playwright').setLevel(logging.WARNING)
logging.getLogger('asyncio').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('httpx').setLevel(logging.WARNING)

# ---------------------------------------------------------------------------
# PROFILE CONFIGURATION
# ---------------------------------------------------------------------------
# Profile is set by:
#   1. The orchestrator (sets os.environ['WSR_PROFILE'] before importing this module)
#   2. The --profile CLI argument (sets os.environ['WSR_PROFILE'] in main())
#   3. Falls back to 'km' if neither is set
#
# Profile 'km'  → Kerr-McCauley stores  → KM_SITE_USERNAME / KM_SITE_PASSWORD
# Profile 'mm'  → MikLin/Mulligan stores → MM_SITE_USERNAME / MM_SITE_PASSWORD
# ---------------------------------------------------------------------------
PROFILE = os.getenv('WSR_PROFILE', 'km').lower()

if PROFILE == 'mm':
    SITE_USERNAME = os.getenv('MM_SITE_USERNAME')
    SITE_PASSWORD = os.getenv('MM_SITE_PASSWORD')
    DOWNLOAD_DIR  = os.path.join(os.getcwd(), 'wsr_downloads_mm')
    BROWSER_DATA_DIR = os.path.join(os.getcwd(), 'browser_data_mm')
    PROFILE_LABEL = 'MikLin/Mulligan'
else:  # default: km
    SITE_USERNAME = os.getenv('KM_SITE_USERNAME')
    SITE_PASSWORD = os.getenv('KM_SITE_PASSWORD')
    DOWNLOAD_DIR  = os.path.join(os.getcwd(), 'wsr_downloads_km')
    BROWSER_DATA_DIR = os.path.join(os.getcwd(), 'browser_data_km')
    PROFILE_LABEL = 'Kerr-McCauley'

# ---------------------------------------------------------------------------
# Shared constants (same login portal for all JJ franchisees)
# ---------------------------------------------------------------------------
LOGIN_URL = "https://jimmyjohns.macromatix.net/MMS_Logon.aspx"
MAX_WAIT_TIME = 1800           # 30 minutes total
CHECK_INTERVAL = 30            # 30s between empty-queue checks
MAX_CONSECUTIVE_EMPTY_CHECKS = 10
MAX_PAGES = 200

# Week/Year override — set via command line or by the orchestrator module
OVERRIDE_WEEK = None
OVERRIDE_YEAR = None


class WSRDownloader:
    def __init__(self):
        self.page    = None
        self.context = None
        self.browser = None

    async def setup(self):
        """Initialize browser and create download directory"""
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
        logger.info(f"[{PROFILE_LABEL}] Download directory: {DOWNLOAD_DIR}")

    async def login(self):
        """
        Login to Macromatix portal.

        Acquisition store accounts have a two-step login:
          Step 1 — Enter username + password → click Log On
          Step 2 — Store selector page ("Select the store where you are currently
                   working") → click Log On again (store choice doesn't matter;
                   we switch to Multi Store view immediately after)
        """
        logger.info(f"[{PROFILE_LABEL}] Logging in to Macromatix...")

        await self.page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(2)

        current_url = self.page.url.lower()

        # Already logged in from a previous persistent-context session
        if 'mms_' in current_url and 'logon' not in current_url and 'login' not in current_url:
            logger.info(f"[{PROFILE_LABEL}] Already logged in! Current page: {self.page.url}")
            return

        if 'logon' in current_url or 'login' in current_url:
            logger.info(f"[{PROFILE_LABEL}] Step 1: Entering credentials...")

            username_input = self.page.locator('input[type="text"]').first
            await username_input.fill(SITE_USERNAME)
            logger.info(f"[{PROFILE_LABEL}] Entered username")

            password_input = self.page.locator('input[type="password"]').first
            await password_input.fill(SITE_PASSWORD)
            logger.info(f"[{PROFILE_LABEL}] Entered password")

            login_button = self.page.locator('input[value="Log On"]')
            button_count = await login_button.count()

            if button_count > 0:
                await login_button.click()
                logger.info(f"[{PROFILE_LABEL}] Clicked 'Log On' button")
            else:
                submit_button = self.page.locator('input[type="submit"]').first
                await submit_button.click()
                logger.info(f"[{PROFILE_LABEL}] Clicked submit button")

            await self.page.wait_for_load_state('networkidle', timeout=10000)
            await asyncio.sleep(3)

            # ── Step 2: Store selector (acquisition accounts only) ─────────
            # After the first Log On, the portal presents a store dropdown
            # ("Select the store where you are currently working") before
            # granting access. Just click Log On — no store selection needed.
            page_text = await self.page.evaluate('() => document.body.innerText')
            if 'Select the store where you are currently working' in page_text:
                logger.info(f"[{PROFILE_LABEL}] Step 2: Store selector page detected — clicking through...")

                store_logon_button = self.page.locator('input[value="Log On"]')
                store_logon_count  = await store_logon_button.count()

                if store_logon_count > 0:
                    await store_logon_button.click()
                    logger.info(f"[{PROFILE_LABEL}] Clicked 'Log On' on store selector page")
                else:
                    submit = self.page.locator('input[type="submit"]').first
                    await submit.click()
                    logger.info(f"[{PROFILE_LABEL}] Clicked submit on store selector page")

                await self.page.wait_for_load_state('networkidle', timeout=10000)
                await asyncio.sleep(3)

            # ── Verify we made it into the portal ─────────────────────────
            # Accept any URL that is no longer on the logon page
            current_url = self.page.url.lower()
            if 'logon' not in current_url and 'login' not in current_url:
                logger.info(f"[{PROFILE_LABEL}] ✓ Login successful! Redirected to: {self.page.url}")
            else:
                logger.error(f"[{PROFILE_LABEL}] Login may have failed. Current URL: {self.page.url}")
                await self.page.screenshot(path=f'login_error_{PROFILE}.png')
                raise Exception("Login verification failed")
        else:
            logger.error(f"[{PROFILE_LABEL}] Unexpected page. Current URL: {self.page.url}")
            await self.page.screenshot(path=f'unexpected_page_{PROFILE}.png')
            raise Exception("Not on login page")

    async def navigate_to_reports(self):
        """Navigate to Business Analytics page (starts on Single Store view)"""
        logger.info(f"[{PROFILE_LABEL}] Navigating to Business Analytics...")
        reports_url = "https://jimmyjohns.macromatix.net/MMS_System_BAReports.aspx?MenuCustomItemID=250"
        await self.page.goto(reports_url, wait_until='domcontentloaded', timeout=15000)
        await asyncio.sleep(2)
        logger.info(f"[{PROFILE_LABEL}] ✓ On Business Analytics page")

    async def get_current_week_dates(self):
        """
        Calculate week dates based on day of week.

        Jimmy Johns weeks: Wednesday-Tuesday (week ends on Tuesday)

        BUSINESS LOGIC:
        - Wednesday: Download PRIOR week (ended yesterday) — data is finalized overnight
        - All other days: Download CURRENT week (even if incomplete)
        """
        today = datetime.now()
        today_weekday = today.weekday()  # Monday=0, Tuesday=1, Wednesday=2, etc.

        if today_weekday == 2:  # Wednesday — SPECIAL CASE
            week_end = today - timedelta(days=1)
            logger.info(f"[{PROFILE_LABEL}] 🔙 Today is Wednesday — downloading PRIOR week (finalized)")
        else:
            days_until_tuesday = (1 - today_weekday) % 7
            if days_until_tuesday == 0:
                week_end = today
                logger.info(f"[{PROFILE_LABEL}] 📅 Today is Tuesday — downloading current week (ends today)")
            else:
                week_end = today + timedelta(days=days_until_tuesday)
                logger.info(f"[{PROFILE_LABEL}] 📅 Downloading current week ending {week_end.strftime('%m-%d-%Y')} (incomplete)")

        week_start = week_end - timedelta(days=6)
        logger.info(f"[{PROFILE_LABEL}] Target week: {week_start.strftime('%m-%d-%Y')} to {week_end.strftime('%m-%d-%Y')}")
        return week_start, week_end

    async def select_current_week(self):
        """Select the week by matching start date or using manual override"""
        logger.info(f"[{PROFILE_LABEL}] Selecting current week...")

        if OVERRIDE_WEEK and OVERRIDE_YEAR:
            override_week = int(OVERRIDE_WEEK)
            override_year = int(OVERRIDE_YEAR)
            logger.info(f"[{PROFILE_LABEL}] 🎯 MANUAL OVERRIDE: Week {override_week}, Year {override_year}")
            use_override = True
        else:
            logger.info(f"[{PROFILE_LABEL}] 📅 Using automatic week detection")
            use_override = False

        week_start, week_end = await self.get_current_week_dates()
        target_start = week_start.strftime('%m-%d-%Y')
        target_end   = week_end.strftime('%m-%d-%Y')
        auto_year    = week_end.year

        if not use_override:
            logger.info(f"[{PROFILE_LABEL}] Target date range: {target_start} to {target_end}")
            logger.info(f"[{PROFILE_LABEL}] Target year: {auto_year}")

        refresh_checkbox = await self.page.query_selector('input[type="checkbox"]#RefreshReport')
        if refresh_checkbox:
            is_checked = await refresh_checkbox.is_checked()
            if not is_checked:
                await refresh_checkbox.check()
                logger.info(f"[{PROFILE_LABEL}] ✓ Checked 'Refresh Report'")

        all_selects = await self.page.query_selector_all('select')
        logger.info(f"[{PROFILE_LABEL}] Found {len(all_selects)} select dropdowns")

        week_select = None
        year_select = None

        for select in all_selects:
            options = await select.query_selector_all('option')
            if len(options) == 0:
                continue
            first_option = options[0]
            first_value  = await first_option.get_attribute('value')
            if first_value and first_value.isdigit():
                week_num = int(first_value)
                if 1 <= week_num <= 53:
                    week_select = select
                    logger.info(f"[{PROFILE_LABEL}] ✓ Found Week dropdown")
                    continue
            if first_value and first_value.isdigit() and len(first_value) == 4:
                year_num = int(first_value)
                if 2000 <= year_num <= 2100:
                    year_select = select
                    logger.info(f"[{PROFILE_LABEL}] ✓ Found Year dropdown")
                    continue

        if not week_select or not year_select:
            logger.error(f"[{PROFILE_LABEL}] Could not find Week and/or Year dropdowns")
            return

        if use_override:
            logger.info(f"[{PROFILE_LABEL}] Selecting manual override: Week {override_week}, Year {override_year}")

            try:
                await year_select.select_option(str(override_year))
                logger.info(f"[{PROFILE_LABEL}] ✓ Selected Year {override_year}")
                logger.info(f"[{PROFILE_LABEL}] Waiting 3s for page to update after year selection...")
                await asyncio.sleep(3)
            except Exception as e:
                logger.error(f"[{PROFILE_LABEL}] Could not select year {override_year}: {e}")
                raise Exception(f"Failed to select year {override_year}")

            logger.info(f"[{PROFILE_LABEL}] Re-querying dropdowns after year change...")
            all_selects = await self.page.query_selector_all('select')
            week_select = None

            for select in all_selects:
                options = await select.query_selector_all('option')
                if len(options) == 0:
                    continue
                first_option = options[0]
                first_value  = await first_option.get_attribute('value')
                if first_value and first_value.isdigit():
                    week_num = int(first_value)
                    if 1 <= week_num <= 53:
                        week_select = select
                        logger.info(f"[{PROFILE_LABEL}] ✓ Re-found Week dropdown with fresh DOM reference")
                        break

            if not week_select:
                logger.error(f"[{PROFILE_LABEL}] Could not re-find week dropdown after year selection")
                await self.page.screenshot(path=f'week_dropdown_missing_{PROFILE}.png')
                raise Exception("Week dropdown not found after year change")

            try:
                await week_select.select_option(str(override_week))
                logger.info(f"[{PROFILE_LABEL}] ✓ Selected Week {override_week}")
                await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"[{PROFILE_LABEL}] Could not select week {override_week}: {e}")
                await self.page.screenshot(path=f'week_selection_failed_{PROFILE}.png')
                raise Exception(f"Failed to select week {override_week}")

            start_date_input = await self.page.query_selector('input[id*="StartDate"], input[name*="StartDate"]')
            end_date_input   = await self.page.query_selector('input[id*="EndDate"], input[name*="EndDate"]')

            if start_date_input and end_date_input:
                actual_start = await start_date_input.input_value()
                actual_end   = await end_date_input.input_value()
                logger.info(f"[{PROFILE_LABEL}] ✅ VERIFIED: Week {override_week}, {override_year} = {actual_start} to {actual_end}")
            else:
                logger.warning(f"[{PROFILE_LABEL}] Could not verify dates, but Week {override_week}, Year {override_year} should be selected")
            return

        # Automatic detection path
        current_week_option = await week_select.query_selector('option[selected]')
        if not current_week_option:
            current_week_option = await week_select.query_selector('option')
        current_week = await current_week_option.get_attribute('value')

        current_year_option = await year_select.query_selector('option[selected]')
        if not current_year_option:
            current_year_option = await year_select.query_selector('option')
        current_year = await current_year_option.get_attribute('value')

        logger.info(f"[{PROFILE_LABEL}] Currently selected: Week {current_week}, Year {current_year}")

        if current_year != str(auto_year):
            logger.info(f"[{PROFILE_LABEL}] Changing year to {auto_year}")
            await year_select.select_option(str(auto_year))
            await self.page.wait_for_load_state('networkidle', timeout=10000)
            await asyncio.sleep(1)

        start_date_input = await self.page.query_selector('input[id*="StartDate"], input[name*="StartDate"]')
        if start_date_input:
            displayed_date = await start_date_input.input_value()
            logger.info(f"[{PROFILE_LABEL}] Start Date currently shows: {displayed_date}")

            if displayed_date == target_start:
                logger.info(f"[{PROFILE_LABEL}] ✓ Already on correct week!")
                return

            logger.info(f"[{PROFILE_LABEL}] Searching for week with start date {target_start}...")

            week_select = await self.page.query_selector('select[name*="Week"], select[name*="Period"]')
            if not week_select:
                logger.error(f"[{PROFILE_LABEL}] Could not find week dropdown after year selection")
                return

            week_options = await week_select.query_selector_all('option')
            week_values  = []
            for option in week_options:
                try:
                    val = await option.get_attribute('value')
                    if val:
                        week_values.append(val)
                except:
                    continue

            logger.info(f"[{PROFILE_LABEL}] Found {len(week_values)} weeks to check")

            for week_val in week_values:
                try:
                    week_select = await self.page.query_selector('select[name*="Week"], select[name*="Period"]')
                    if not week_select:
                        continue
                    await week_select.select_option(week_val)
                    await self.page.wait_for_load_state('networkidle', timeout=10000)
                    await asyncio.sleep(1)
                    start_date_input = await self.page.query_selector('input[name*="StartDate"], input[id*="StartDate"]')
                    if not start_date_input:
                        continue
                    displayed_date = await start_date_input.input_value()
                    if displayed_date == target_start:
                        logger.info(f"[{PROFILE_LABEL}] ✓ Found correct week! Week {week_val} = {target_start}")
                        return
                except Exception as e:
                    logger.debug(f"[{PROFILE_LABEL}] Error checking week {week_val}: {e}")
                    continue

            logger.warning(f"[{PROFILE_LABEL}] Could not find week with start date {target_start}, continuing with currently selected week")
        else:
            logger.warning(f"[{PROFILE_LABEL}] Could not find Start Date field, continuing with currently selected week")

    async def switch_to_multi_store(self):
        """Click on Multi Store tab"""
        logger.info(f"[{PROFILE_LABEL}] Switching to Multi Store view...")
        multi_store_tab = await self.page.wait_for_selector('text="Multi Store"')
        await multi_store_tab.click()
        await asyncio.sleep(2)
        logger.info(f"[{PROFILE_LABEL}] ✓ Switched to Multi Store view")

    async def select_all_stores(self):
        """Select all stores by checking the 'All' checkbox"""
        logger.info(f"[{PROFILE_LABEL}] Selecting all stores...")
        await asyncio.sleep(3)
        await self.page.screenshot(path=f'before_select_all_{PROFILE}.png')

        # Strategy 1: Look for checkbox near "All (N)" text
        try:
            logger.info(f"[{PROFILE_LABEL}] Strategy 1: Looking for checkbox near 'All' text...")
            all_text = await self.page.query_selector('text=/All \\(\\d+\\)/')
            if all_text:
                parent   = await all_text.evaluate_handle('el => el.closest("label") || el.parentElement')
                checkbox = await parent.query_selector('input[type="checkbox"]')
                if checkbox:
                    is_checked = await checkbox.is_checked()
                    if not is_checked:
                        await checkbox.scroll_into_view_if_needed()
                        await asyncio.sleep(0.5)
                        await checkbox.click()
                        await asyncio.sleep(2)
                        if await checkbox.is_checked():
                            logger.info(f"[{PROFILE_LABEL}] ✓ Selected all stores (Strategy 1)")
                            return True
        except Exception as e:
            logger.warning(f"[{PROFILE_LABEL}] Strategy 1 failed: {e}")

        # Strategy 2: Find checkboxes with "All" nearby
        try:
            logger.info(f"[{PROFILE_LABEL}] Strategy 2: Looking in store tree area...")
            checkboxes = await self.page.query_selector_all('input[type="checkbox"]')
            logger.info(f"[{PROFILE_LABEL}] Found {len(checkboxes)} total checkboxes on page")
            for i, checkbox in enumerate(checkboxes):
                try:
                    is_visible = await checkbox.is_visible()
                    is_checked = await checkbox.is_checked()
                    if is_checked or not is_visible:
                        continue
                    parent      = await checkbox.evaluate_handle('el => el.parentElement')
                    parent_text = await parent.evaluate('el => el.textContent')
                    if 'All' in parent_text and '(' in parent_text:
                        await checkbox.scroll_into_view_if_needed()
                        await asyncio.sleep(0.5)
                        await checkbox.click()
                        await asyncio.sleep(2)
                        if await checkbox.is_checked():
                            logger.info(f"[{PROFILE_LABEL}] ✓ Selected all stores (Strategy 2)")
                            return True
                except Exception as e:
                    logger.debug(f"[{PROFILE_LABEL}] Error checking checkbox {i}: {e}")
                    continue
        except Exception as e:
            logger.warning(f"[{PROFILE_LABEL}] Strategy 2 failed: {e}")

        # Strategy 3: First unchecked checkbox that triggers multi-select
        try:
            logger.info(f"[{PROFILE_LABEL}] Strategy 3: Trying first unchecked checkbox in tree...")
            checkboxes = await self.page.query_selector_all('input[type="checkbox"]')
            for i, checkbox in enumerate(checkboxes):
                if i < 2:
                    continue
                is_visible = await checkbox.is_visible()
                is_checked = await checkbox.is_checked()
                if is_visible and not is_checked:
                    await checkbox.scroll_into_view_if_needed()
                    await asyncio.sleep(0.5)
                    await checkbox.click()
                    await asyncio.sleep(2)
                    all_checkboxes = await self.page.query_selector_all('input[type="checkbox"]:checked')
                    if len(all_checkboxes) > 1:
                        logger.info(f"[{PROFILE_LABEL}] ✓ Selected all stores (Strategy 3)")
                        return True
                    else:
                        await checkbox.click()
                        await asyncio.sleep(1)
        except Exception as e:
            logger.error(f"[{PROFILE_LABEL}] Strategy 3 failed: {e}")

        logger.error(f"[{PROFILE_LABEL}] ❌ Failed to select all stores!")
        await self.page.screenshot(path=f'select_all_failed_{PROFILE}.png')
        return False

    async def generate_all_reports(self):
        """Click the 'Generate All Reports' button"""
        logger.info(f"[{PROFILE_LABEL}] Generating all reports...")
        checkboxes = await self.page.query_selector_all('input[type="checkbox"]:checked')
        logger.info(f"[{PROFILE_LABEL}] {len(checkboxes)} store(s) selected")

        if len(checkboxes) == 0:
            logger.error(f"[{PROFILE_LABEL}] ⚠ No stores are selected!")
            await self.page.screenshot(path=f'no_stores_selected_{PROFILE}.png')
            raise Exception("No stores selected before generating reports")

        await self.page.screenshot(path=f'before_generate_{PROFILE}.png')
        generate_button = await self.page.wait_for_selector('text="Generate All Reports"', timeout=5000)
        await generate_button.click()
        logger.info(f"[{PROFILE_LABEL}] Clicked 'Generate All Reports'")
        await asyncio.sleep(2)

        try:
            confirm_button = await self.page.wait_for_selector('text="OK"', timeout=3000)
            if confirm_button:
                await confirm_button.click()
                logger.info(f"[{PROFILE_LABEL}] ✓ Confirmed report generation")
        except PlaywrightTimeout:
            pass

        logger.info(f"[{PROFILE_LABEL}] ✓ Reports generation initiated")

    async def navigate_to_download_center(self):
        """Navigate to the Download Center"""
        logger.info(f"[{PROFILE_LABEL}] Navigating to Download Center...")
        download_center_url = "https://jimmyjohns.macromatix.net/MMS_System_DownloadManager.aspx?MenuCustomItemID=333"
        await self.page.goto(download_center_url, wait_until='domcontentloaded', timeout=15000)
        await asyncio.sleep(2)
        logger.info(f"[{PROFILE_LABEL}] ✓ On Download Center")

    async def set_status_filter_to_all(self):
        """Set the Status filter to 'All' to prevent reports from disappearing after download"""
        try:
            logger.info(f"[{PROFILE_LABEL}] Setting Status filter to 'All'...")
            selects = await self.page.query_selector_all('select')
            for select in selects:
                try:
                    options      = await select.query_selector_all('option')
                    option_texts = []
                    for opt in options:
                        text = await opt.text_content()
                        option_texts.append(text.strip() if text else "")
                    if "All" in option_texts and ("Not Downloaded" in option_texts or "Downloaded" in option_texts):
                        await select.select_option(label="All")
                        logger.info(f"[{PROFILE_LABEL}] ✓ Set Status filter to 'All'")
                        await asyncio.sleep(2)
                        return True
                except:
                    continue
            logger.warning(f"[{PROFILE_LABEL}] Could not find Status filter dropdown")
            return False
        except Exception as e:
            logger.error(f"[{PROFILE_LABEL}] Error setting status filter: {e}")
            return False

    async def download_reports_on_current_page(self):
        """Download Ready Collated Reports ONE AT A TIME, refreshing after each"""
        page_downloaded = 0
        try:
            max_attempts = 100
            attempt      = 0
            while attempt < max_attempts:
                attempt += 1
                data_rows   = await self.page.query_selector_all('tr:has(td)')
                found_ready = False

                for i, row in enumerate(data_rows):
                    try:
                        row_text = await row.text_content()
                        if not row_text or len(row_text.strip()) < 20:
                            continue
                        if "Command item" in row_text or "Refres" in row_text:
                            continue
                        if "Collated Report" not in row_text:
                            continue
                        if "Ready" in row_text and "Queued" not in row_text and "Downloaded" not in row_text and "Failed" not in row_text:
                            found_ready = True
                            cells = await row.query_selector_all('td')
                            if len(cells) < 3:
                                continue
                            title = ""
                            for cell in cells[1:4]:
                                cell_text = await cell.text_content()
                                cell_text = cell_text.strip() if cell_text else ""
                                if "Collated Report" in cell_text and len(cell_text) > 20:
                                    title = cell_text[:70]
                                    break
                            if not title:
                                for cell in cells:
                                    cell_text = await cell.text_content()
                                    cell_text = cell_text.strip() if cell_text else ""
                                    if len(cell_text) > 30 and "Command" not in cell_text:
                                        title = cell_text[:70]
                                        break
                            if not title or "Command" in title:
                                continue

                            logger.info(f"[{PROFILE_LABEL}] ⬇️  {title}")
                            download_link = await row.query_selector('a:has-text("Download")')
                            if not download_link:
                                logger.warning(f"[{PROFILE_LABEL}]   ✗ No Download link found")
                                break

                            try:
                                async with self.page.expect_download(timeout=120000) as download_info:
                                    await download_link.click()
                                download = await download_info.value
                                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                                suggested_filename = download.suggested_filename
                                if suggested_filename:
                                    from pathlib import Path
                                    extension = Path(suggested_filename).suffix
                                    base_name = Path(suggested_filename).stem
                                    filename  = f"{base_name}_{timestamp}{extension}"
                                else:
                                    filename = f"Report_{timestamp}.zip"
                                save_path = os.path.join(DOWNLOAD_DIR, filename)
                                await download.save_as(save_path)
                                if os.path.exists(save_path):
                                    file_size = os.path.getsize(save_path)
                                    logger.info(f"[{PROFILE_LABEL}]   ✓ {file_size / 1024:.0f} KB")
                                    page_downloaded += 1
                                await asyncio.sleep(1)

                            except Exception as e:
                                logger.error(f"[{PROFILE_LABEL}]   ✗ Failed: {str(e)[:50]}")

                            await self.page.reload(wait_until='networkidle')
                            await asyncio.sleep(2)
                            break

                    except Exception:
                        continue

                if not found_ready:
                    break

            if page_downloaded > 0:
                logger.info(f"[{PROFILE_LABEL}] ✅ Downloaded {page_downloaded} reports from this page")
            return page_downloaded

        except Exception as e:
            logger.error(f"[{PROFILE_LABEL}] Error downloading from current page: {e}")
            return page_downloaded

    async def go_to_next_page(self):
        """Navigate to the next page by clicking the > arrow"""
        try:
            next_selectors = [
                'a:has-text(">")',
                'a:has-text("»")',
                'a:has-text("›")',
                'a img[alt*="Next"]',
                'a img[src*="next"]',
                'a img[src*="arrow"]',
                'input[type="image"][alt*="Next"]',
                'input[type="image"][src*="next"]',
            ]
            for selector in next_selectors:
                try:
                    elements = await self.page.query_selector_all(selector)
                    for element in elements:
                        if 'img' in selector or 'input' in selector:
                            link = await element.evaluate_handle('el => el.closest("a")')
                            if link:
                                element = await link.as_element()
                        if element:
                            is_disabled = await element.evaluate('''el => {
                                return el.disabled ||
                                       el.classList.contains("disabled") ||
                                       el.classList.contains("aspNetDisabled") ||
                                       (el.onclick && el.onclick.toString().includes("return false"));
                            }''')
                            if not is_disabled:
                                await element.click()
                                await self.page.wait_for_load_state('networkidle', timeout=15000)
                                await asyncio.sleep(2)
                                return True
                except Exception as e:
                    logger.debug(f"[{PROFILE_LABEL}] Selector {selector} failed: {e}")
                    continue

            try:
                table_links = await self.page.query_selector_all('table tr td a')
                for link in table_links:
                    try:
                        text = (await link.text_content() or "").strip()
                        if text == "2":
                            await link.click()
                            await self.page.wait_for_load_state('networkidle', timeout=15000)
                            await asyncio.sleep(2)
                            return True
                    except Exception:
                        continue
            except Exception as e:
                logger.debug(f"[{PROFILE_LABEL}] Error examining pagination: {e}")

            logger.info(f"[{PROFILE_LABEL}] No next page link found — on last page")
            return False

        except Exception as e:
            logger.error(f"[{PROFILE_LABEL}] Error clicking next page: {e}")
            return False

    async def download_all_reports(self):
        """Download all available reports"""
        logger.info(f"[{PROFILE_LABEL}] Starting download process...")

        total_downloaded          = 0
        start_time                = asyncio.get_event_loop().time()
        consecutive_empty_checks  = 0

        while True:
            elapsed = asyncio.get_event_loop().time() - start_time
            if elapsed > MAX_WAIT_TIME:
                logger.info(f"[{PROFILE_LABEL}] Reached maximum time ({MAX_WAIT_TIME}s). Stopping.")
                break

            data_rows  = await self.page.query_selector_all('tr:has(td)')
            ready_rows = []
            for row in data_rows:
                try:
                    row_text = await row.text_content()
                    if "Ready" in row_text and "Queued" not in row_text and "Failed" not in row_text and "Downloaded" not in row_text:
                        ready_rows.append(row)
                except:
                    continue

            if len(ready_rows) == 0:
                consecutive_empty_checks += 1
                logger.info(f"[{PROFILE_LABEL}] No Ready reports found (check {consecutive_empty_checks}/{MAX_CONSECUTIVE_EMPTY_CHECKS})")
                if consecutive_empty_checks >= MAX_CONSECUTIVE_EMPTY_CHECKS:
                    logger.info(f"[{PROFILE_LABEL}] ✓ All downloads complete!")
                    break
                logger.info(f"[{PROFILE_LABEL}] Waiting {CHECK_INTERVAL}s for more reports...")
                await asyncio.sleep(CHECK_INTERVAL)
                await self.page.reload(wait_until='networkidle')
                await asyncio.sleep(2)
                continue

            consecutive_empty_checks = 0
            logger.info(f"[{PROFILE_LABEL}] Found {len(ready_rows)} Ready reports")

            try:
                row           = ready_rows[0]
                download_link = await row.query_selector('a:has-text("Download")')
                if not download_link:
                    logger.warning(f"[{PROFILE_LABEL}] Could not find Download link in Ready row")
                    await asyncio.sleep(2)
                    continue

                cells = await row.query_selector_all('td')
                title = "Report"
                if len(cells) > 1:
                    title_text = await cells[1].text_content()
                    if title_text:
                        title = title_text.strip()[:50]

                logger.info(f"[{PROFILE_LABEL}] ⬇️  Downloading: {title}")
                await download_link.scroll_into_view_if_needed()
                await asyncio.sleep(0.5)

                async with self.page.expect_download(timeout=120000) as download_info:
                    await download_link.click()

                download = await download_info.value
                filename = download.suggested_filename
                filepath = os.path.join(DOWNLOAD_DIR, filename)
                await download.save_as(filepath)
                total_downloaded += 1
                logger.info(f"[{PROFILE_LABEL}]   ✓ Downloaded ({total_downloaded}): {filename}")
                await asyncio.sleep(2)
                await self.page.reload(wait_until='networkidle')
                await asyncio.sleep(2)

            except Exception as e:
                logger.error(f"[{PROFILE_LABEL}] Failed to download: {e}")
                await asyncio.sleep(2)
                await self.page.reload(wait_until='networkidle')
                await asyncio.sleep(2)
                continue

        logger.info(f"\n{'='*60}")
        logger.info(f"[{PROFILE_LABEL}] 📊 DOWNLOAD COMPLETE!")
        logger.info(f"{'='*60}")
        logger.info(f"[{PROFILE_LABEL}] ✅ Downloaded {total_downloaded} Reports")
        logger.info(f"[{PROFILE_LABEL}] 📁 Files saved to: {DOWNLOAD_DIR}")
        logger.info(f"{'='*60}")

    async def run(self):
        """Main execution flow"""
        async with async_playwright() as p:
            try:
                logger.info(f"[{PROFILE_LABEL}] Launching browser...")
                os.makedirs(BROWSER_DATA_DIR, exist_ok=True)

                is_ci         = os.getenv('CI') == 'true' or os.getenv('GITHUB_ACTIONS') == 'true'
                headless_mode = is_ci

                if is_ci:
                    logger.info(f"[{PROFILE_LABEL}] 🤖 Running in CI — headless mode")
                else:
                    logger.info(f"[{PROFILE_LABEL}] 💻 Running locally — browser will be visible")

                self.context = await p.chromium.launch_persistent_context(
                    user_data_dir=BROWSER_DATA_DIR,
                    headless=headless_mode,
                    accept_downloads=True,
                    viewport={"width": 1920, "height": 1080},
                    args=[
                        '--disable-blink-features=AutomationControlled',
                        '--log-level=3',
                        '--disable-logging',
                        '--no-sandbox',
                        '--disable-gpu'
                    ]
                )

                if self.context.pages:
                    self.page = self.context.pages[0]
                else:
                    self.page = await self.context.new_page()

                self.page.on("console",   lambda msg: None)
                self.page.on("pageerror", lambda err: None)
                self.page.set_default_timeout(60000)

                await self.setup()

                await self.login()
                await self.navigate_to_reports()
                await self.select_current_week()
                await self.switch_to_multi_store()

                stores_selected = await self.select_all_stores()
                if not stores_selected:
                    logger.error(f"[{PROFILE_LABEL}] ✗ Failed to select stores — cannot continue")
                    raise Exception("Store selection failed")

                await self.generate_all_reports()
                await self.navigate_to_download_center()
                await self.download_all_reports()

                logger.info(f"[{PROFILE_LABEL}] ✓ All done! Process complete.")
                logger.info(f"[{PROFILE_LABEL}] Keeping browser open for 10 seconds...")
                await asyncio.sleep(10)

            except Exception as e:
                logger.error(f"[{PROFILE_LABEL}] Error during execution: {e}", exc_info=True)
                if self.page:
                    await self.page.screenshot(path=f'error_screenshot_{PROFILE}.png')
                    logger.info(f"[{PROFILE_LABEL}] Error screenshot saved: error_screenshot_{PROFILE}.png")
                raise
            finally:
                if self.context:
                    await self.context.close()


async def main():
    """Entry point"""
    global OVERRIDE_WEEK, OVERRIDE_YEAR
    global PROFILE, SITE_USERNAME, SITE_PASSWORD, DOWNLOAD_DIR, BROWSER_DATA_DIR, PROFILE_LABEL

    parser = argparse.ArgumentParser(
        description='Download WSR reports for acquisition stores from Jimmy Johns Macromatix portal',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Kerr-McCauley, automatic week:
  python wsr_download_acquisition.py --profile km

  # MikLin/Mulligan, specific week:
  python wsr_download_acquisition.py --profile mm --week 46 --year 2025
        """
    )
    parser.add_argument('--profile', choices=['km', 'mm'], default='km',
                        help='Store group profile: km=Kerr-McCauley, mm=MikLin/Mulligan')
    parser.add_argument('--week', type=int, help='Week number (1-52) to download')
    parser.add_argument('--year', type=int, help='Year (e.g., 2025) to download')

    args = parser.parse_args()

    # Apply profile — re-resolve all globals based on the CLI argument
    os.environ['WSR_PROFILE'] = args.profile
    PROFILE = args.profile

    if PROFILE == 'mm':
        SITE_USERNAME    = os.getenv('MM_SITE_USERNAME')
        SITE_PASSWORD    = os.getenv('MM_SITE_PASSWORD')
        DOWNLOAD_DIR     = os.path.join(os.getcwd(), 'wsr_downloads_mm')
        BROWSER_DATA_DIR = os.path.join(os.getcwd(), 'browser_data_mm')
        PROFILE_LABEL    = 'MikLin/Mulligan'
    else:
        SITE_USERNAME    = os.getenv('KM_SITE_USERNAME')
        SITE_PASSWORD    = os.getenv('KM_SITE_PASSWORD')
        DOWNLOAD_DIR     = os.path.join(os.getcwd(), 'wsr_downloads_km')
        BROWSER_DATA_DIR = os.path.join(os.getcwd(), 'browser_data_km')
        PROFILE_LABEL    = 'Kerr-McCauley'

    if (args.week and not args.year) or (args.year and not args.week):
        logger.error("❌ ERROR: Both --week and --year must be provided together")
        parser.print_help()
        return

    if args.week and (args.week < 1 or args.week > 52):
        logger.error(f"❌ ERROR: Week must be between 1 and 52, got {args.week}")
        return

    if args.week and args.year:
        OVERRIDE_WEEK = args.week
        OVERRIDE_YEAR = args.year

    if not SITE_USERNAME or not SITE_PASSWORD:
        logger.error(f"[{PROFILE_LABEL}] Please set {PROFILE.upper()}_SITE_USERNAME and {PROFILE.upper()}_SITE_PASSWORD in .env")
        return

    logger.info("=" * 60)
    if OVERRIDE_WEEK and OVERRIDE_YEAR:
        logger.info(f"🎯 MANUAL OVERRIDE MODE — Week {OVERRIDE_WEEK}, Year {OVERRIDE_YEAR}")
    else:
        logger.info("📅 AUTOMATIC MODE — Current/prior week based on today")
    logger.info(f"👤 Profile: {PROFILE_LABEL}")
    logger.info("=" * 60)

    downloader = WSRDownloader()
    await downloader.run()


if __name__ == "__main__":
    asyncio.run(main())
