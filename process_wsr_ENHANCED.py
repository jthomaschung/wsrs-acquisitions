# TEST COMMENT - DELETE ME
#!/usr/bin/env python3
"""
WSR Processing Tool v4 - SHIFT-LEVEL + DAILY AGGREGATION
Extracts sales, inventory, labor, deductions, and financial reconciliation data
PLUS generates pre-aggregated DAILY tables for fast dashboard queries

UPLOADS BOTH:
- Shift-level (AM/PM) tables: wsr_sales, wsr_labor, wsr_financial, wsr_dmr
- Pre-aggregated DAILY tables: wsr_sales_daily, wsr_labor_daily, wsr_financial_daily, wsr_dmr_daily
- Metadata: wsr_headers
- Inventory: wsr_inventory (only when complete week detected)
- Inventory Unit Costs: wsr_inventory_unit_costs (tracked item prices, complete week only)

This gives maximum flexibility:
- Use shift-level tables for detailed AM/PM analysis
- Use daily aggregated tables for fast dashboard performance
"""

import os
import sys
import json
import argparse
import zipfile
import shutil
import tempfile
import time
import traceback
import re
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from collections import defaultdict

import pandas as pd
import numpy as np
from dotenv import load_dotenv

# Import WSR Email Notifier for sophisticated email reports
try:
    from wsr_email_notifier import WSREmailNotifier
    EMAIL_NOTIFIER_AVAILABLE = True
except ImportError:
    EMAIL_NOTIFIER_AVAILABLE = False
    print("Warning: wsr_email_notifier not found - email notifications will be limited")

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

# Load environment variables
load_dotenv()

# Supabase client
try:
    from supabase import create_client, Client
    SUPABASE_AVAILABLE = True
except ImportError:
    SUPABASE_AVAILABLE = False
    print("Warning: supabase-py not installed. Run: pip install supabase")

# Color support for Windows
try:
    import colorama
    colorama.init()
except ImportError:
    pass


class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    MAGENTA = '\033[95m'
    RESET = '\033[0m'
    BOLD = '\033[1m'


class SupabaseUploader:
    """Handles uploading WSR data to Supabase with proper column structures"""
    
    def __init__(self):
        if not SUPABASE_AVAILABLE:
            raise ImportError("supabase-py not installed. Run: pip install supabase")
        
        self.url = os.getenv('SUPABASE_URL')
        self.key = os.getenv('SUPABASE_KEY')
        
        if not self.url or not self.key:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in environment variables")
        
        self.client: Client = create_client(self.url, self.key)
        
        print(f"{Colors.GREEN}✓ Supabase client initialized{Colors.RESET}")
    
    def upload_parsed_data(self, result: Dict[str, Any], batch_size: int = 500) -> Dict[str, Any]:
        """
        Upload all parsed data to Supabase
        
        Args:
            result: Dictionary with keys like 'wsr_sales_daily', 'wsr_labor_daily', 'wsr_dmr', etc.
            batch_size: Number of records to upload per batch
        
        Returns:
            Dictionary with upload statistics
        """
        upload_results = {
            'total_records': 0,
            'successful': 0,
            'failed': 0,
            'by_table': {},
            'errors': [],
            'duration': 0,
            'skipped_tables': []
        }
        
        start_time = time.time()
        
        # All tables to upload - both shift-level AND aggregated
        # Order matters: headers first, then raw data, then aggregated
        all_tables = [
            'wsr_headers',           # Metadata
            'wsr_sales',             # Shift-level (AM/PM)
            'wsr_labor',             # Shift-level (AM/PM)
            'wsr_labor_metrics',     # Shift-level labor metrics from Weekly Sales
            'wsr_financial',         # Shift-level (AM/PM)
            'wsr_inventory',         # Only uploaded if complete week
            'wsr_inventory_unit_costs',  # Item-level unit costs (complete week only)
            'wsr_sales_daily',       # Aggregated daily
            'wsr_labor_daily',       # Aggregated daily
            'wsr_financial_daily',   # Aggregated daily
            'wsr_dmr',               # Shift-level DMR
            'wsr_dmr_daily',         # Aggregated daily DMR
            'wsr_labor_cost_summary' # Daily labor cost categories from Sales Summary
        ]
        
        for table_name in all_tables:
            data = result.get(table_name, [])
            
            # Skip inventory if not present (incomplete week)
            if table_name == 'wsr_inventory' and not data:
                print(f"\n{Colors.YELLOW}⊘ Skipping wsr_inventory (incomplete week){Colors.RESET}")
                upload_results['skipped_tables'].append('wsr_inventory')
                continue
            
            # Skip inventory unit costs if not present (incomplete week)
            if table_name == 'wsr_inventory_unit_costs' and not data:
                print(f"\n{Colors.YELLOW}⊘ Skipping wsr_inventory_unit_costs (incomplete week){Colors.RESET}")
                upload_results['skipped_tables'].append('wsr_inventory_unit_costs')
                continue
            
            # Skip if no data
            if not data:
                continue
            
            print(f"\n{Colors.BLUE}Uploading {len(data)} records to {table_name}...{Colors.RESET}")
            
            table_start = time.time()
            table_results = self._upload_to_table(data, table_name, batch_size)
            table_duration = time.time() - table_start
            
            upload_results['by_table'][table_name] = {
                'records': table_results['successful'],
                'failed': table_results['failed'],
                'duration': table_duration
            }
            
            upload_results['successful'] += table_results['successful']
            upload_results['failed'] += table_results['failed']
            upload_results['errors'].extend(table_results['errors'])
            
            if table_results['successful'] > 0:
                print(f"{Colors.GREEN}  ✓ Uploaded {table_results['successful']:,} records in {table_duration:.1f}s{Colors.RESET}")
            if table_results['failed'] > 0:
                print(f"{Colors.RED}  ✗ Failed: {table_results['failed']} records{Colors.RESET}")
        
        upload_results['total_records'] = upload_results['successful'] + upload_results['failed']
        upload_results['duration'] = time.time() - start_time
        
        return upload_results
    
    def _upload_to_table(self, data: List[Dict], table_name: str, batch_size: int) -> Dict[str, Any]:
        """Upload data to a specific table in batches using UPSERT to handle duplicates"""
        results = {
            'successful': 0,
            'failed': 0,
            'errors': [],
            'upserted': 0
        }
        
        # Define unique constraint columns for each table (for UPSERT conflict resolution)
        conflict_columns = {
            'wsr_headers': 'store_number,week_ending',
            'wsr_inventory': 'store_number,week_ending,category',
            'wsr_inventory_unit_costs': 'store_number,week_ending,item_name',
            'wsr_sales': 'store_number,date,shift,category',
            'wsr_labor': 'store_number,date,shift,labor_type',
            'wsr_labor_metrics': 'store_number,date,shift',
            'wsr_labor_cost_summary': 'store_number,week_number,year,category',
            'wsr_financial': 'store_number,date,shift,category',
            'wsr_dmr': 'store_number,date,shift',
            'wsr_sales_daily': 'store_number,date,category',
            'wsr_labor_daily': 'store_number,date',
            'wsr_financial_daily': 'store_number,date,category',
            'wsr_dmr_daily': 'store_number,date'
        }
        
        # Get conflict columns for this table
        on_conflict = conflict_columns.get(table_name)
        
        if not on_conflict:
            print(f"{Colors.YELLOW}  ⚠️  Warning: No conflict columns defined for {table_name}, using insert instead of upsert{Colors.RESET}")
        
        # wsr_labor_cost_summary has two competing unique indexes. Delete only by
        # (store, week, year) — specific enough to avoid wiping other weeks' data.
        if table_name == 'wsr_labor_cost_summary' and data:
            combos = set(
                (r.get('store_number'), r.get('week_number'), r.get('year'))
                for r in data
                if r.get('store_number') and r.get('week_number') and r.get('year')
            )
            for store_num, week_num, year_val in combos:
                try:
                    self.client.table(table_name).delete()\
                        .eq('store_number', store_num)\
                        .eq('week_number', week_num)\
                        .eq('year', year_val)\
                        .execute()
                except Exception as e:
                    print(f"  ⚠️  Pre-delete failed for store {store_num} week {week_num}: {e}")

        # Process in batches
        total_batches = (len(data) + batch_size - 1) // batch_size
        
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            
            try:
                # Convert date objects to strings for Supabase
                prepared_batch = []
                for record in batch:
                    prepared_record = record.copy()
                    for key, value in prepared_record.items():
                        if hasattr(value, 'strftime'):  # It's a date/datetime object
                            prepared_record[key] = value.strftime('%Y-%m-%d')
                    prepared_batch.append(prepared_record)
                
                # wsr_labor_cost_summary: pre-delete already ran above, use plain insert
                if table_name == 'wsr_labor_cost_summary':
                    response = self.client.table(table_name).insert(prepared_batch).execute()
                # Use UPSERT for all other tables
                elif on_conflict:
                    response = self.client.table(table_name).upsert(
                        prepared_batch,
                        on_conflict=on_conflict
                    ).execute()
                else:
                    # Fallback to insert if no conflict columns defined
                    response = self.client.table(table_name).insert(prepared_batch).execute()
                
                results['successful'] += len(batch)
                results['upserted'] += len(batch)
                
                # Show progress for large uploads
                if total_batches > 5 and batch_num % 5 == 0:
                    progress = (batch_num / total_batches) * 100
                    print(f"  Progress: {batch_num}/{total_batches} batches ({progress:.0f}%)")
                
            except Exception as e:
                error_msg = f"Batch {batch_num} failed: {str(e)}"
                results['errors'].append(error_msg)
                results['failed'] += len(batch)
                print(f"{Colors.RED}  ✗ {error_msg}{Colors.RESET}")
        
        return results


class WSRParserV4:
    """Multi-tab WSR parser with aggregation tables for fast queries"""
    
    def __init__(self):
        self.metadata = {}
        
        # Category mappings for revenue grouping
        self.INSHOP_CATEGORIES = ['IN-Sub', 'IN-Club', 'IN-Pop', 'IN-Side', 'IN-Combos / Kids 1/2 off']
        self.DELIVERY_CATEGORIES = ['DEL-Sub', 'DEL-Club', 'DEL-Pop', 'DEL-Side', 'DEL-Combos / Kids 1/2 off']
        self.CATERING_CATEGORIES = ['Box Lunch', 'Platters / Mini Jimmys']
        self.DESSERT_CATEGORIES = ['Cookie']
        self.OTHER_REVENUE_CATEGORIES = ['Day Old Bread', 'Fresh Bread', 'Delivery Fee', 'Other', 'Modifiers']
        self.DEDUCTION_CATEGORIES = [
            'Net Employee Freebies', 'Net Manager Freebies', 'Sampling',
            'Waste', 'Other Promo', 'Loyalty / Coupon'
        ]
        
        # NEW: Line item categories for unified aggregation
        self.LINE_ITEM_CATEGORIES = {
            'revenue': [
                'IN-Sub', 'IN-Club', 'IN-Pop', 'IN-Side', 'IN-Combos / Kids 1/2 off',
                'DEL-Sub', 'DEL-Club', 'DEL-Pop', 'DEL-Side', 'DEL-Combos / Kids 1/2 off',
                'Day Old Bread', 'Fresh Bread', 'Cookie',
                'Box Lunch', 'Platters / Mini Jimmys',
                'Delivery Fee', 'Other', 'Modifiers'
            ],
            'calculation': [
                'Total of Above', 'OVER-RINGS', 'Adjusted Sales', 'Royalty Sales'
            ],
            'deduction': [
                'Net Employee Freebies', 'Net Manager Freebies', 'Sampling',
                'Waste', 'Other Promo', 'Loyalty / Coupon'
            ],
            '3pf': [
                'DoorDash', 'GrubHub', 'UberEats'
            ],
            'financial': [
                'Sales Tax', 'Other Tax/Fee', 'Over Rings Tax', 'Freebie/Promo Tax',
                'Cash Payouts', 'A/R Due', 'A/R Paid', 'A/R Drivers',
                'A/R Due CC (InShop) V/MC/D', 'A/R Due CC (InShop) Amex',
                'A/R Due CC (MOTO) V/MC/D', 'A/R Due CC (MOTO) Amex',
                'A/R Due CC (ONLINE) V/MC/D', 'A/R Due CC (ONLINE) Amex',
                'Other Payment Types',
                'Gift Cards Issued', 'Gift Cards Redeemed',
                'NSF Deposit', 'Expected Deposit', 'Total Deposit', 'Cash Over/(Under)'
            ],
            'metric': [
                '# Of Sales', '# Of Checks',
                'Labor $', 'Labor %', 'Labor OverTime $',
                'Total Online Orders', 'Online Cash',
                'PDQ/Mx Sales Check', 'PDQ/Mx Tax Check', 'Gift Cards Total'
            ]
        }
    def test_method_exists(self):
        """Quick test"""
        return True    
    def parse_file(self, file_path: str) -> Dict[str, Any]:
        """Parse all relevant tabs from WSR Excel file"""
        try:
            # Extract metadata from Weekly Sales tab
            self.metadata = self._extract_metadata(file_path)
            
            # Parse each data type
            sales_data = self._parse_sales(file_path)
            inventory_data = self._parse_inventory(file_path)
            inventory_unit_costs = self._parse_inventory_unit_costs(file_path)
            labor_data = self._parse_labor(file_path)
            labor_metrics = self._parse_labor_metrics(file_path)  # NEW: Labor $, Labor %, Labor OT from Weekly Sales
            financial_data = self._parse_financial(file_path)
            dmr_data = self._parse_dmr(file_path)
            labor_cost_summary = self._parse_labor_cost_summary(file_path)
            
            return {
                'metadata': self.metadata,
                'sales_data': sales_data,
                'inventory_data': inventory_data,
                'inventory_unit_costs': inventory_unit_costs,
                'labor_data': labor_data,
                'labor_metrics': labor_metrics,  # NEW
                'financial_data': financial_data,
                'dmr_data': dmr_data,
                'labor_cost_summary': labor_cost_summary,
                'parse_timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            raise Exception(f"Error parsing WSR file: {str(e)}")
    def _parse_labor_cost_summary(self, file_path: str, audit_mode: bool = True) -> List[Dict[str, Any]]:
        """Parse labor cost summary data from Sales Summary tab with deep auditing"""
        labor_cost_data = []
        yesterday = datetime.now() - timedelta(days=1)
        report_date = yesterday.strftime('%Y-%m-%d')
        
        try:
            df = pd.read_excel(file_path, sheet_name='Sales Summary', header=None)
            
            if audit_mode:
                print(f"\n{Colors.CYAN}{'='*70}")
                print(f"LABOR COST SUMMARY PARSING AUDIT")
                print(f"{'='*70}{Colors.RESET}")
                print(f"File: {os.path.basename(file_path)}")
                print(f"Report Date: {report_date}")
                print(f"Sheet Dimensions: {len(df)} rows x {len(df.columns)} columns\n")
            
            # Search for "Labor Cost Summary" header
            header_row = None
            header_col = None
            
            for row_idx in range(len(df)):
                for col_idx in range(len(df.columns)):
                    if pd.notna(df.iloc[row_idx, col_idx]):
                        cell_value = str(df.iloc[row_idx, col_idx]).strip()
                        if 'labor' in cell_value.lower() and ('cost' in cell_value.lower() or 'summary' in cell_value.lower()):
                            header_row = row_idx
                            header_col = col_idx
                            print(f"  {Colors.GREEN}✓ Found 'Labor Cost Summary' at row {row_idx}, column {col_idx}{Colors.RESET}")
                            break
                if header_row is not None:
                    break
            
            if header_row is None:
                print(f"  {Colors.YELLOW}⚠ Labor Cost Summary section not found{Colors.RESET}")
                return labor_cost_data
            
            data_start_row = header_row + 1
            data_end_row = len(df)
            
            for row_idx in range(data_start_row, min(len(df), data_start_row + 50)):
                if pd.isna(df.iloc[row_idx, header_col]):
                    data_end_row = row_idx
                    break
                cell_value = str(df.iloc[row_idx, header_col]).strip() if pd.notna(df.iloc[row_idx, header_col]) else ""
                if 'total labor cost' in cell_value.lower():
                    data_end_row = row_idx + 1
                    break
            
            print(f"\n  {Colors.CYAN}Parsing labor cost rows {data_start_row} to {data_end_row}{Colors.RESET}")
            
            if audit_mode:
                print(f"\n{Colors.CYAN}{'─'*70}")
                print(f"ROW-BY-ROW AUDIT")
                print(f"{'─'*70}{Colors.RESET}\n")
            
            for row_idx in range(data_start_row, data_end_row):
                category_name = str(df.iloc[row_idx, header_col]).strip() if pd.notna(df.iloc[row_idx, header_col]) else None
                
                if audit_mode:
                    print(f"{Colors.BLUE}Row {row_idx}:{Colors.RESET}")
                    print(f"  Category (col {header_col}): '{category_name}'")
                
                if not category_name or category_name.startswith(('=', '+', '-')):
                    if audit_mode:
                        print(f"  {Colors.YELLOW}⊗ SKIPPED - Invalid category{Colors.RESET}\n")
                    continue
                
                # Skip summary/total rows to avoid double-counting
                if 'total' in category_name.lower():
                    if audit_mode:
                        print(f"  {Colors.YELLOW}⊗ SKIPPED - Summary row (contains 'Total'){Colors.RESET}\n")
                    continue
                
                amount = None
                found_amount_at_col = None
                
                if audit_mode:
                    print(f"  Checking adjacent columns for dollar amount:")
                
                for offset in range(1, 4):
                    col_idx = header_col + offset
                    if col_idx < len(df.columns):
                        val = df.iloc[row_idx, col_idx]
                        
                        if audit_mode:
                            val_display = f"'{val}'" if pd.notna(val) else "BLANK"
                            val_type = type(val).__name__ if pd.notna(val) else "NaN"
                            print(f"    Col {col_idx} (offset +{offset}): {val_display} [type: {val_type}]")
                        
                        if pd.notna(val):
                            try:
                                if isinstance(val, (int, float)):
                                    if val < 1.0:
                                        if audit_mode:
                                            print(f"      {Colors.YELLOW}→ Rejected: Value < 1.0{Colors.RESET}")
                                        continue
                                    amount = float(val)
                                    found_amount_at_col = col_idx
                                    if audit_mode:
                                        print(f"      {Colors.GREEN}→ ACCEPTED as ${amount:,.2f}{Colors.RESET}")
                                    break
                                elif isinstance(val, str):
                                    if '%' in val:
                                        if audit_mode:
                                            print(f"      {Colors.YELLOW}→ Rejected: Contains '%'{Colors.RESET}")
                                        continue
                                    cleaned = val.replace('$', '').replace(',', '').strip()
                                    if cleaned and cleaned not in ['-', '']:
                                        amount = float(cleaned)
                                        found_amount_at_col = col_idx
                                        if audit_mode:
                                            print(f"      {Colors.GREEN}→ ACCEPTED as ${amount:,.2f}{Colors.RESET}")
                                        break
                            except (ValueError, AttributeError) as e:
                                if audit_mode:
                                    print(f"      {Colors.YELLOW}→ Rejected: {str(e)}{Colors.RESET}")
                                continue
                
                if amount is not None and amount > 0:
                    record = {
                        'store_number': self.metadata.get('store_number'),
                        'date': report_date,
                        'week_ending': self.metadata.get('week_ending'),
                        'week_number': self.metadata.get('week_number'),
                        'year': self.metadata.get('year'),
                        'category': category_name,
                        'amount': amount,
                        'uploaded_at': datetime.now().isoformat()
                    }
                    labor_cost_data.append(record)
                    if audit_mode:
                        print(f"  {Colors.GREEN}✓ PARSED SUCCESSFULLY{Colors.RESET}")
                        print(f"    Category: {category_name}")
                        print(f"    Amount: ${amount:,.2f}\n")
                else:
                    if audit_mode:
                        print(f"  {Colors.RED}✗ NO VALID AMOUNT FOUND{Colors.RESET}\n")
            
            if audit_mode:
                print(f"{Colors.CYAN}{'─'*70}")
                print(f"PARSING SUMMARY")
                print(f"{'─'*70}{Colors.RESET}")
            
            if labor_cost_data:
                total = sum(r['amount'] for r in labor_cost_data)
                print(f"  {Colors.GREEN}✓ Parsed {len(labor_cost_data)} labor cost categories{Colors.RESET}")
                print(f"    Total: ${total:,.2f}\n")
                if audit_mode:
                    for record in labor_cost_data:
                        print(f"    • {record['category']}: ${record['amount']:,.2f}")
            else:
                print(f"  {Colors.YELLOW}⚠ No labor cost data found{Colors.RESET}")
            
            if audit_mode:
                print(f"{Colors.CYAN}{'='*70}{Colors.RESET}\n")
        
        except ValueError:
            print(f"  {Colors.YELLOW}⚠ Sales Summary tab not found{Colors.RESET}")
        except Exception as e:
            print(f"  {Colors.RED}✗ Error: {e}{Colors.RESET}")
            traceback.print_exc()
        
        return labor_cost_data
    def _extract_metadata(self, file_path: str) -> Dict[str, Any]:
        """Extract store metadata from Weekly Sales tab and filename"""
        metadata = {}
        
        # Parse fiscal week and year from filename (primary source for week_number)
        filename_week = None
        filename_year = None
        if file_path:
            filename = Path(file_path).stem
            # Try multiple patterns: "Week 1, 2024", "Week_1__2024", "Week 1 2024", etc.
            week_match = re.search(r'Week[_\s]+(\d+)[_,\s]+(\d{4})', filename, re.IGNORECASE)
            if week_match:
                filename_week = int(week_match.group(1))
                filename_year = int(week_match.group(2))
                metadata['week_number'] = filename_week
                metadata['year'] = filename_year
        
        # Read header info from Weekly Sales tab
        df = pd.read_excel(file_path, sheet_name='Weekly Sales', header=None, nrows=5)
        
        # Week ending date (row 1, col 2) - PRIMARY SOURCE for week_ending
        week_ending = df.iloc[1, 2] if pd.notna(df.iloc[1, 2]) else None
        if week_ending:
            week_date = pd.to_datetime(week_ending)
            if isinstance(week_ending, str):
                week_date = pd.to_datetime(week_ending, format='%m/%d/%Y', errors='coerce')
                if pd.isna(week_date):
                    week_date = pd.to_datetime(week_ending)
            
            original_date = week_date  # Save for validation
            
            # Ensure Tuesday (JJ weeks end on Tuesday)
            if week_date.weekday() != 1:
                days_ahead = 1 - week_date.weekday()
                if days_ahead <= 0:
                    days_ahead += 7
                week_date = week_date + timedelta(days=days_ahead)
                
                # Warn if we had to adjust the date
                print(f"{Colors.YELLOW}⚠ Week ending date was {original_date.strftime('%A %Y-%m-%d')}, "
                      f"adjusted to Tuesday {week_date.strftime('%Y-%m-%d')}{Colors.RESET}")
            
            metadata['week_ending'] = week_date.strftime('%Y-%m-%d')
            metadata['week_ending_date'] = week_date.date()  # Date object for DMR timedelta calculations
            
            # Use week_date year if not in filename
            if 'year' not in metadata:
                metadata['year'] = week_date.year
            
            # CALCULATE week_number from date if not found in filename
            if 'week_number' not in metadata:
                calculated_week = self._calculate_week_number(week_date)
                metadata['week_number'] = calculated_week
                print(f"{Colors.CYAN}ℹ Week number not in filename, calculated from date: Week {calculated_week}{Colors.RESET}")
            
            # VALIDATION: Check if filename week and date are consistent
            # Calculate expected week_ending date from filename week number
            if filename_week and filename_year:
                # Estimate: Week 1 typically starts around Jan 3-9
                # Each week is 7 days, so Week N ends roughly N*7 days after Week 1
                # This is approximate validation, not exact
                
                # Simple sanity check: weeks should be in same month range
                expected_month_range = {
                    range(1, 5): [1],      # Weeks 1-4 in January
                    range(5, 9): [2],      # Weeks 5-8 in February
                    range(9, 13): [3],     # Weeks 9-12 in March
                    range(13, 18): [4],    # Weeks 13-17 in April
                    range(18, 22): [5],    # Weeks 18-21 in May
                    range(22, 27): [6],    # Weeks 22-26 in June
                    range(27, 31): [7],    # Weeks 27-30 in July
                    range(31, 35): [8],    # Weeks 31-34 in August
                    range(35, 40): [9],    # Weeks 35-39 in September
                    range(40, 44): [10],   # Weeks 40-43 in October
                    range(44, 48): [11],   # Weeks 44-47 in November
                    range(48, 53): [12],   # Weeks 48-52 in December
                }
                
                actual_month = week_date.month
                expected_months = None
                for week_range, months in expected_month_range.items():
                    if filename_week in week_range:
                        expected_months = months
                        break
                
                if expected_months and actual_month not in expected_months:
                    # Significant mismatch detected
                    print(f"{Colors.RED}{'='*70}")
                    print(f"❌ WEEK NUMBER MISMATCH DETECTED")
                    print(f"{'='*70}")
                    print(f"File: {Path(file_path).name}")
                    print(f"Filename says: Week {filename_week}, {filename_year}")
                    print(f"Excel date is: {week_date.strftime('%Y-%m-%d')} (month {actual_month})")
                    print(f"Expected month(s) for Week {filename_week}: {expected_months}")
                    print(f"This suggests filename and Excel date are out of sync!")
                    print(f"{'='*70}{Colors.RESET}")
        
        # Store location (row 2, col 2)
        store_location = df.iloc[2, 2] if pd.notna(df.iloc[2, 2]) else None
        if store_location:
            metadata['store_location'] = str(store_location)
            parts = store_location.split(',')
            if len(parts) >= 2:
                metadata['city'] = parts[0].split('-')[-1].strip()
                metadata['state'] = parts[-1].strip()
        
        # Store number (row 3, col 2)
        store_number = df.iloc[3, 2] if pd.notna(df.iloc[3, 2]) else None
        if store_number:
            metadata['store_number'] = str(store_number).strip()
        
        # General Manager (row 4, col 2)
        gm_name = df.iloc[4, 2] if pd.notna(df.iloc[4, 2]) else None
        if gm_name:
            metadata['general_manager'] = str(gm_name).strip()
        
        return metadata
    
    def _calculate_week_number(self, week_ending_date: datetime) -> int:
        """
        Calculate JJ fiscal week number from a week-ending date (Tuesday)
        
        JJ uses a 4-5-4 retail calendar:
        - 52 weeks per year, 13 periods of 4 weeks each
        - Weeks run Wednesday-Tuesday
        - Week 1 starts on the Wednesday of the week containing January 1st
        
        Examples:
        - 2024: Week 1 started 01/03/2024 (Jan 1 was Monday)
        - 2025: Week 1 started 01/01/2025 (Jan 1 was Wednesday)
        - 2026: Week 1 starts 12/31/2025 (Jan 1, 2026 is Thursday)
        
        Args:
            week_ending_date: A Tuesday representing the end of a JJ week
            
        Returns:
            Week number (1-52)
        """
        # First, determine which fiscal year this date belongs to
        # A date could belong to the fiscal year that started in the previous calendar year
        
        # Try current calendar year first
        year = week_ending_date.year
        week_1_start = self._get_fiscal_year_start(year)
        week_1_ends = week_1_start + timedelta(days=6)
        
        # If the date is before Week 1 ends, it belongs to previous fiscal year
        if week_ending_date < week_1_ends:
            year = year - 1
            week_1_start = self._get_fiscal_year_start(year)
            week_1_ends = week_1_start + timedelta(days=6)
        
        # Calculate week number
        days_since_week_1_end = (week_ending_date - week_1_ends).days
        weeks_since = days_since_week_1_end // 7
        week_number = weeks_since + 1
        
        # Cap at week 52
        if week_number > 52:
            week_number = 52
        
        return week_number
    
    def _get_fiscal_year_start(self, fiscal_year: int) -> datetime:
        """
        Get the Wednesday that starts the fiscal year (Week 1)
        
        Rule: Week 1 starts on the Wednesday of the week containing January 1st
        
        Args:
            fiscal_year: The fiscal year (e.g., 2024, 2025)
            
        Returns:
            datetime of the Wednesday that starts Week 1
        """
        jan_1 = datetime(fiscal_year, 1, 1)
        jan_1_weekday = jan_1.weekday()  # 0=Mon, 1=Tue, 2=Wed, 3=Thu, 4=Fri, 5=Sat, 6=Sun
        
        # Calculate days to Wednesday
        days_to_wednesday = (2 - jan_1_weekday) % 7
        
        # If Jan 1 is Thu/Fri/Sat/Sun, we want the PREVIOUS Wednesday (in December)
        if days_to_wednesday > 3:
            days_to_wednesday -= 7
        
        week_1_start = jan_1 + timedelta(days=days_to_wednesday)
        return week_1_start
    
    def _parse_sales(self, file_path: str) -> List[Dict[str, Any]]:
        """Parse sales data INCLUDING deductions and 3rd party from Weekly Sales tab"""
        df = pd.read_excel(file_path, sheet_name='Weekly Sales', header=None)
        
        sales_data = []
        
        # Build column mapping from dates and dayparts
        date_row = 8
        daypart_row = 9
        data_start_row = 10
        
        dates = df.iloc[date_row].tolist()
        dayparts = df.iloc[daypart_row].tolist()
        
        # Map date/daypart columns
        column_mapping = {}
        for i in range(3, min(17, len(dates))):
            if pd.notna(dates[i]) and i < len(dayparts):
                date_str = pd.to_datetime(dates[i]).strftime('%Y-%m-%d')
                if pd.notna(dayparts[i]):
                    column_mapping[i] = {'date': date_str, 'daypart': dayparts[i]}
                if i+1 < len(dayparts) and pd.notna(dayparts[i+1]):
                    column_mapping[i+1] = {'date': date_str, 'daypart': dayparts[i+1]}
        
        # Track parsing state
        passed_total = False
        
        # Define 3rd party platforms
        third_party_platforms = ['DoorDash', 'GrubHub', 'UberEats']
        
        # Parse sales rows INCLUDING deductions and 3rd party
        for row_idx in range(data_start_row, len(df)):
            if pd.notna(df.iloc[row_idx, 0]):
                sales_item = str(df.iloc[row_idx, 0]).strip()
                
                # Mark when we pass "Total of Above"
                if 'Total of Above' in sales_item:
                    passed_total = True
                    continue
                
                # Stop at Royalty Sales (end of deduction section)
                if '= Royalty Sales' in sales_item:
                    break
                
                # Skip calculated totals and section breaks
                if any(marker in sales_item for marker in [
                    '= Adjusted Sales',
                    '= Expected Deposit',
                    'TOTAL SALES',
                    'Grand Total'
                ]):
                    continue
                
                # Skip formula rows starting with = or +
                if sales_item.startswith(('=', '+')) or len(sales_item) == 0:
                    continue
                
                # Clean category name (remove leading symbols)
                clean_category = sales_item.lstrip('- +').strip()
                
                # Determine category type
                if not passed_total:
                    # Revenue items (before "Total of Above")
                    category_type = 'revenue'
                elif any(platform in clean_category for platform in third_party_platforms):
                    # 3rd party platforms (DoorDash, GrubHub, UberEats)
                    category_type = 'third_party'
                elif sales_item.startswith('-'):
                    # Deduction items (Employee Freebies, Waste, Loyalty, etc.)
                    category_type = 'deduction'
                else:
                    # Skip anything else
                    continue
                
                # Get quantity (column 2) - typically only for revenue items
                quantity = int(df.iloc[row_idx, 2]) if pd.notna(df.iloc[row_idx, 2]) else 0
                
                # Add daypart sales
                for col in range(3, min(17, len(df.columns))):
                    if col in column_mapping and isinstance(column_mapping[col], dict):
                        value = self._clean_currency(df.iloc[row_idx, col])
                        if value is not None and value != 0:
                            sales_data.append({
                                'date': column_mapping[col]['date'],
                                'shift': column_mapping[col]['daypart'],
                                'category': clean_category,
                                'category_type': category_type,
                                'sales_amount': value,
                                'quantity': quantity
                            })
        
        # Now look for additional metrics and 3rd party: # Of Sales, Total Online Orders, DoorDash, GrubHub, UberEats
        for row_idx in range(data_start_row, len(df)):
            if pd.notna(df.iloc[row_idx, 0]):
                sales_item = str(df.iloc[row_idx, 0]).strip()
                clean_item = sales_item.lstrip('- ').strip()
                
                # Look for # Of Sales row
                if '# Of Sales' in sales_item or '# of Sales' in sales_item:
                    for col in range(3, min(17, len(df.columns))):
                        if col in column_mapping and isinstance(column_mapping[col], dict):
                            value = self._clean_currency(df.iloc[row_idx, col])
                            if value is not None:
                                sales_data.append({
                                    'date': column_mapping[col]['date'],
                                    'shift': column_mapping[col]['daypart'],
                                    'category': '# Of Sales',
                                    'category_type': 'metric',
                                    'sales_amount': value,
                                    'quantity': 0
                                })
                
                # Look for Total Online Orders row (metric only, not revenue - already counted in other categories)
                elif 'Total Online Orders' in sales_item:
                    for col in range(3, min(17, len(df.columns))):
                        if col in column_mapping and isinstance(column_mapping[col], dict):
                            value = self._clean_currency(df.iloc[row_idx, col])
                            if value is not None and value != 0:
                                sales_data.append({
                                    'date': column_mapping[col]['date'],
                                    'shift': column_mapping[col]['daypart'],
                                    'category': 'Total Online Orders',
                                    'category_type': 'metric',
                                    'sales_amount': value,
                                    'quantity': 0
                                })
                
                # Look for 3rd party platforms (DoorDash, GrubHub, UberEats)
                elif clean_item in third_party_platforms:
                    for col in range(3, min(17, len(df.columns))):
                        if col in column_mapping and isinstance(column_mapping[col], dict):
                            value = self._clean_currency(df.iloc[row_idx, col])
                            if value is not None and value != 0:
                                sales_data.append({
                                    'date': column_mapping[col]['date'],
                                    'shift': column_mapping[col]['daypart'],
                                    'category': clean_item,
                                    'category_type': 'third_party',
                                    'sales_amount': value,
                                    'quantity': 0
                                })
        
        return sales_data
    
    def _parse_labor_metrics(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Parse Labor $, Labor %, and Labor OverTime $ from Weekly Sales tab
        Returns shift-level (AM/PM) labor metrics with date
        """
        df = pd.read_excel(file_path, sheet_name='Weekly Sales', header=None)
        
        labor_metrics = []
        
        # Build column mapping from dates and dayparts (same as sales)
        date_row = 8
        daypart_row = 9
        
        dates = df.iloc[date_row].tolist()
        dayparts = df.iloc[daypart_row].tolist()
        
        # Map date/daypart columns
        column_mapping = {}
        for i in range(3, min(17, len(dates))):
            if pd.notna(dates[i]):
                date_str = pd.to_datetime(dates[i]).strftime('%Y-%m-%d')
                # Map both AM and PM columns
                if i < len(dayparts) and pd.notna(dayparts[i]):
                    column_mapping[i] = {'date': date_str, 'daypart': dayparts[i]}
                if i+1 < len(dayparts) and pd.notna(dayparts[i+1]):
                    column_mapping[i+1] = {'date': date_str, 'daypart': dayparts[i+1]}
        
        # Labor metric rows to capture
        labor_rows = {
            'Labor $': 'labor_dollars',
            'Labor %': 'labor_percent',
            'Labor OverTime $': 'labor_overtime'
        }
        
        # Find and parse labor rows
        for row_idx in range(60, min(len(df), 75)):  # Labor section typically around row 65-67
            if pd.notna(df.iloc[row_idx, 0]):
                row_label = str(df.iloc[row_idx, 0]).strip()
                
                if row_label in labor_rows:
                    metric_name = labor_rows[row_label]
                    
                    # Extract values for each day/shift
                    for col in range(3, min(17, len(df.columns))):
                        if col in column_mapping:
                            value = self._clean_currency(df.iloc[row_idx, col])
                            
                            # For Labor %, keep as-is (it's already a percentage)
                            if metric_name == 'labor_percent' and value is not None:
                                # Value is already in percentage format (e.g., 31.98 means 31.98%)
                                pass
                            
                            if value is not None:  # Include zeros
                                labor_metrics.append({
                                    'date': column_mapping[col]['date'],
                                    'shift': column_mapping[col]['daypart'],
                                    'metric_name': metric_name,
                                    'metric_value': value
                                })
        
        return labor_metrics
    
    def _parse_financial(self, file_path: str) -> List[Dict[str, Any]]:
        """Parse financial reconciliation data from Weekly Sales tab"""
        df = pd.read_excel(file_path, sheet_name='Weekly Sales', header=None)
        
        financial_data = []
        
        # Build column mapping from dates and dayparts (same as sales)
        date_row = 8
        daypart_row = 9
        
        dates = df.iloc[date_row].tolist()
        dayparts = df.iloc[daypart_row].tolist()
        
        column_mapping = {}
        for i in range(3, min(17, len(dates))):
            if pd.notna(dates[i]) and i < len(dayparts):
                date_str = pd.to_datetime(dates[i]).strftime('%Y-%m-%d')
                if pd.notna(dayparts[i]):
                    column_mapping[i] = {'date': date_str, 'daypart': dayparts[i]}
                if i+1 < len(dayparts) and pd.notna(dayparts[i+1]):
                    column_mapping[i+1] = {'date': date_str, 'daypart': dayparts[i+1]}
        
        # Financial categories to capture (starting after "= Royalty Sales")
        financial_categories = [
            'Other Tax/Fee',
            'Sales Tax',
            'Over Rings Tax',
            'Freebie/Promo Tax',
            'Cash Payouts',
            'A/R Due',
            'A/R Paid',
            'A/R Drivers',
            'A/R Due CC (InShop) V/MC/D',
            'A/R Due CC (InShop) Amex',
            'A/R Due CC (MOTO) V/MC/D',
            'A/R Due CC (MOTO) Amex',
            'A/R Due CC (ONLINE) V/MC/D',
            'A/R Due CC (ONLINE) Amex',
            'Other Payment Types',
            'DoorDash',           # 3rd party
            'GrubHub',            # 3rd party
            'UberEats',           # 3rd party
            'Gift Cards Issued',
            'Gift Cards Redeemed',
            'NSF Deposit',
            'Expected Deposit',
            'Total Deposit',
            'Cash Over/(Under)'
        ]
        
        # Find where financial section starts (after "Royalty Sales")
        # Handle variations: "= Royalty Sales", "=Royalty Sales", "Royalty Sales"
        financial_start_row = None
        for row_idx in range(len(df)):
            if pd.notna(df.iloc[row_idx, 0]):
                cell_value = str(df.iloc[row_idx, 0]).strip().upper()
                # Check for various formats of Royalty Sales marker
                if any(marker in cell_value for marker in ['ROYALTY SALES', 'ROYALTYSALES', '=ROYALTY']):
                    financial_start_row = row_idx + 1
                    break
        
        if financial_start_row is None:
            # Enhanced diagnostic: show what we found instead
            sample_rows = []
            for row_idx in range(10, min(40, len(df))):
                if pd.notna(df.iloc[row_idx, 0]):
                    cell_text = str(df.iloc[row_idx, 0]).strip()
                    sample_rows.append(f"Row {row_idx}: {cell_text[:60]}")
            
            print(f"{Colors.RED}⚠️  Financial section NOT FOUND in {Path(file_path).name}{Colors.RESET}")
            print(f"{Colors.YELLOW}  Searched for: 'ROYALTY SALES', '=ROYALTY', 'ROYALTYSALES'{Colors.RESET}")
            if sample_rows:
                print(f"{Colors.YELLOW}  Sample rows from sheet (rows 10-40):{Colors.RESET}")
                for sample in sample_rows[:10]:  # Show first 10
                    print(f"{Colors.YELLOW}    {sample}{Colors.RESET}")
            return []
        else:
            # Success - log where we found it
            marker_row = financial_start_row - 1
            marker_text = str(df.iloc[marker_row, 0]) if pd.notna(df.iloc[marker_row, 0]) else "N/A"
            print(f"  Found financial section at row {marker_row}: '{marker_text[:50]}'")
        
        # Parse financial rows
        for row_idx in range(financial_start_row, min(financial_start_row + 30, len(df))):
            if pd.notna(df.iloc[row_idx, 0]):
                row_label = str(df.iloc[row_idx, 0]).strip()
                
                # Clean the label (remove leading symbols)
                clean_label = row_label.lstrip('- +=').strip()
                
                # Check if this is a financial category we want to capture
                if clean_label in financial_categories:
                    # Extract values for each day/shift
                    for col in range(3, min(17, len(df.columns))):
                        if col in column_mapping and isinstance(column_mapping[col], dict):
                            value = self._clean_currency(df.iloc[row_idx, col])
                            if value is not None:  # Include zero values for financial data
                                financial_data.append({
                                    'date': column_mapping[col]['date'],
                                    'shift': column_mapping[col]['daypart'],
                                    'financial_category': clean_label,
                                    'amount': value
                                })
        
        return financial_data
    
    def _parse_inventory(self, file_path: str) -> List[Dict[str, Any]]:
        """Parse inventory COS $ and COS % from Inventory tab"""
        try:
            df = pd.read_excel(file_path, sheet_name='Inventory', header=None)
        except Exception as e:
            print(f"{Colors.YELLOW}Warning: Could not read Inventory tab: {e}{Colors.RESET}")
            return []
        
        inventory_data = []
        current_category = None
        
        # Scan through rows looking for category names and COS values
        for row_idx in range(len(df)):
            if pd.notna(df.iloc[row_idx, 0]):
                cell_value = str(df.iloc[row_idx, 0]).strip()
                
                # Check if this is a category header (e.g., "Bread 4311", "Food 4312", "Produce 4315")
                # Must have a space followed by exactly 4 digits at the end
                if re.search(r'\s\d{4}$', cell_value):
                    current_category = cell_value
                    continue
            
            # Look for rows with "Beginning Inventory" in column 0
            # Then search for "COS $'s" and "COS %" headers across all columns
            col_0_value = str(df.iloc[row_idx, 0]).strip() if pd.notna(df.iloc[row_idx, 0]) else ""
            
            if 'Beginning Inventory' in col_0_value:
                cos_dollars = None
                cos_percent = None
                cos_dollars_col = None
                cos_percent_col = None
                
                # Search across all columns for COS headers
                for col_idx in range(len(df.columns)):
                    if pd.notna(df.iloc[row_idx, col_idx]):
                        cell_str = str(df.iloc[row_idx, col_idx]).strip()
                        
                        # Find column with "COS $" or "COS $'s" header
                        if 'COS $' in cell_str or "COS $'s" in cell_str:
                            cos_dollars_col = col_idx + 1  # Value is in next column
                        
                        # Find column with "COS %" header
                        elif 'COS %' in cell_str:
                            cos_percent_col = col_idx + 1  # Value is in next column
                
                # Get COS $ value if we found the column
                if cos_dollars_col is not None and cos_dollars_col < len(df.columns):
                    if pd.notna(df.iloc[row_idx, cos_dollars_col]):
                        try:
                            cos_dollars = float(df.iloc[row_idx, cos_dollars_col])
                        except (ValueError, TypeError):
                            pass
                
                # Get COS % value if we found the column
                if cos_percent_col is not None and cos_percent_col < len(df.columns):
                    if pd.notna(df.iloc[row_idx, cos_percent_col]):
                        try:
                            cos_value = df.iloc[row_idx, cos_percent_col]
                            
                            # Handle both decimal (0.033) and percentage (3.3) formats
                            if isinstance(cos_value, (int, float)):
                                cos_pct = float(cos_value)
                                # If value is between 0 and 1, assume it's decimal format
                                if 0 <= cos_pct <= 1:
                                    cos_pct = cos_pct * 100
                                
                                # Validate percentage range (0-100%)
                                if 0 <= cos_pct <= 100:
                                    cos_percent = round(cos_pct, 2)
                        except (ValueError, TypeError):
                            pass
                
                # Add record if we have valid data and a category
                if current_category and (cos_dollars is not None or cos_percent is not None):
                    record = {'category': current_category}
                    if cos_dollars is not None:
                        record['cos_dollars'] = round(cos_dollars, 2)
                    if cos_percent is not None:
                        record['cos_percent'] = cos_percent
                    inventory_data.append(record)
        
        return inventory_data
    
    def _parse_inventory_unit_costs(self, file_path: str) -> List[Dict[str, Any]]:
        """Parse unit costs for tracked inventory items from Inventory tab.
        
        Extracts Unit Cost (column 10) for specific high-value items:
        - Meats: Capocollo, Salami, Ham, Turkey, Beef, Bacon
        - Dairy: Provolone Cheese
        - Condiments: Mayonnaise
        - Produce: Yellow Onions, Lettuce, Tomatoes, Onions, Celery, Cucumbers
        """
        try:
            df = pd.read_excel(file_path, sheet_name='Inventory', header=None)
        except Exception as e:
            print(f"{Colors.YELLOW}Warning: Could not read Inventory tab for unit costs: {e}{Colors.RESET}")
            return []
        
        # Items to track - use lowercase for case-insensitive matching
        # Map from search key -> canonical name for the database
        TRACKED_ITEMS = {
            'capocollo': 'Capocollo',
            'salami': 'Salami',
            'ham': 'Ham',
            'turkey': 'Turkey',
            'beef': 'Beef',
            'bacon': 'Bacon',
            'provolone cheese': 'Provolone Cheese',
            'mayonnaise': 'Mayonnaise',
            'yellow onions': 'Yellow Onions',
            'lettuce': 'Lettuce',
            'tomatoes': 'Tomatoes',
            'onions': 'Onions',
            'celery': 'Celery',
            'cucumbers': 'Cucumbers',
        }
        
        unit_cost_data = []
        current_category = None
        
        for row_idx in range(len(df)):
            cell_0 = df.iloc[row_idx, 0] if pd.notna(df.iloc[row_idx, 0]) else None
            if cell_0 is None:
                continue
            
            cell_value = str(cell_0).strip()
            
            # Track current category (e.g., "Food 4312", "Produce 4315")
            if re.search(r'\s\d{4}$', cell_value):
                current_category = cell_value
                continue
            
            # Skip header/summary rows
            if 'Beginning Inventory' in cell_value or cell_value in ('Item', ''):
                continue
            
            # Check if this item name matches any tracked item
            item_lower = cell_value.lower().strip()
            
            matched_name = None
            for search_key, canonical_name in TRACKED_ITEMS.items():
                # Exact match on the lowercase item name (the item name from the
                # spreadsheet may have trailing spaces or extra text, so we check
                # if it starts with the search key)
                if item_lower == search_key or item_lower.startswith(search_key):
                    # Special case: "Onions" should not match "Yellow Onions"
                    # and vice versa. "Onions" is an exact match only.
                    if search_key == 'onions' and item_lower.startswith('yellow'):
                        continue
                    matched_name = canonical_name
                    break
            
            if matched_name is None:
                continue
            
            # Extract Unit Cost from column 10
            unit_cost = None
            if len(df.columns) > 10 and pd.notna(df.iloc[row_idx, 10]):
                try:
                    unit_cost = float(df.iloc[row_idx, 10])
                except (ValueError, TypeError):
                    pass
            
            # Extract Units of Measure from column 1
            unit_of_measure = None
            if len(df.columns) > 1 and pd.notna(df.iloc[row_idx, 1]):
                unit_of_measure = str(df.iloc[row_idx, 1]).strip()
            
            if unit_cost is not None:
                record = {
                    'item_name': matched_name,
                    'category': current_category,
                    'unit_cost': round(unit_cost, 4),
                    'unit_of_measure': unit_of_measure,
                }
                unit_cost_data.append(record)
        
        if unit_cost_data:
            print(f"{Colors.GREEN}  ✓ Parsed {len(unit_cost_data)} inventory unit costs{Colors.RESET}")
            for item in unit_cost_data:
                print(f"    {item['item_name']}: ${item['unit_cost']:.4f}/{item['unit_of_measure']}")
        
        return unit_cost_data
    
    def _parse_labor(self, file_path: str) -> List[Dict[str, Any]]:
        """Parse labor data from Labor Managers, Labor In Shop, Driver, and DMR tabs"""
        labor_data = []
        
        # Parse each labor tab
        labor_data.extend(self._parse_labor_tab(file_path, 'Labor Managers', 'Manager'))
        labor_data.extend(self._parse_labor_tab(file_path, 'Labor In Shop', 'InShop'))
        labor_data.extend(self._parse_driver_labor(file_path))
        
        return labor_data
    
    def _parse_labor_tab(self, file_path: str, sheet_name: str, labor_type: str) -> List[Dict[str, Any]]:
        """Parse Manager or In Shop labor tabs"""
        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
        except:
            return []
        
        labor_records = []
        
        # Find date row
        date_row = None
        for row_idx in range(min(10, len(df))):
            row_vals = df.iloc[row_idx].tolist()
            date_count = sum(1 for v in row_vals if pd.notna(v) and self._is_date(v))
            if date_count >= 5:
                date_row = row_idx
                break
        
        if date_row is None:
            return []
        
        # The daypart row is immediately after the date row (typically row 8)
        daypart_row = date_row + 1
        
        # Find TOTAL row (sum of all employees)
        total_row = None
        for row_idx in range(date_row + 2, min(len(df), date_row + 20)):
            if pd.notna(df.iloc[row_idx, 0]) and 'TOTAL' in str(df.iloc[row_idx, 0]).upper():
                total_row = row_idx
                break
        
        if total_row is None:
            return []
        
        # Build column mapping: ONLY parse columns where daypart = "TOTAL"
        dates = df.iloc[date_row].tolist()
        dayparts = df.iloc[daypart_row].tolist()
        
        for col_idx in range(len(dates)):
            # Check if this is a TOTAL column
            if col_idx < len(dayparts) and dayparts[col_idx] == 'TOTAL':
                # Get the date
                if pd.notna(dates[col_idx]) and self._is_date(dates[col_idx]):
                    date_str = pd.to_datetime(dates[col_idx]).strftime('%Y-%m-%d')
                    
                    # Determine shift by looking at previous column's daypart
                    if col_idx > 0 and col_idx - 1 < len(dayparts):
                        prev_daypart = dayparts[col_idx - 1]
                        if prev_daypart == 'AM':
                            shift = 'AM'
                        elif prev_daypart == 'PM':
                            shift = 'PM'
                        else:
                            continue
                    else:
                        continue
                    
                    # Get the labor dollar value from TOTAL row
                    value = self._clean_currency(df.iloc[total_row, col_idx])
                    
                    if value is not None:  # Include zeros
                        labor_records.append({
                            'date': date_str,
                            'shift': shift,
                            'labor_type': labor_type,
                            'labor_dollars': value,
                            'straight_pay': None,
                            'ot_pay': None,
                            'penalty_pay': None,
                            'dmr_expense': None,
                            'total_miles': None
                        })
        
        return labor_records
    
    def _parse_driver_labor(self, file_path: str) -> List[Dict[str, Any]]:
        """Parse Driver tab for labor and expenses"""
        try:
            df = pd.read_excel(file_path, sheet_name='Driver', header=None)
        except:
            return []
        
        labor_records = []
        
        # Find date row
        date_row = None
        for row_idx in range(min(10, len(df))):
            row_vals = df.iloc[row_idx].tolist()
            date_count = sum(1 for v in row_vals if pd.notna(v) and self._is_date(v))
            if date_count >= 5:
                date_row = row_idx
                break
        
        if date_row is None:
            return []
        
        # The daypart row is immediately after the date row
        daypart_row = date_row + 1
        
        # Find rows for labor breakdown
        total_row = None
        straight_pay_row = None
        ot_pay_row = None
        penalty_pay_row = None
        
        for row_idx in range(date_row + 2, len(df)):
            if pd.notna(df.iloc[row_idx, 0]):
                cell_val = str(df.iloc[row_idx, 0]).strip()
                if 'TOTAL' in cell_val.upper() and total_row is None:
                    total_row = row_idx
                elif 'Straight Pay' in cell_val:
                    straight_pay_row = row_idx
                elif 'OT Pay' in cell_val:
                    ot_pay_row = row_idx
                elif 'Penalty Pay' in cell_val:
                    penalty_pay_row = row_idx
        
        if total_row is None:
            return []
        
        # Build column mapping: ONLY parse columns where daypart = "TOTAL"
        dates = df.iloc[date_row].tolist()
        dayparts = df.iloc[daypart_row].tolist()
        
        for col_idx in range(len(dates)):
            # Check if this is a TOTAL column
            if col_idx < len(dayparts) and dayparts[col_idx] == 'TOTAL':
                # Get the date
                if pd.notna(dates[col_idx]) and self._is_date(dates[col_idx]):
                    date_str = pd.to_datetime(dates[col_idx]).strftime('%Y-%m-%d')
                    
                    # Determine shift by looking at previous column's daypart
                    if col_idx > 0 and col_idx - 1 < len(dayparts):
                        prev_daypart = dayparts[col_idx - 1]
                        if prev_daypart == 'AM':
                            shift = 'AM'
                        elif prev_daypart == 'PM':
                            shift = 'PM'
                        else:
                            continue
                    else:
                        continue
                    
                    # Get labor values
                    total = self._clean_currency(df.iloc[total_row, col_idx])
                    straight = self._clean_currency(df.iloc[straight_pay_row, col_idx]) if straight_pay_row else None
                    ot = self._clean_currency(df.iloc[ot_pay_row, col_idx]) if ot_pay_row else None
                    penalty = self._clean_currency(df.iloc[penalty_pay_row, col_idx]) if penalty_pay_row else None
                    
                    if total is not None:  # Include zeros
                        labor_records.append({
                            'date': date_str,
                            'shift': shift,
                            'labor_type': 'Driver',
                            'labor_dollars': total,
                            'straight_pay': straight,
                            'ot_pay': ot,
                            'penalty_pay': penalty,
                            'dmr_expense': None,
                            'total_miles': None
                        })
        
        # Try to get DMR expenses from DMR tab
        try:
            dmr_df = pd.read_excel(file_path, sheet_name='DMR', header=None)
            
            # Find date row in DMR tab
            dmr_date_row = None
            for row_idx in range(min(10, len(dmr_df))):
                row_vals = dmr_df.iloc[row_idx].tolist()
                date_count = sum(1 for v in row_vals if pd.notna(v) and self._is_date(v))
                if date_count >= 5:
                    dmr_date_row = row_idx
                    break
            
            if dmr_date_row is not None:
                dmr_daypart_row = dmr_date_row + 1
                dmr_dates = dmr_df.iloc[dmr_date_row].tolist()
                dmr_dayparts = dmr_df.iloc[dmr_daypart_row].tolist()
                
                # Find DMR TOTAL row
                for row_idx in range(dmr_date_row + 2, len(dmr_df)):
                    if pd.notna(dmr_df.iloc[row_idx, 0]) and 'TOTAL' in str(dmr_df.iloc[row_idx, 0]).upper():
                        # Parse DMR expenses by date/shift
                        for col_idx in range(len(dmr_dates)):
                            if col_idx < len(dmr_dayparts) and dmr_dayparts[col_idx] == 'TOTAL':
                                if pd.notna(dmr_dates[col_idx]) and self._is_date(dmr_dates[col_idx]):
                                    date_str = pd.to_datetime(dmr_dates[col_idx]).strftime('%Y-%m-%d')
                                    
                                    # Determine shift
                                    if col_idx > 0 and col_idx - 1 < len(dmr_dayparts):
                                        prev_daypart = dmr_dayparts[col_idx - 1]
                                        if prev_daypart == 'AM':
                                            shift = 'AM'
                                        elif prev_daypart == 'PM':
                                            shift = 'PM'
                                        else:
                                            continue
                                    else:
                                        continue
                                    
                                    dmr_value = self._clean_currency(dmr_df.iloc[row_idx, col_idx])
                                    
                                    # Find matching driver record and add DMR expense
                                    for record in labor_records:
                                        if (record['labor_type'] == 'Driver' and 
                                            record['date'] == date_str and 
                                            record['shift'] == shift):
                                            record['dmr_expense'] = dmr_value
                                            break
                        break
        except:
            pass
        
        return labor_records
    
    def _parse_dmr(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Parse DMR data with dynamic shift detection
        
        Handles variable number of shifts per week (typically up to 14: 7 days × 2 shifts).
        The parser dynamically detects all shift blocks throughout the sheet by scanning vertically
        for "Shift N" markers, where odd shift numbers = AM and even shift numbers = PM.
        
        Note: DMR sheet may only show shifts where deliveries occurred, so the number
        of shift sections can vary from 0 to 14 depending on business activity.
        """
        try:
            df = pd.read_excel(file_path, sheet_name='DMR', header=None)
        except:
            return []
        
        dmr_data = []
        
        # Relative row offsets within each shift block (from the "Shift N" header row)
        metric_row_offsets = {
            4: 'Total Sales',
            5: 'Net Sales',
            6: 'Total Miles',
            7: 'DMR',
            8: '# of Checks',
            9: 'Amount of Checks',
            10: 'Cash Due',
            11: 'Tips',
            12: 'Taxable Income'
        }
        
        # First, find ALL shift blocks by scanning column 0 for "Shift N" markers
        shift_blocks = []  # List of row indices where shift blocks start
        
        for row_idx in range(len(df)):
            cell_value = str(df.iloc[row_idx, 0]).strip() if pd.notna(df.iloc[row_idx, 0]) else ''
            # Check for "Shift N" pattern
            if 'Shift' in cell_value or 'shift' in cell_value.lower():
                match = re.search(r'(\d+)', cell_value)
                if match:
                    shift_blocks.append(row_idx)
        
        # Process each shift block found
        for block_start_row in shift_blocks:
            # Within this block, scan horizontally for shift sections
            shift_row = df.iloc[block_start_row]
            shift_sections = []  # Shift sections within this block
            
            i = 0
            while i < len(shift_row):
                cell_value = str(shift_row.iloc[i]).strip() if pd.notna(shift_row.iloc[i]) else ''
                
                # Check for "Shift N" pattern
                if 'Shift' in cell_value or 'shift' in cell_value.lower():
                    match = re.search(r'(\d+)', cell_value)
                    if match:
                        shift_num = int(match.group(1))
                        shift_label = 'AM' if shift_num % 2 == 1 else 'PM'
                        
                        # Find the next section start (or end of row)
                        next_section_col = len(shift_row)
                        for j in range(i + 1, len(shift_row)):
                            next_val = str(shift_row.iloc[j]).strip() if pd.notna(shift_row.iloc[j]) else ''
                            if 'Shift' in next_val or 'shift' in next_val.lower():
                                next_section_col = j
                                break
                        
                        shift_sections.append({
                            'start_col': i,
                            'end_col': next_section_col,
                            'shift': shift_label,
                            'shift_num': shift_num
                        })
                        
                        i = next_section_col
                    else:
                        i += 1
                else:
                    i += 1
            
            # Process each shift section within this block
            for section in shift_sections:
                start_col = section['start_col']
                end_col = section['end_col']
                shift = section['shift']
                shift_num = section['shift_num']
                
                # Driver names are at offset +3 from block start
                driver_row_idx = block_start_row + 3
                
                # Process columns in this shift section (skip label columns)
                for col_idx in range(start_col + 1, end_col):
                    driver_name = str(df.iloc[driver_row_idx, col_idx]).strip() if pd.notna(df.iloc[driver_row_idx, col_idx]) else ''
                    
                    # Skip if no driver, label column, or totals column
                    if not driver_name or driver_name.upper() in ['TOTALS', 'TOTAL', 'DRIVERS NAME', 'NAN', '']:
                        continue
                    
                    # Extract metrics for this driver using relative offsets
                    for row_offset, metric_name in metric_row_offsets.items():
                        metric_row_idx = block_start_row + row_offset
                        if metric_row_idx < len(df):
                            value = self._clean_currency(df.iloc[metric_row_idx, col_idx])
                            if value is not None:
                                dmr_data.append({
                                    'shift': shift,
                                    'shift_num': shift_num,
                                    'driver_name': driver_name,
                                    'metric': metric_name,
                                    'amount': value
                                })
        
        return dmr_data
    
    def _is_date(self, value) -> bool:
        """Check if value is a date"""
        if pd.isna(value):
            return False
        try:
            pd.to_datetime(value)
            return True
        except:
            return False
    
    def _clean_currency(self, value) -> Optional[float]:
        """Clean currency values"""
        if pd.isna(value):
            return None
        str_val = str(value).strip().replace('$', '').replace(',', '').strip()
        try:
            return float(str_val)
        except:
            return None
    
    def _clean_value(self, value):
        """Clean value for JSON serialization"""
        if value is None or pd.isna(value):
            return None
        
        if hasattr(value, 'item'):
            value = value.item()
        
        if isinstance(value, (np.integer, np.int64, np.int32)):
            return int(value)
        if isinstance(value, (np.floating, np.float64, np.float32)):
            if np.isnan(value) or np.isinf(value):
                return None
            return round(float(value), 2)
        if isinstance(value, (int, float)):
            if isinstance(value, float) and (np.isnan(value) or np.isinf(value)):
                return None
            if isinstance(value, float):
                return round(value, 2)
            return value
        if isinstance(value, str):
            return str(value).strip()
        
        return str(value)
    
    def _validate_record(self, record: Dict, required_fields: List[str]) -> bool:
        """Validate that required fields are present and non-null"""
        for field in required_fields:
            if field not in record or record[field] is None:
                return False
        return True
    
    # ========================================================================
    # AGGREGATION FUNCTIONS (EXISTING)
    # ========================================================================
    
    def _is_complete_week(self, sales_data: List[Dict]) -> bool:
        """
        Check if the week has actually ended (not just if there are 7 dates in the file).
        Macromatix pre-generates rows for all 7 days, so we need to verify:
        1. There are 7 unique dates
        2. Dates span Wed-Tue (6 days)
        3. Today's date is PAST the week ending date
        
        Only upload inventory/COS when week is complete.
        """
        if not sales_data:
            return False
        
        unique_dates = set(record['date'] for record in sales_data if 'date' in record)
        
        # Should have exactly 7 unique dates
        if len(unique_dates) != 7:
            return False
        
        # Verify dates are consecutive and span a full week
        try:
            sorted_dates = sorted([pd.to_datetime(d) for d in unique_dates])
            first_date = sorted_dates[0]
            last_date = sorted_dates[-1]
            
            # Should span 6 days (Wed to Tue)
            day_diff = (last_date - first_date).days
            if day_diff != 6:
                return False
            
            # First day should be Wednesday (weekday = 2)
            if first_date.weekday() != 2:
                return False
            
            # CRITICAL: Week is only complete if today is PAST the week ending date
            # This prevents marking incomplete weeks as complete when Macromatix
            # pre-generates rows with zeros for future days
            today = pd.Timestamp.now().normalize()
            if today <= last_date:
                return False
            
            return True
        except:
            return False
    
    def _aggregate_sales_to_daily(self, sales_data: List[Dict]) -> Dict[str, Dict]:
        """
        Aggregate AM/PM sales data into daily totals for wsr_sales_daily table
        
        Returns: Dict[date, aggregated_sales_dict]
        """
        daily_sales = defaultdict(lambda: {
            # InShop revenue
            'in_sub': 0.0, 'in_club': 0.0, 'in_pop': 0.0, 'in_side': 0.0, 'in_combos': 0.0,
            # Delivery revenue
            'del_sub': 0.0, 'del_club': 0.0, 'del_pop': 0.0, 'del_side': 0.0, 'del_combos': 0.0,
            # Other revenue
            'day_old_bread': 0.0, 'fresh_bread': 0.0, 'cookie': 0.0,
            'box_lunch': 0.0, 'platters': 0.0, 'delivery_fee': 0.0,
            'other_revenue': 0.0, 'modifiers': 0.0,
            # Calculations
            'total_of_above': 0.0, 'over_rings': 0.0, 'adjusted_sales': 0.0,
            # Deductions
            'net_employee_freebies': 0.0, 'net_manager_freebies': 0.0,
            'sampling': 0.0, 'waste': 0.0, 'other_promo': 0.0, 'loyalty_coupon': 0.0,
            'total_deductions': 0.0,
            # Final
            'royalty_sales': 0.0,
            # 3PF
            'doordash': 0.0, 'grubhub': 0.0, 'ubereats': 0.0,
            # Metrics
            'num_of_sales': 0, 'num_of_checks': 0, 'total_online_orders': 0, 'total_quantity': 0
        })
        
        # Category mapping to column names
        # NEW menu categories are mapped to OLD column equivalents for backward compatibility
        category_to_column = {
            # InShop products - OLD menu
            'IN-Sub': 'in_sub',
            'IN-Club': 'in_club',
            'IN-Pop': 'in_pop',
            'IN-Side': 'in_side',
            'IN-Sides': 'in_side',  # Plural variant
            'IN-Combos / Kids 1/2 off': 'in_combos',
            'IN-Combos': 'in_combos',  # Short variant
            # InShop products - NEW menu → mapped to OLD columns
            'IN-Originals': 'in_sub',      # Originals → Sub
            'IN-Favorites': 'in_club',     # Favorites → Club
            'IN-Beverage': 'in_pop',       # Beverage → Pop
            
            # Delivery products - OLD menu
            'DEL-Sub': 'del_sub',
            'DEL-Club': 'del_club',
            'DEL-Pop': 'del_pop',
            'DEL-Side': 'del_side',
            'DEL-Sides': 'del_side',  # Plural variant
            'DEL-Combos / Kids 1/2 off': 'del_combos',
            'DEL-Combos': 'del_combos',  # Short variant
            # Delivery products - NEW menu → mapped to OLD columns
            'DEL-Originals': 'del_sub',    # Originals → Sub
            'DEL-Favorites': 'del_club',   # Favorites → Club
            'DEL-Beverage': 'del_pop',     # Beverage → Pop
            
            # Other revenue
            'Day Old Bread': 'day_old_bread',
            'Fresh Bread': 'fresh_bread',
            'Cookie': 'cookie',
            'Desserts': 'cookie',          # Desserts → Cookie
            'Box Lunch': 'box_lunch',
            'Platters / Mini Jimmys': 'platters',
            'Catering': 'platters',        # Catering → Platters
            'Delivery Fee': 'delivery_fee',
            'Other': 'other_revenue',
            'Modifiers': 'modifiers',
            # Calculations
            'Total of Above': 'total_of_above',
            'OVER-RINGS': 'over_rings',
            'Adjusted Sales': 'adjusted_sales',
            # Deductions
            'Net Employee Freebies': 'net_employee_freebies',
            'Net Manager Freebies': 'net_manager_freebies',
            'Sampling': 'sampling',
            'Waste': 'waste',
            'Other Promo': 'other_promo',
            'Loyalty / Coupon': 'loyalty_coupon',
            # 3rd Party
            'DoorDash': 'doordash',
            'GrubHub': 'grubhub',
            'UberEats': 'ubereats',
            # Metrics
            '# Of Sales': 'num_of_sales',
            '# Of Checks': 'num_of_checks',
            'Total Online Orders': 'total_online_orders'
        }
        
        # Aggregate sales data
        for record in sales_data:
            date = record['date']
            category = record['category']
            amount = record['sales_amount']
            quantity = record.get('quantity', 0)
            
            # Map category to column
            column = category_to_column.get(category)
            
            if column:
                if column in ['num_of_sales', 'num_of_checks', 'total_online_orders']:
                    # Integer metrics
                    daily_sales[date][column] += int(amount)
                else:
                    # Dollar amounts
                    daily_sales[date][column] += amount
            
            # Track total quantity
            if quantity:
                daily_sales[date]['total_quantity'] += quantity
        
        # Calculate derived fields for each day
        for date, data in daily_sales.items():
            # Calculate total_of_above if not explicitly set
            if data['total_of_above'] == 0:
                data['total_of_above'] = (
                    data['in_sub'] + data['in_club'] + data['in_pop'] + data['in_side'] + data['in_combos'] +
                    data['del_sub'] + data['del_club'] + data['del_pop'] + data['del_side'] + data['del_combos'] +
                    data['day_old_bread'] + data['fresh_bread'] + data['cookie'] +
                    data['box_lunch'] + data['platters'] + data['delivery_fee'] +
                    data['other_revenue'] + data['modifiers']
                )
            
            # Calculate adjusted_sales if not explicitly set
            if data['adjusted_sales'] == 0:
                data['adjusted_sales'] = data['total_of_above'] + data['over_rings']
            
            # Calculate total_deductions
            data['total_deductions'] = (
                data['net_employee_freebies'] + data['net_manager_freebies'] +
                data['sampling'] + data['waste'] + data['other_promo'] + data['loyalty_coupon']
            )
            
            # Calculate royalty_sales
            data['royalty_sales'] = data['adjusted_sales'] - data['total_deductions']
        
        return dict(daily_sales)
    
    def _aggregate_daily_to_weekly(self, daily_sales: Dict[str, Dict]) -> Dict:
        """Aggregate daily sales data into weekly totals"""
        weekly = {
            'inshop': 0.0,
            'delivery': 0.0,
            'catering': 0.0,
            'desserts': 0.0,
            'other_revenue': 0.0,
            'total_of_above': 0.0,
            'over_rings': 0.0,
            'adjusted_sales': 0.0,
            'net_employee_freebies': 0.0,
            'net_manager_freebies': 0.0,
            'sampling': 0.0,
            'waste': 0.0,
            'other_promo': 0.0,
            'loyalty_coupon': 0.0,
            'total_deductions': 0.0,
            'royalty_sales': 0.0,
            'total_quantity': 0,
            'avg_daily_royalty_sales': 0.0
        }
        
        for date, data in daily_sales.items():
            for key in weekly.keys():
                if key != 'avg_daily_royalty_sales':
                    weekly[key] += data.get(key, 0)
        
        # Calculate average
        num_days = len(daily_sales)
        if num_days > 0:
            weekly['avg_daily_royalty_sales'] = weekly['royalty_sales'] / num_days
        
        return weekly
    
    
    
    def _parse_labor_cost_summary(self, file_path: str, audit_mode: bool = True) -> List[Dict[str, Any]]:
        """
        Parse labor cost summary data from Sales Summary tab
        
        Dynamically searches for "Labor Cost Summary" anywhere on the sheet
        and parses data from the correct columns regardless of position.
        
        Args:
            file_path: Path to Excel file
            audit_mode: If True, shows detailed parsing audit trail
        
        Returns:
            List of labor cost summary records with categories and amounts
        """
        labor_cost_data = []
        
        try:
            # Read Sales Summary tab
            df = pd.read_excel(file_path, sheet_name='Sales Summary', header=None)
            
            if audit_mode:
                print(f"\n{Colors.CYAN}{'='*70}")
                print(f"LABOR COST SUMMARY PARSING AUDIT")
                print(f"{'='*70}{Colors.RESET}")
                print(f"File: {os.path.basename(file_path)}")
                print(f"Sheet Dimensions: {len(df)} rows x {len(df.columns)} columns\n")
            
            # Search ENTIRE sheet for "Labor Cost Summary" header
            header_row = None
            header_col = None
            
            for row_idx in range(len(df)):
                for col_idx in range(len(df.columns)):
                    if pd.notna(df.iloc[row_idx, col_idx]):
                        cell_value = str(df.iloc[row_idx, col_idx]).strip()
                        
                        if 'labor' in cell_value.lower() and ('cost' in cell_value.lower() or 'summary' in cell_value.lower()):
                            header_row = row_idx
                            header_col = col_idx
                            print(f"  {Colors.GREEN}✓ Found 'Labor Cost Summary' at row {row_idx}, column {col_idx}{Colors.RESET}")
                            print(f"    Header text: '{cell_value}'")
                            break
                
                if header_row is not None:
                    break
            
            if header_row is None:
                print(f"  {Colors.YELLOW}⚠ Labor Cost Summary section not found in Sales Summary tab{Colors.RESET}")
                return labor_cost_data
            
            # Labor Cost Summary shows cumulative week-to-date data
            # We'll use an "as_of" date (yesterday) to track daily snapshots
            # This allows us to see how labor costs accumulate throughout the week
            as_of_date = datetime.now().date() - timedelta(days=1)
            as_of_date_str = as_of_date.strftime('%Y-%m-%d')
            
            # Also keep week_ending for context
            week_ending = self.metadata.get('week_ending')
            if isinstance(week_ending, str):
                week_ending_date = week_ending
            else:
                week_ending_date = week_ending.strftime('%Y-%m-%d') if week_ending else None
            
            if not week_ending_date:
                print(f"  {Colors.YELLOW}⚠ Could not determine week ending date{Colors.RESET}")
                return labor_cost_data
            
            if audit_mode:
                print(f"  As of date (snapshot): {as_of_date_str}")
                print(f"  Week ending date: {week_ending_date}")
            
            # Data starts on the next row
            data_start_row = header_row + 1
            
            # Find where data ends (look for "Total Labor Cost" or blank rows)
            data_end_row = len(df)
            for row_idx in range(data_start_row, min(len(df), data_start_row + 50)):
                # Check if category column is blank
                if pd.isna(df.iloc[row_idx, header_col]):
                    data_end_row = row_idx
                    break
                
                # Check for "Total Labor Cost" which marks the end
                cell_value = str(df.iloc[row_idx, header_col]).strip() if pd.notna(df.iloc[row_idx, header_col]) else ""
                if 'total labor cost' in cell_value.lower():
                    data_end_row = row_idx + 1  # Include this row
                    break
            
            print(f"\n  {Colors.CYAN}Parsing labor cost rows {data_start_row} to {data_end_row}{Colors.RESET}")
            
            if audit_mode:
                print(f"\n{Colors.CYAN}{'─'*70}")
                print(f"ROW-BY-ROW AUDIT")
                print(f"{'─'*70}{Colors.RESET}\n")
            
            # Parse each row of labor cost data
            for row_idx in range(data_start_row, data_end_row):
                # Get category name from the column where header was found
                category_name = str(df.iloc[row_idx, header_col]).strip() if pd.notna(df.iloc[row_idx, header_col]) else None
                
                if audit_mode:
                    print(f"{Colors.BLUE}Row {row_idx}:{Colors.RESET}")
                    print(f"  Category (col {header_col}): '{category_name}'")
                
                # Skip invalid rows
                if not category_name or category_name.startswith(('=', '+', '-')):
                    if audit_mode:
                        print(f"  {Colors.YELLOW}⊗ SKIPPED - Invalid category name{Colors.RESET}\n")
                    continue
                
                # Skip summary/total rows to avoid double-counting
                if 'total' in category_name.lower():
                    if audit_mode:
                        print(f"  {Colors.YELLOW}⊗ SKIPPED - Summary row (contains 'Total'){Colors.RESET}\n")
                    continue
                
                # Look for percentage and dollar amount in adjacent columns
                # Typically: Category in col N, Percentage in col N+1, Dollar in col N+2
                amount = None
                percent = None
                found_amount_at_col = None
                found_percent_at_col = None
                
                if audit_mode:
                    print(f"  Checking adjacent columns for percentage and dollar amount:")
                
                # First pass: Look for percentage (should be first column after category)
                for offset in range(1, 4):  # Check next 3 columns
                    col_idx = header_col + offset
                    if col_idx < len(df.columns):
                        val = df.iloc[row_idx, col_idx]
                        
                        if audit_mode:
                            val_display = f"'{val}'" if pd.notna(val) else "BLANK"
                            val_type = type(val).__name__ if pd.notna(val) else "NaN"
                            print(f"    Col {col_idx} (offset +{offset}): {val_display} [type: {val_type}]")
                        
                        if pd.notna(val):
                            try:
                                # Try to extract percentage value
                                if isinstance(val, (int, float)):
                                    # Percentages are typically < 1.0 (e.g., 0.2061 = 20.61%)
                                    if 0 <= val <= 1.0:
                                        percent = round(float(val) * 100, 2)  # Convert to percentage
                                        found_percent_at_col = col_idx
                                        if audit_mode:
                                            print(f"      {Colors.GREEN}→ ACCEPTED as {percent}% (percentage){Colors.RESET}")
                                        break
                                elif isinstance(val, str):
                                    # Check if it's a percentage string like "20.61%"
                                    if '%' in val:
                                        cleaned = val.replace('%', '').strip()
                                        if cleaned and cleaned not in ['-', '']:
                                            percent = round(float(cleaned), 2)
                                            found_percent_at_col = col_idx
                                            if audit_mode:
                                                print(f"      {Colors.GREEN}→ ACCEPTED as {percent}% (parsed from '{val}'){Colors.RESET}")
                                            break
                            except (ValueError, AttributeError) as e:
                                if audit_mode:
                                    print(f"      {Colors.YELLOW}→ Rejected: Parse error - {str(e)}{Colors.RESET}")
                                continue
                
                # Second pass: Look for dollar amount (should be after percentage)
                for offset in range(1, 4):  # Check next 3 columns
                    col_idx = header_col + offset
                    if col_idx < len(df.columns):
                        val = df.iloc[row_idx, col_idx]
                        
                        if pd.notna(val):
                            try:
                                # Try to extract numeric value (dollar amount)
                                if isinstance(val, (int, float)):
                                    # Skip if it looks like a percentage (< 1.0)
                                    if val < 1.0:
                                        continue
                                    amount = float(val)
                                    found_amount_at_col = col_idx
                                    if audit_mode:
                                        print(f"      {Colors.GREEN}→ ACCEPTED as ${amount:,.2f} (dollar amount){Colors.RESET}")
                                    break
                                elif isinstance(val, str):
                                    # Skip percentage columns
                                    if '%' in val:
                                        continue
                                    # Clean and parse dollar amounts
                                    cleaned = val.replace('$', '').replace(',', '').strip()
                                    if cleaned and cleaned not in ['-', '']:
                                        amount = float(cleaned)
                                        found_amount_at_col = col_idx
                                        if audit_mode:
                                            print(f"      {Colors.GREEN}→ ACCEPTED as ${amount:,.2f} (parsed from '{val}'){Colors.RESET}")
                                        break
                            except (ValueError, AttributeError) as e:
                                if audit_mode:
                                    print(f"      {Colors.YELLOW}→ Rejected: Parse error - {str(e)}{Colors.RESET}")
                                continue
                
                # Only add if we found an amount (percentage is optional)
                if amount is not None and amount > 0:
                    record = {
                        'store_number': self.metadata.get('store_number'),
                        'as_of': as_of_date_str,  # Yesterday's date (when snapshot was taken)
                        'date': week_ending_date,  # Week ending date for reference
                        'week_ending': self.metadata.get('week_ending'),
                        'week_number': self.metadata.get('week_number'),
                        'year': self.metadata.get('year'),
                        'category': category_name,
                        'amount': amount,
                        'uploaded_at': datetime.now().isoformat()
                    }
                    
                    # Add percentage if found
                    if percent is not None:
                        record['labor_percent'] = percent
                    
                    labor_cost_data.append(record)
                    if audit_mode:
                        print(f"  {Colors.GREEN}✓ PARSED SUCCESSFULLY{Colors.RESET}")
                        print(f"    Category: {category_name}")
                        print(f"    Amount: ${amount:,.2f}")
                        if percent is not None:
                            print(f"    Percent: {percent}%")
                        print(f"    As of: {as_of_date_str}")
                        print(f"    Found at: Amount col {found_amount_at_col}, Percent col {found_percent_at_col}\n")
                else:
                    if audit_mode:
                        print(f"  {Colors.RED}✗ NO VALID AMOUNT FOUND{Colors.RESET}\n")
            
            if audit_mode:
                print(f"{Colors.CYAN}{'─'*70}")
                print(f"PARSING SUMMARY")
                print(f"{'─'*70}{Colors.RESET}")
            
            if labor_cost_data:
                total = sum(r['amount'] for r in labor_cost_data)
                print(f"  {Colors.GREEN}✓ Successfully parsed {len(labor_cost_data)} labor cost categories{Colors.RESET}")
                print(f"    As of: {as_of_date_str}")
                print(f"    Week ending: {week_ending_date}")
                print(f"    Total Amount: ${total:,.2f}")
                
                if audit_mode:
                    print(f"\n  Categories parsed:")
                    for record in labor_cost_data:
                        percent_str = f" ({record.get('labor_percent', 'N/A')}%)" if 'labor_percent' in record else ""
                        print(f"    • {record['category']}: ${record['amount']:,.2f}{percent_str}")
            else:
                print(f"  {Colors.YELLOW}⚠ No labor cost data found in section{Colors.RESET}")
                if audit_mode:
                    print(f"\n  {Colors.YELLOW}Possible reasons:")
                    print(f"    1. Dollar amounts are in columns beyond offset +3")
                    print(f"    2. Values are formatted in an unexpected way")
                    print(f"    3. All values were rejected (percentages, blanks, etc.)")
                    print(f"    4. Category names contain special characters (=, +, -){Colors.RESET}")
            
            if audit_mode:
                print(f"{Colors.CYAN}{'='*70}{Colors.RESET}\n")
        
        except ValueError as e:
            # Sheet doesn't exist
            print(f"  {Colors.YELLOW}⚠ Sales Summary tab not found - skipping labor cost summary{Colors.RESET}")
        except Exception as e:
            print(f"  {Colors.RED}✗ Error parsing labor cost summary: {e}{Colors.RESET}")
            traceback.print_exc()
        
        return labor_cost_data
    
    def _aggregate_labor_to_daily(self, labor_data: List[Dict]) -> Dict[str, Dict]:
        """
        Aggregate AM/PM shift labor into daily totals by labor type
        
        Returns: Dict[date, aggregated_labor]
        """
        daily_labor = defaultdict(lambda: {
            'manager_labor': 0.0,
            'inshop_labor': 0.0,
            'driver_labor': 0.0,
            'driver_straight_pay': 0.0,
            'driver_ot_pay': 0.0,
            'driver_penalty_pay': 0.0,
            'driver_dmr_expense': 0.0,
            'total_labor': 0.0,
            'overtime_labor': 0.0,
            'labor_percentage': 0.0
        })
        
        for record in labor_data:
            date = record['date']
            labor_type = record['labor_type']
            labor_dollars = record.get('labor_dollars', 0) or 0
            
            if labor_type == 'Manager':
                daily_labor[date]['manager_labor'] += labor_dollars
            elif labor_type == 'InShop':
                daily_labor[date]['inshop_labor'] += labor_dollars
            elif labor_type == 'Driver':
                daily_labor[date]['driver_labor'] += labor_dollars
                daily_labor[date]['driver_straight_pay'] += record.get('straight_pay', 0) or 0
                daily_labor[date]['driver_ot_pay'] += record.get('ot_pay', 0) or 0
                daily_labor[date]['driver_penalty_pay'] += record.get('penalty_pay', 0) or 0
                daily_labor[date]['driver_dmr_expense'] += record.get('dmr_expense', 0) or 0
            
            daily_labor[date]['total_labor'] += labor_dollars
        
        # Calculate overtime total
        for date, data in daily_labor.items():
            data['overtime_labor'] = data['driver_ot_pay']
        
        return dict(daily_labor)
    
    def _calculate_labor_percentages(self, daily_labor: Dict[str, Dict], daily_sales: Dict[str, Dict]):
        """
        Calculate labor percentage for each day using sales data
        Modifies daily_labor dict in place
        """
        for date, labor_data in daily_labor.items():
            if date in daily_sales:
                royalty_sales = daily_sales[date].get('royalty_sales', 0)
                if royalty_sales > 0:
                    labor_data['labor_percentage'] = round((labor_data['total_labor'] / royalty_sales) * 100, 2)
    
    def _aggregate_labor_daily_to_weekly(self, daily_labor: Dict[str, Dict]) -> Dict:
        """Aggregate daily labor data into weekly totals"""
        weekly = {
            'manager_labor': 0.0,
            'inshop_labor': 0.0,
            'driver_labor': 0.0,
            'driver_straight_pay': 0.0,
            'driver_ot_pay': 0.0,
            'driver_penalty_pay': 0.0,
            'driver_dmr_expense': 0.0,
            'total_labor': 0.0,
            'avg_daily_labor': 0.0
        }
        
        for date, data in daily_labor.items():
            for key in weekly.keys():
                if key != 'avg_daily_labor':
                    weekly[key] += data.get(key, 0)
        
        # Calculate average
        num_days = len(daily_labor)
        if num_days > 0:
            weekly['avg_daily_labor'] = weekly['total_labor'] / num_days
        
        return weekly
    
    def _aggregate_labor_metrics_to_daily(self, labor_metrics: List[Dict]) -> Dict[str, Dict]:
        """
        Aggregate AM/PM shift labor metrics from Weekly Sales tab into daily totals
        
        Args:
            labor_metrics: List of records with date, shift, metric_name, metric_value
        
        Returns: Dict[date, {labor_dollars, labor_percent, labor_overtime}]
        """
        daily_metrics = defaultdict(lambda: {
            'labor_dollars': 0.0,
            'labor_percent': 0.0,
            'labor_overtime': 0.0,
            'labor_dollars_count': 0,  # For averaging
            'labor_percent_count': 0   # For averaging
        })
        
        for record in labor_metrics:
            date = record['date']
            metric_name = record['metric_name']
            metric_value = record.get('metric_value', 0) or 0
            
            if metric_name == 'labor_dollars':
                daily_metrics[date]['labor_dollars'] += metric_value
                daily_metrics[date]['labor_dollars_count'] += 1
            elif metric_name == 'labor_percent':
                # For percentages, we need to average them (not sum)
                # Accumulate for averaging
                daily_metrics[date]['labor_percent'] += metric_value
                daily_metrics[date]['labor_percent_count'] += 1
            elif metric_name == 'labor_overtime':
                daily_metrics[date]['labor_overtime'] += metric_value
        
        # Calculate averages for percentages
        for date, metrics in daily_metrics.items():
            if metrics['labor_percent_count'] > 0:
                # Average the AM/PM percentages
                metrics['labor_percent'] = round(metrics['labor_percent'] / metrics['labor_percent_count'], 2)
            # Remove count fields
            del metrics['labor_dollars_count']
            del metrics['labor_percent_count']
        
        return dict(daily_metrics)
    
    def _aggregate_financial_to_daily(self, financial_data: List[Dict]) -> Dict[str, List[Dict]]:
        """
        Aggregate AM/PM shift financial line items into daily totals
        
        Returns: Dict[date, List[{line_item, amount}]]
        """
        daily_financial = defaultdict(lambda: defaultdict(float))
        
        for record in financial_data:
            date = record['date']
            line_item = record['financial_category']
            amount = record.get('amount', 0) or 0
            
            daily_financial[date][line_item] += amount
        
        # Convert to list format
        result = {}
        for date, items in daily_financial.items():
            result[date] = [{'line_item': item, 'amount': amt} for item, amt in items.items()]
        
        return result
    
    def _aggregate_dmr_to_daily(self, dmr_data: List[Dict], metadata: Dict) -> List[Dict]:
        """
        Aggregate shift-level DMR data to daily totals per driver
        Groups by date and driver, sums all metrics
        """
        # Group by date and driver
        daily_data = defaultdict(lambda: {
            'Total Sales': 0,
            'Net Sales': 0,
            'Total Miles': 0,
            'DMR': 0,
            '# of Checks': 0,
            'Amount of Checks': 0,
            'Cash Due': 0,
            'Tips': 0,
            'Taxable Income': 0
        })
        
        for record in dmr_data:
            # Calculate date from shift_num
            if 'shift_num' in record and metadata.get('week_ending_date'):
                shift_num = record['shift_num']
                day_offset = ((shift_num - 1) // 2)  # 0-6 for Mon-Sun
                date_offset = day_offset - 1  # Monday = -1 from Tuesday
                week_ending_date = metadata.get('week_ending_date')
                # Ensure it's a date object
                if isinstance(week_ending_date, str):
                    week_ending_date = datetime.strptime(week_ending_date, '%Y-%m-%d').date()
                record_date = week_ending_date + timedelta(days=date_offset)
                date_str = record_date.strftime('%Y-%m-%d')
            else:
                week_ending = metadata.get('week_ending_date') or metadata.get('week_ending')
                if hasattr(week_ending, 'strftime'):
                    date_str = week_ending.strftime('%Y-%m-%d')
                else:
                    date_str = str(week_ending)
            
            driver_name = record.get('driver_name', '')
            metric = record.get('metric', '')
            amount = record.get('amount', 0) or 0
            
            key = (date_str, driver_name)
            
            if metric in daily_data[key]:
                daily_data[key][metric] += amount
        
        # Convert to list of records
        result = []
        for (date, driver_name), metrics in daily_data.items():
            result.append({
                'date': date,
                'driver_name': driver_name,
                'total_sales': metrics['Total Sales'],
                'net_sales': metrics['Net Sales'],
                'total_miles': metrics['Total Miles'],
                'dmr': metrics['DMR'],
                'num_of_checks': int(metrics['# of Checks']),
                'amount_of_checks': metrics['Amount of Checks'],
                'cash_due': metrics['Cash Due'],
                'tips': metrics['Tips'],
                'taxable_income': metrics['Taxable Income']
            })
        
        return result
    
    def _aggregate_financial_daily_to_weekly(self, daily_financial: Dict[str, List[Dict]]) -> List[Dict]:
        """Aggregate daily financial line items into weekly totals"""
        weekly_items = defaultdict(float)
        
        for date, items in daily_financial.items():
            for item_dict in items:
                line_item = item_dict['line_item']
                amount = item_dict['amount']
                weekly_items[line_item] += amount
        
        return [{'line_item': item, 'amount': amt} for item, amt in weekly_items.items()]
    
    # ========================================================================
    # NEW: UNIFIED AGGREGATION FUNCTIONS
    # ========================================================================
    
    def _get_line_item_category(self, line_item: str) -> str:
        """
        Determine the category for a line item
        Returns: 'revenue', 'deduction', 'calculation', 'financial', 'metric', '3pf', or 'unknown'
        """
        for category, items in self.LINE_ITEM_CATEGORIES.items():
            if line_item in items:
                return category
        return 'unknown'
    
    def _aggregate_all_items_to_daily(self, parsed_data: Dict[str, Any]) -> List[Dict]:
        """
        Aggregate ALL line items (AM + PM) into daily totals
        Returns: List of records ready for wsr_items_daily table
        """
        daily_items = defaultdict(lambda: {
            'amount': 0.0,
            'quantity': 0
        })
        
        # Get metadata
        metadata = parsed_data.get('metadata', {})
        store_number = metadata.get('store_number', '')
        week_ending = metadata.get('week_ending', '')
        week_number = metadata.get('week_number', 0)
        year = metadata.get('year', 0)
        
        # -------------------------------------------------------------------------
        # 1. SALES DATA (revenue, deductions, calculations)
        # -------------------------------------------------------------------------
        sales_data = parsed_data.get('sales_data', [])
        
        for record in sales_data:
            date = record.get('date', '')
            line_item = record.get('category', '')
            amount = record.get('sales_amount', 0)
            quantity = record.get('quantity', 0)
            
            key = (date, line_item)
            daily_items[key]['amount'] += amount
            daily_items[key]['quantity'] += quantity
        
        # -------------------------------------------------------------------------
        # 2. FINANCIAL DATA (tax, A/R, deposits, 3PF)
        # -------------------------------------------------------------------------
        financial_data = parsed_data.get('financial_data', [])
        
        for record in financial_data:
            date = record.get('date', '')
            line_item = record.get('financial_category', '')
            amount = record.get('amount', 0)
            
            key = (date, line_item)
            daily_items[key]['amount'] += amount
        
        # -------------------------------------------------------------------------
        # 3. LABOR DATA (Labor $, Labor %, etc.)
        # -------------------------------------------------------------------------
        labor_data = parsed_data.get('labor_data', [])
        
        for record in labor_data:
            date = record.get('date', '')
            
            # Aggregate by labor type
            labor_types = {
                'Labor $': record.get('labor_dollars', 0),
                'Labor OverTime $': (record.get('ot_pay', 0) or 0)
            }
            
            for line_item, amount in labor_types.items():
                if amount != 0:  # Only add non-zero values
                    key = (date, line_item)
                    daily_items[key]['amount'] += amount
        
        # -------------------------------------------------------------------------
        # Convert to list format for Supabase upload
        # -------------------------------------------------------------------------
        result = []
        
        for (date, line_item), data in daily_items.items():
            category = self._get_line_item_category(line_item)
            
            result.append({
                'store_number': store_number,
                'date': date,
                'week_ending': week_ending,
                'week_number': week_number,
                'year': year,
                'line_item': line_item,
                'category': category,
                'amount': round(data['amount'], 2),
                'quantity': data['quantity']
            })
        
        return result
    
    def _aggregate_daily_to_weekly_unified(self, daily_items: List[Dict]) -> List[Dict]:
        """
        Aggregate daily items into weekly totals
        Returns: List of records ready for wsr_items_weekly table
        """
        if not daily_items:
            return []
        
        # Group by store + week + line_item
        weekly_agg = defaultdict(lambda: {
            'amount': 0.0,
            'quantity': 0,
            'num_days': 0,
            'category': '',
            'store_number': '',
            'week_ending': '',
            'week_number': 0,
            'year': 0
        })
        
        for item in daily_items:
            key = (item['store_number'], item['week_ending'], item['line_item'])
            
            weekly_agg[key]['amount'] += item['amount']
            weekly_agg[key]['quantity'] += item['quantity']
            weekly_agg[key]['num_days'] += 1
            
            # Store metadata (same for all days in the week)
            if not weekly_agg[key]['category']:
                weekly_agg[key]['category'] = item['category']
                weekly_agg[key]['store_number'] = item['store_number']
                weekly_agg[key]['week_ending'] = item['week_ending']
                weekly_agg[key]['week_number'] = item['week_number']
                weekly_agg[key]['year'] = item['year']
        
        # Convert to list
        result = []
        for (store_number, week_ending, line_item), data in weekly_agg.items():
            avg_daily = data['amount'] / data['num_days'] if data['num_days'] > 0 else 0
            
            result.append({
                'store_number': data['store_number'],
                'week_ending': data['week_ending'],
                'week_number': data['week_number'],
                'year': data['year'],
                'line_item': line_item,
                'category': data['category'],
                'amount': round(data['amount'], 2),
                'quantity': data['quantity'],
                'avg_daily_amount': round(avg_daily, 2)
            })
        
        return result
    
    # ========================================================================
    # UPDATED to_supabase_format() WITH UNIFIED AGGREGATION
    # ========================================================================
    
    def to_supabase_format(self, parsed_data: Dict[str, Any]) -> Dict[str, List[Dict]]:
        """
        Convert parsed data to Supabase format with DAILY AGGREGATION ONLY
        
        GENERATES:
        - Original tables (wsr_sales, wsr_labor, wsr_financial, wsr_inventory, wsr_headers)
        - Daily aggregation: wsr_sales_daily (with detailed columns), wsr_labor_daily, wsr_financial_daily
        """
        result = {
            # Original tables
            'wsr_headers': [],
            'wsr_sales': [],
            'wsr_inventory': [],
            'wsr_inventory_unit_costs': [],  # Item-level unit costs for tracked items
            'wsr_labor': [],
            'wsr_labor_metrics': [],  # NEW: Labor metrics from Weekly Sales (shift-level)
            'wsr_financial': [],
            
            # DAILY aggregated tables ONLY (matching existing database)
            'wsr_sales_daily': [],
            'wsr_labor_daily': [],
            'wsr_financial_daily': [],
            
            # DMR tables (raw shift-level + daily aggregated)
            'wsr_dmr': [],
            'wsr_dmr_daily': [],
            
            # Labor Cost Summary (daily timestamped data)
            'wsr_labor_cost_summary': []
        }
        
        duplicates_removed = {
            'sales': 0,
            'inventory': 0,
            'labor': 0,
            'financial': 0
        }
        
        metadata = parsed_data['metadata']
        
        # Validate required metadata
        required_meta = ['store_number', 'week_ending', 'week_number', 'year']
        if not all(metadata.get(field) for field in required_meta):
            missing = [f for f in required_meta if not metadata.get(f)]
            raise ValueError(f"Missing required metadata: {', '.join(missing)}")
        
        # ========================================================================
        # HEADER RECORD (unchanged)
        # ========================================================================
        header = {
            'store_number': self._clean_value(metadata.get('store_number')),
            'week_ending': metadata.get('week_ending'),
            'week_number': self._clean_value(metadata.get('week_number')),
            'year': self._clean_value(metadata.get('year')),
            'general_manager': self._clean_value(metadata.get('general_manager')),
            'store_location': self._clean_value(metadata.get('store_location')),
            'city': self._clean_value(metadata.get('city')),
            'state': self._clean_value(metadata.get('state')),
            'processed_at': parsed_data.get('parse_timestamp')
        }
        
        if self._validate_record(header, required_meta):
            result['wsr_headers'].append(header)
        else:
            raise ValueError(f"Header record missing required fields")
        
        # ========================================================================
        # RAW SALES RECORDS (unchanged - keep AM/PM granularity)
        # ========================================================================
        sales_seen = set()
        for item in parsed_data.get('sales_data', []):
            record = {
                'store_number': self._clean_value(metadata.get('store_number')),
                'week_ending': metadata.get('week_ending'),
                'week_number': self._clean_value(metadata.get('week_number')),
                'year': self._clean_value(metadata.get('year')),
                'date': item['date'],
                'shift': item['shift'],
                'category': item['category'],
                'category_type': item.get('category_type', 'revenue'),
                'sales_amount': self._clean_value(item['sales_amount']),
                'quantity': self._clean_value(item['quantity'])
            }
            
            unique_key = (
                record['store_number'],
                record['week_ending'],
                record['date'],
                record['shift'],
                record['category']
            )
            
            if self._validate_record(record, ['store_number', 'week_ending', 'date', 'shift', 'category']):
                if unique_key not in sales_seen:
                    result['wsr_sales'].append(record)
                    sales_seen.add(unique_key)
                else:
                    duplicates_removed['sales'] += 1
        
        # ========================================================================
        # EXISTING AGGREGATED SALES - DAILY
        # ========================================================================
        daily_sales = self._aggregate_sales_to_daily(parsed_data.get('sales_data', []))
        
        for date, daily_data in daily_sales.items():
            record = {
                'store_number': self._clean_value(metadata.get('store_number')),
                'date': date,
                'week_ending': metadata.get('week_ending'),
                'week_number': self._clean_value(metadata.get('week_number')),
                'year': self._clean_value(metadata.get('year')),
                
                # InShop revenue (detailed)
                'in_sub': self._clean_value(daily_data['in_sub']),
                'in_club': self._clean_value(daily_data['in_club']),
                'in_pop': self._clean_value(daily_data['in_pop']),
                'in_side': self._clean_value(daily_data['in_side']),
                'in_combos': self._clean_value(daily_data['in_combos']),
                
                # Delivery revenue (detailed)
                'del_sub': self._clean_value(daily_data['del_sub']),
                'del_club': self._clean_value(daily_data['del_club']),
                'del_pop': self._clean_value(daily_data['del_pop']),
                'del_side': self._clean_value(daily_data['del_side']),
                'del_combos': self._clean_value(daily_data['del_combos']),
                
                # Other revenue
                'day_old_bread': self._clean_value(daily_data['day_old_bread']),
                'fresh_bread': self._clean_value(daily_data['fresh_bread']),
                'cookie': self._clean_value(daily_data['cookie']),
                'box_lunch': self._clean_value(daily_data['box_lunch']),
                'platters': self._clean_value(daily_data['platters']),
                'delivery_fee': self._clean_value(daily_data['delivery_fee']),
                'other_revenue': self._clean_value(daily_data['other_revenue']),
                'modifiers': self._clean_value(daily_data['modifiers']),
                
                # Calculations
                'total_of_above': self._clean_value(daily_data['total_of_above']),
                'over_rings': self._clean_value(daily_data['over_rings']),
                'adjusted_sales': self._clean_value(daily_data['adjusted_sales']),
                
                # Deductions
                'net_employee_freebies': self._clean_value(daily_data['net_employee_freebies']),
                'net_manager_freebies': self._clean_value(daily_data['net_manager_freebies']),
                'sampling': self._clean_value(daily_data['sampling']),
                'waste': self._clean_value(daily_data['waste']),
                'other_promo': self._clean_value(daily_data['other_promo']),
                'loyalty_coupon': self._clean_value(daily_data['loyalty_coupon']),
                'total_deductions': self._clean_value(daily_data['total_deductions']),
                
                # Final calculation
                'royalty_sales': self._clean_value(daily_data['royalty_sales']),
                
                # 3rd Party
                'doordash': self._clean_value(daily_data['doordash']),
                'grubhub': self._clean_value(daily_data['grubhub']),
                'ubereats': self._clean_value(daily_data['ubereats']),
                
                # Metrics
                'num_of_sales': self._clean_value(daily_data['num_of_sales']),
                'num_of_checks': self._clean_value(daily_data['num_of_checks']),
                'total_online_orders': self._clean_value(daily_data['total_online_orders']),
                'total_quantity': self._clean_value(daily_data['total_quantity'])
            }
            
            result['wsr_sales_daily'].append(record)
        
        # ========================================================================
        # RAW FINANCIAL RECORDS (unchanged - keep AM/PM granularity)
        # ========================================================================
        financial_seen = set()
        for item in parsed_data.get('financial_data', []):
            record = {
                'store_number': self._clean_value(metadata.get('store_number')),
                'week_ending': metadata.get('week_ending'),
                'week_number': self._clean_value(metadata.get('week_number')),
                'year': self._clean_value(metadata.get('year')),
                'date': item['date'],
                'shift': item['shift'],
                'financial_category': item['financial_category'],
                'amount': self._clean_value(item['amount'])
            }
            
            unique_key = (
                record['store_number'],
                record['week_ending'],
                record['date'],
                record['shift'],
                record['financial_category']
            )
            
            if self._validate_record(record, ['store_number', 'week_ending', 'date', 'shift', 'financial_category']):
                if unique_key not in financial_seen:
                    result['wsr_financial'].append(record)
                    financial_seen.add(unique_key)
                else:
                    duplicates_removed['financial'] += 1
        
        # ========================================================================
        # AGGREGATED FINANCIAL - DAILY (wsr_financial_daily table)
        # ========================================================================
        daily_financial = self._aggregate_financial_to_daily(parsed_data.get('financial_data', []))
        
        for date, items in daily_financial.items():
            for item_dict in items:
                record = {
                    'store_number': self._clean_value(metadata.get('store_number')),
                    'date': date,
                    'week_ending': metadata.get('week_ending'),
                    'week_number': self._clean_value(metadata.get('week_number')),
                    'year': self._clean_value(metadata.get('year')),
                    'line_item': item_dict['line_item'],
                    'amount': self._clean_value(item_dict['amount'])
                }
                
                result['wsr_financial_daily'].append(record)
        
        # ========================================================================
        # INVENTORY RECORDS - ONLY IF COMPLETE WEEK
        # ========================================================================
        is_complete = self._is_complete_week(parsed_data.get('sales_data', []))
        
        if is_complete:
            inventory_seen = set()
            for item in parsed_data.get('inventory_data', []):
                record = {
                    'store_number': self._clean_value(metadata.get('store_number')),
                    'week_ending': metadata.get('week_ending'),
                    'week_number': self._clean_value(metadata.get('week_number')),
                    'year': self._clean_value(metadata.get('year')),
                    'category': item['category']
                }
                
                # Add optional COS fields
                if 'cos_dollars' in item:
                    record['cos_dollars'] = self._clean_value(item['cos_dollars'])
                if 'cos_percent' in item:
                    record['cos_percent'] = self._clean_value(item['cos_percent'])
                
                unique_key = (
                    record['store_number'],
                    record['week_ending'],
                    record['category']
                )
                
                if self._validate_record(record, ['store_number', 'week_ending', 'category']):
                    if unique_key not in inventory_seen:
                        result['wsr_inventory'].append(record)
                        inventory_seen.add(unique_key)
                    else:
                        duplicates_removed['inventory'] += 1
            
            # Also upload inventory unit costs when week is complete
            unit_cost_seen = set()
            for item in parsed_data.get('inventory_unit_costs', []):
                record = {
                    'store_number': self._clean_value(metadata.get('store_number')),
                    'week_ending': metadata.get('week_ending'),
                    'week_number': self._clean_value(metadata.get('week_number')),
                    'year': self._clean_value(metadata.get('year')),
                    'item_name': item['item_name'],
                    'category': item.get('category'),
                    'unit_cost': self._clean_value(item['unit_cost']),
                    'unit_of_measure': item.get('unit_of_measure'),
                }
                
                unique_key = (
                    record['store_number'],
                    record['week_ending'],
                    record['item_name']
                )
                
                if self._validate_record(record, ['store_number', 'week_ending', 'item_name', 'unit_cost']):
                    if unique_key not in unit_cost_seen:
                        result['wsr_inventory_unit_costs'].append(record)
                        unit_cost_seen.add(unique_key)
        
        # ========================================================================
        # LABOR COST SUMMARY - DAILY SNAPSHOTS (NO LONGER RESTRICTED TO COMPLETE WEEK)
        # ========================================================================
        # Calculate "as_of" date (yesterday - representing the data as of end of that day)
        as_of_date = datetime.now().date() - timedelta(days=1)
        as_of_date_str = as_of_date.strftime('%Y-%m-%d')
        
        for item in parsed_data.get('labor_cost_summary', []):
            record = {
                'store_number': self._clean_value(metadata.get('store_number')),
                'as_of': as_of_date_str,  # New: Yesterday's date (when snapshot was taken)
                'date': item['date'],  # Original week_ending date for reference
                'week_ending': metadata.get('week_ending'),
                'week_number': self._clean_value(metadata.get('week_number')),
                'year': self._clean_value(metadata.get('year')),
                'category': item['category'],
                'amount': self._clean_value(item['amount']),
                'labor_percent': self._clean_value(item.get('labor_percent')),  # Add percentage
                'uploaded_at': item.get('uploaded_at')
            }
            
            if self._validate_record(record, ['store_number', 'as_of', 'date', 'category']):
                result['wsr_labor_cost_summary'].append(record)

        # ========================================================================
        # INJECT VACATION + VACATION TAX INTO wsr_labor_metrics
        # So the dashboard total labor calculation includes these costs
        # Stored with shift='WEEK' as weekly totals (not shift-level)
        # ========================================================================
        vacation_categories = {'Vacation': 'vacation_dollars', 'Vacation Tax': 'vacation_tax_dollars'}
        for item in parsed_data.get('labor_cost_summary', []):
            category = item.get('category', '')
            if category in vacation_categories:
                metric_name = vacation_categories[category]
                amount = self._clean_value(item.get('amount'))
                if amount is not None and amount != 0:
                    week_ending = metadata.get('week_ending')
                    record = {
                        'store_number': self._clean_value(metadata.get('store_number')),
                        'week_ending': week_ending,
                        'week_number': self._clean_value(metadata.get('week_number')),
                        'year': self._clean_value(metadata.get('year')),
                        'date': week_ending,  # Use week_ending date for weekly totals
                        'shift': 'WEEK',
                        'metric_name': metric_name,
                        'metric_value': amount
                    }
                    if self._validate_record(record, ['store_number', 'week_ending', 'date', 'shift', 'metric_name']):
                        result['wsr_labor_metrics'].append(record)

        # ========================================================================
        # RAW LABOR RECORDS (unchanged - keep AM/PM granularity)
        # ========================================================================
        labor_seen = set()
        for item in parsed_data.get('labor_data', []):
            record = {
                'store_number': self._clean_value(metadata.get('store_number')),
                'week_ending': metadata.get('week_ending'),
                'week_number': self._clean_value(metadata.get('week_number')),
                'year': self._clean_value(metadata.get('year')),
                'date': item['date'],
                'shift': item['shift'],
                'labor_type': item['labor_type'],
                'labor_dollars': self._clean_value(item['labor_dollars']),
                'straight_pay': self._clean_value(item.get('straight_pay')),
                'ot_pay': self._clean_value(item.get('ot_pay')),
                'penalty_pay': self._clean_value(item.get('penalty_pay')),
                'dmr_expense': self._clean_value(item.get('dmr_expense')),
                'total_miles': self._clean_value(item.get('total_miles'))
            }
            
            unique_key = (
                record['store_number'],
                record['week_ending'],
                record['date'],
                record['shift'],
                record['labor_type']
            )
            
            if self._validate_record(record, ['store_number', 'week_ending', 'date', 'shift', 'labor_type']):
                if unique_key not in labor_seen:
                    result['wsr_labor'].append(record)
                    labor_seen.add(unique_key)
                else:
                    duplicates_removed['labor'] += 1
        
        # ========================================================================
        # LABOR METRICS FROM WEEKLY SALES (NEW - shift-level)
        # ========================================================================
        labor_metrics_seen = set()
        for item in parsed_data.get('labor_metrics', []):
            record = {
                'store_number': self._clean_value(metadata.get('store_number')),
                'week_ending': metadata.get('week_ending'),
                'week_number': self._clean_value(metadata.get('week_number')),
                'year': self._clean_value(metadata.get('year')),
                'date': item['date'],
                'shift': item['shift'],
                'metric_name': item['metric_name'],  # labor_dollars, labor_percent, labor_overtime
                'metric_value': self._clean_value(item['metric_value'])
            }
            
            unique_key = (
                record['store_number'],
                record['week_ending'],
                record['date'],
                record['shift'],
                record['metric_name']
            )
            
            if self._validate_record(record, ['store_number', 'week_ending', 'date', 'shift', 'metric_name']):
                if unique_key not in labor_metrics_seen:
                    result['wsr_labor_metrics'].append(record)
                    labor_metrics_seen.add(unique_key)
        
        # ========================================================================
        # AGGREGATED LABOR - DAILY (combining Labor tabs + Weekly Sales metrics)
        # ========================================================================
        daily_labor = self._aggregate_labor_to_daily(parsed_data.get('labor_data', []))
        daily_labor_metrics = self._aggregate_labor_metrics_to_daily(parsed_data.get('labor_metrics', []))
        
        # Calculate labor percentages from Labor tabs data
        self._calculate_labor_percentages(daily_labor, daily_sales)
        
        # Merge all dates (from both labor tabs and weekly sales metrics)
        all_dates = set(daily_labor.keys()) | set(daily_labor_metrics.keys())
        
        for date in all_dates:
            labor_data = daily_labor.get(date, {})
            metrics_data = daily_labor_metrics.get(date, {})
            
            record = {
                'store_number': self._clean_value(metadata.get('store_number')),
                'date': date,
                'week_ending': metadata.get('week_ending'),
                'week_number': self._clean_value(metadata.get('week_number')),
                'year': self._clean_value(metadata.get('year')),
                
                # From Labor tabs (detailed breakdown)
                'manager_labor': self._clean_value(labor_data.get('manager_labor', 0)),
                'inshop_labor': self._clean_value(labor_data.get('inshop_labor', 0)),
                'driver_labor': self._clean_value(labor_data.get('driver_labor', 0)),
                'driver_straight_pay': self._clean_value(labor_data.get('driver_straight_pay', 0)),
                'driver_ot_pay': self._clean_value(labor_data.get('driver_ot_pay', 0)),
                'driver_penalty_pay': self._clean_value(labor_data.get('driver_penalty_pay', 0)),
                'driver_dmr_expense': self._clean_value(labor_data.get('driver_dmr_expense', 0)),
                'total_labor': self._clean_value(labor_data.get('total_labor', 0)),
                'overtime_labor': self._clean_value(labor_data.get('overtime_labor', 0)),
                'labor_percentage': self._clean_value(labor_data.get('labor_percentage', 0)),
                
                # From Weekly Sales tab (official totals)
                'wsr_labor_dollars': self._clean_value(metrics_data.get('labor_dollars', 0)),
                'wsr_labor_percent': self._clean_value(metrics_data.get('labor_percent', 0)),
                'wsr_labor_overtime': self._clean_value(metrics_data.get('labor_overtime', 0))
            }
            
            result['wsr_labor_daily'].append(record)
        
        # ========================================================================
        # DMR DATA - RAW SHIFT-LEVEL
        # ========================================================================
        dmr_seen = set()
        for item in parsed_data.get('dmr_data', []):
            # Calculate date from shift_num (1-2=Mon, 3-4=Tue, etc.)
            if 'shift_num' in item and metadata.get('week_ending_date'):
                shift_num = item['shift_num']
                day_offset = ((shift_num - 1) // 2)  # 0-6 for Mon-Sun
                date_offset = day_offset - 1  # Monday = -1 from Tuesday
                week_ending_date = metadata.get('week_ending_date')
                # Ensure it's a date object
                if isinstance(week_ending_date, str):
                    from datetime import date as date_type
                    week_ending_date = datetime.strptime(week_ending_date, '%Y-%m-%d').date()
                item_date = week_ending_date + timedelta(days=date_offset)
            else:
                item_date = metadata.get('week_ending_date') or metadata.get('week_ending')
            
            # Convert to string for database
            if hasattr(item_date, 'strftime'):
                date_str = item_date.strftime('%Y-%m-%d')
            else:
                date_str = str(item_date)
            
            record = {
                'store_number': self._clean_value(metadata.get('store_number')),
                'week_ending_date': metadata.get('week_ending'),
                'date': date_str,
                'shift': item.get('shift'),
                'shift_num': self._clean_value(item.get('shift_num')),
                'driver_name': self._clean_value(item.get('driver_name')),
                'metric': item.get('metric'),
                'amount': self._clean_value(item.get('amount'))
            }
            
            unique_key = (
                record['store_number'],
                record['date'],
                record['shift_num'],
                record['driver_name'],
                record['metric']
            )
            
            if self._validate_record(record, ['store_number', 'week_ending_date', 'date', 'driver_name', 'metric']):
                if unique_key not in dmr_seen:
                    result['wsr_dmr'].append(record)
                    dmr_seen.add(unique_key)
        
        # ========================================================================
        # DMR DATA - AGGREGATED DAILY
        # ========================================================================
        dmr_daily = self._aggregate_dmr_to_daily(parsed_data.get('dmr_data', []), metadata)
        
        for item in dmr_daily:
            record = {
                'store_number': self._clean_value(metadata.get('store_number')),
                'week_ending_date': metadata.get('week_ending'),
                'date': item['date'],
                'driver_name': self._clean_value(item['driver_name']),
                'total_sales': self._clean_value(item.get('total_sales')),
                'net_sales': self._clean_value(item.get('net_sales')),
                'total_miles': self._clean_value(item.get('total_miles')),
                'dmr': self._clean_value(item.get('dmr')),
                'num_of_checks': self._clean_value(item.get('num_of_checks')),
                'amount_of_checks': self._clean_value(item.get('amount_of_checks')),
                'cash_due': self._clean_value(item.get('cash_due')),
                'tips': self._clean_value(item.get('tips')),
                'taxable_income': self._clean_value(item.get('taxable_income'))
            }
            
            if self._validate_record(record, ['store_number', 'week_ending_date', 'date', 'driver_name']):
                result['wsr_dmr_daily'].append(record)
        
        # ========================================================================
        # METADATA
        # ========================================================================
        result['_duplicates_removed'] = duplicates_removed
        result['_is_complete_week'] = is_complete
        result['_inventory_uploaded'] = is_complete
        
        print(f"\n✓ Data Processing Summary:")
        print(f"  Shift-Level Tables (AM/PM):")
        print(f"  - wsr_headers: {len(result['wsr_headers'])} record")
        print(f"  - wsr_sales: {len(result['wsr_sales'])} records")
        print(f"  - wsr_labor: {len(result['wsr_labor'])} records")
        print(f"  - wsr_labor_metrics: {len(result['wsr_labor_metrics'])} records")
        print(f"  - wsr_financial: {len(result['wsr_financial'])} records")
        print(f"  - wsr_inventory: {len(result['wsr_inventory'])} records" + (" (complete week)" if is_complete else " (SKIPPED - incomplete week)"))
        print(f"  - wsr_inventory_unit_costs: {len(result['wsr_inventory_unit_costs'])} records" + (" (complete week)" if is_complete else " (SKIPPED - incomplete week)"))
        print(f"  - wsr_dmr: {len(result['wsr_dmr'])} records")
        print(f"  Daily Aggregated Tables:")
        print(f"  - wsr_sales_daily: {len(result['wsr_sales_daily'])} records")
        print(f"  - wsr_labor_daily: {len(result['wsr_labor_daily'])} records")
        print(f"  - wsr_financial_daily: {len(result['wsr_financial_daily'])} records")
        print(f"  - wsr_dmr_daily: {len(result['wsr_dmr_daily'])} records")
        print(f"  - wsr_labor_cost_summary: {len(result['wsr_labor_cost_summary'])} records (daily snapshot)")
        
        return result
    
    def validate_parsed_data(self, parsed_data: Dict[str, Any], file_name: str) -> Dict[str, Any]:
        """Validate that all expected data was parsed"""
        validation = {
            'valid': True,
            'warnings': [],
            'errors': [],
            'counts': {}
        }
        
        # Count records by type
        sales_data = parsed_data.get('sales_data', [])
        sales_count = len(sales_data)
        
        revenue_count = sum(1 for s in sales_data if s.get('category_type') == 'revenue')
        deduction_count = sum(1 for s in sales_data if s.get('category_type') == 'deduction')
        third_party_count = sum(1 for s in sales_data if s.get('category_type') == 'third_party')
        metric_count = sum(1 for s in sales_data if s.get('category_type') == 'metric')
        
        inventory_count = len(parsed_data.get('inventory_data', []))
        inventory_unit_costs_count = len(parsed_data.get('inventory_unit_costs', []))
        labor_count = len(parsed_data.get('labor_data', []))
        financial_count = len(parsed_data.get('financial_data', []))
        
        # Check if complete week
        is_complete_week = self._is_complete_week(sales_data)
        
        validation['counts'] = {
            'sales_total': sales_count,
            'sales_revenue': revenue_count,
            'sales_deductions': deduction_count,
            'sales_third_party': third_party_count,
            'sales_metrics': metric_count,
            'inventory': inventory_count,
            'inventory_unit_costs': inventory_unit_costs_count,
            'labor': labor_count,
            'financial': financial_count,
            'is_complete_week': is_complete_week
        }
        
        # Check for missing data
        if sales_count == 0:
            validation['errors'].append("No sales data parsed")
            validation['valid'] = False
        elif sales_count < 10:
            validation['warnings'].append(f"Only {sales_count} sales records")
        
        if revenue_count == 0:
            validation['errors'].append("No revenue data parsed")
            validation['valid'] = False
        
        if deduction_count == 0:
            validation['warnings'].append("No deduction categories found")
        
        if third_party_count == 0:
            validation['warnings'].append("No 3rd party sales found (DoorDash/GrubHub/UberEats)")
        
        if financial_count == 0:
            validation['warnings'].append("No financial reconciliation data parsed")
        elif financial_count < 10:
            validation['warnings'].append(f"Only {financial_count} financial records (expected 100+)")
        
        if inventory_count == 0:
            if is_complete_week:
                validation['errors'].append("No inventory data parsed - CRITICAL (complete week)")
                validation['valid'] = False
            else:
                validation['warnings'].append("No inventory data (incomplete week - expected)")
        
        if labor_count == 0:
            validation['warnings'].append("No labor data parsed")
        
        if not is_complete_week:
            validation['warnings'].append("Incomplete week - inventory will not be uploaded")
        
        # Check metadata
        metadata = parsed_data.get('metadata', {})
        required_meta = ['store_number', 'week_ending', 'week_number', 'year']
        missing_meta = [f for f in required_meta if not metadata.get(f)]
        if missing_meta:
            validation['errors'].append(f"Missing metadata: {', '.join(missing_meta)}")
            validation['valid'] = False
        
        return validation

    def export_to_excel(self, parsed_data: Dict[str, Any], file_name: str, export_dir: Path) -> str:
        """
        Export parsed data to Excel file for auditing
        
        Args:
            parsed_data: Dictionary with all parsed data tables
            file_name: Original WSR file name
            export_dir: Directory to save the Excel file
        
        Returns:
            Path to created Excel file
        """
        # Convert to Supabase format to get the properly structured data
        supabase_data = self.to_supabase_format(parsed_data)
        
        metadata = parsed_data.get('metadata', {})
        store_number = metadata.get('store_number', 'UNKNOWN')
        week_number = metadata.get('week_number', 'XX')
        year = metadata.get('year', 'YYYY')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        excel_filename = f"AUDIT_{store_number}_Week_{week_number}_{year}_{timestamp}.xlsx"
        excel_path = export_dir / excel_filename
        
        print(f"{Colors.CYAN}Creating Excel audit export...{Colors.RESET}")
        print(f"  File: {excel_filename}")
        
        with pd.ExcelWriter(str(excel_path), engine='openpyxl') as writer:
            
            # 1. SUMMARY TAB
            summary_data = []
            summary_data.append(['STORE METADATA', ''])
            summary_data.append(['Store Number', store_number])
            summary_data.append(['Week Ending', metadata.get('week_ending', 'N/A')])
            summary_data.append(['Week Number', week_number])
            summary_data.append(['Year', year])
            summary_data.append(['Original File', file_name])
            summary_data.append(['Export Date', datetime.now().strftime('%Y-%m-%d %H:%M:%S')])
            summary_data.append(['', ''])
            
            summary_data.append(['RECORD COUNTS', ''])
            # Count records in each table
            tables_to_count = [
                ('wsr_sales', 'Sales (Shift Level)'),
                ('wsr_sales_daily', 'Sales (Daily Aggregated)'), 
                ('wsr_labor', 'Labor (Shift Level)'),
                ('wsr_labor_metrics', 'Labor Metrics (Shift Level - from Weekly Sales)'),
                ('wsr_labor_daily', 'Labor (Daily Aggregated)'),
                ('wsr_labor_cost_summary', 'Labor Cost Summary'),
                ('wsr_financial', 'Financial (Shift Level)'),
                ('wsr_financial_daily', 'Financial (Daily Aggregated)'),
                ('wsr_inventory', 'Inventory'),
                ('wsr_inventory_unit_costs', 'Inventory Unit Costs'),
                ('wsr_dmr', 'DMR (Shift Level)'),
                ('wsr_dmr_daily', 'DMR (Daily Aggregated)')
            ]
            
            total_records = 0
            for table_key, table_name in tables_to_count:
                count = len(supabase_data.get(table_key, []))
                summary_data.append([table_name, count])
                total_records += count
            
            summary_data.append(['', ''])
            summary_data.append(['TOTAL RECORDS', total_records])
            
            summary_df = pd.DataFrame(summary_data, columns=['Metric', 'Value'])
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            # 2. SALES SHIFT LEVEL TAB
            if 'wsr_sales' in supabase_data and supabase_data['wsr_sales']:
                sales_df = pd.DataFrame(supabase_data['wsr_sales'])
                sales_df.to_excel(writer, sheet_name='Sales_ShiftLevel', index=False)
            
            # 3. SALES DAILY TAB  
            if 'wsr_sales_daily' in supabase_data and supabase_data['wsr_sales_daily']:
                sales_daily_df = pd.DataFrame(supabase_data['wsr_sales_daily'])
                sales_daily_df.to_excel(writer, sheet_name='Sales_Daily', index=False)
            
            # 4. LABOR SHIFT LEVEL TAB
            if 'wsr_labor' in supabase_data and supabase_data['wsr_labor']:
                labor_df = pd.DataFrame(supabase_data['wsr_labor'])
                labor_df.to_excel(writer, sheet_name='Labor_ShiftLevel', index=False)
            
            # 5. LABOR DAILY TAB
            if 'wsr_labor_daily' in supabase_data and supabase_data['wsr_labor_daily']:
                labor_daily_df = pd.DataFrame(supabase_data['wsr_labor_daily'])
                labor_daily_df.to_excel(writer, sheet_name='Labor_Daily', index=False)
            
            # 6. LABOR COST SUMMARY TAB
            if 'wsr_labor_cost_summary' in supabase_data and supabase_data['wsr_labor_cost_summary']:
                labor_cost_df = pd.DataFrame(supabase_data['wsr_labor_cost_summary'])
                labor_cost_df.to_excel(writer, sheet_name='Labor_Cost_Summary', index=False)
            
            # 7. FINANCIAL SHIFT LEVEL TAB
            if 'wsr_financial' in supabase_data and supabase_data['wsr_financial']:
                financial_df = pd.DataFrame(supabase_data['wsr_financial'])
                financial_df.to_excel(writer, sheet_name='Financial_ShiftLevel', index=False)
            
            # 8. FINANCIAL DAILY TAB
            if 'wsr_financial_daily' in supabase_data and supabase_data['wsr_financial_daily']:
                financial_daily_df = pd.DataFrame(supabase_data['wsr_financial_daily'])
                financial_daily_df.to_excel(writer, sheet_name='Financial_Daily', index=False)
            
            # 9. INVENTORY TAB (if complete week)
            if 'wsr_inventory' in supabase_data and supabase_data['wsr_inventory']:
                inventory_df = pd.DataFrame(supabase_data['wsr_inventory'])
                inventory_df.to_excel(writer, sheet_name='Inventory', index=False)
            
            # 9b. INVENTORY UNIT COSTS TAB (if complete week)
            if 'wsr_inventory_unit_costs' in supabase_data and supabase_data['wsr_inventory_unit_costs']:
                inv_uc_df = pd.DataFrame(supabase_data['wsr_inventory_unit_costs'])
                inv_uc_df.to_excel(writer, sheet_name='Inventory_UnitCosts', index=False)
            
            # 10. DMR SHIFT LEVEL TAB
            if 'wsr_dmr' in supabase_data and supabase_data['wsr_dmr']:
                dmr_df = pd.DataFrame(supabase_data['wsr_dmr'])
                dmr_df.to_excel(writer, sheet_name='DMR_ShiftLevel', index=False)
            
            # 11. DMR DAILY TAB
            if 'wsr_dmr_daily' in supabase_data and supabase_data['wsr_dmr_daily']:
                dmr_daily_df = pd.DataFrame(supabase_data['wsr_dmr_daily'])
                dmr_daily_df.to_excel(writer, sheet_name='DMR_Daily', index=False)
        
        print(f"{Colors.GREEN}✓ Excel audit export created: {excel_path}{Colors.RESET}")
        return str(excel_path)


# ============================================================================
# PROCESSOR AND CLI 
# ============================================================================

def extract_zips(source_dir: Path) -> Path:
    """Extract all zip files from source directory"""
    extract_path = Path(tempfile.mkdtemp(prefix="wsr_extract_"))
    
    print(f"{Colors.BLUE}Extracting WSR files...{Colors.RESET}")
    
    zip_files = list(source_dir.glob("*.zip"))
    if not zip_files:
        excel_files = list(source_dir.glob("*.xls*"))
        if excel_files:
            for excel_file in excel_files:
                shutil.copy2(excel_file, extract_path)
        return extract_path
    
    print(f"Found {len(zip_files)} zip files\n")
    
    total_extracted = 0
    
    for zip_path in zip_files:
        print(f"  Extracting: {zip_path.name}")
        
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                file_list = zip_ref.namelist()
                excel_files = [f for f in file_list if f.endswith(('.xls', '.xlsx'))]
                
                print(f"    Contains {len(excel_files)} Excel files")
                
                for file_name in excel_files:
                    target_path = extract_path / Path(file_name).name
                    
                    if target_path.exists():
                        continue
                    
                    zip_ref.extract(file_name, extract_path)
                    total_extracted += 1
                    
                    extracted_file = extract_path / file_name
                    if extracted_file.parent != extract_path:
                        shutil.move(str(extracted_file), str(target_path))
                
        except Exception as e:
            print(f"    {Colors.RED}Error: {e}{Colors.RESET}")
            continue
    
    print(f"\n{Colors.GREEN}Extracted {total_extracted} WSR files{Colors.RESET}\n")
    
    # Clean up empty subdirectories
    for item in extract_path.iterdir():
        if item.is_dir():
            try:
                item.rmdir()
            except:
                pass
    
    return extract_path


class WSRProcessorV4:
    """Processor with fiscal calendar, multi-tab parsing, validation, and aggregation"""
    
    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.parser = WSRParserV4()
        self.stats = defaultdict(int)
        self.fiscal_calendar = {}
        self.errors = []
        self.validation_failures = []
        self.failed_stores = []  # Track failed stores with details
        
        if not dry_run:
            self._init_supabase()
    
    def _init_supabase(self):
        """Initialize Supabase connection"""
        self.supabase_url = os.getenv('SUPABASE_URL')
        # Try both SUPABASE_KEY and SUPABASE_SERVICE_KEY for flexibility
        self.supabase_key = os.getenv('SUPABASE_KEY') or os.getenv('SUPABASE_SERVICE_KEY')
        
        if not self.supabase_url or not self.supabase_key:
            raise ValueError("Set SUPABASE_URL and SUPABASE_KEY in .env file or environment variables")
        
        try:
            from supabase import create_client
            self.supabase = create_client(self.supabase_url, self.supabase_key)
            
            # Test connection
            try:
                self.supabase.table('wsr_headers').select('*').limit(0).execute()
                print(f"{Colors.GREEN}✓ Connected to Supabase{Colors.RESET}")
                print(f"{Colors.GREEN}✓ Table 'wsr_headers' exists{Colors.RESET}\n")
            except Exception as table_err:
                print(f"{Colors.YELLOW}⚠️  Connected to Supabase but table check failed:{Colors.RESET}")
                print(f"{Colors.YELLOW}   {str(table_err)}{Colors.RESET}")
                print(f"{Colors.YELLOW}   Tables may not exist or service key lacks permissions{Colors.RESET}\n")
                
        except Exception as e:
            raise Exception(f"Failed to connect to Supabase: {e}")
    
    def _build_fiscal_calendar(self, directory: Path) -> Dict[int, Dict[str, int]]:
        """Build complete fiscal calendar from known weeks"""
        known_weeks = defaultdict(dict)
        excel_files = list(directory.glob("*.xls*"))
        
        print(f"{Colors.CYAN}Building fiscal calendar...{Colors.RESET}")
        
        # Scan for files with week numbers in filename
        anchor_files = []
        for file_path in excel_files:
            filename = file_path.stem
            week_match = re.search(r'Week (\d+), (\d{4})', filename)
            
            if week_match:
                try:
                    df = pd.read_excel(str(file_path), sheet_name='Weekly Sales', header=None, nrows=3)
                    week_ending = df.iloc[1, 2]
                    
                    if pd.notna(week_ending):
                        week_date = pd.to_datetime(week_ending)
                        if isinstance(week_ending, str):
                            week_date = pd.to_datetime(week_ending, format='%m/%d/%Y', errors='coerce')
                            if pd.isna(week_date):
                                week_date = pd.to_datetime(week_ending)
                        
                        if week_date.weekday() != 1:
                            days_ahead = 1 - week_date.weekday()
                            if days_ahead <= 0:
                                days_ahead += 7
                            week_date = week_date + timedelta(days=days_ahead)
                        
                        week_num = int(week_match.group(1))
                        year = int(week_match.group(2))
                        
                        known_weeks[year][week_num] = week_date
                        anchor_files.append((year, week_num, week_date, file_path.name))
                        
                except:
                    continue
        
        if anchor_files:
            print(f"{Colors.GREEN}Found {len(anchor_files)} anchor files with week numbers{Colors.RESET}")
        
        # Calculate all 52 weeks for each year
        complete_calendar = {}
        base_week_1 = {}
        
        for year, weeks_dict in sorted(known_weeks.items()):
            if not weeks_dict:
                continue
            
            known_week_num = list(weeks_dict.keys())[0]
            known_week_date = weeks_dict[known_week_num]
            
            week_1_date = known_week_date - timedelta(days=(known_week_num - 1) * 7)
            base_week_1[year] = week_1_date
            
            year_calendar = {}
            for week in range(1, 53):
                week_ending = week_1_date + timedelta(days=(week - 1) * 7)
                date_str = week_ending.strftime('%Y-%m-%d')
                year_calendar[date_str] = week
            
            complete_calendar[year] = year_calendar
            print(f"{Colors.GREEN}✓ {year} fiscal calendar built{Colors.RESET}")
        
        print()
        return complete_calendar
    
    def _lookup_fiscal_week(self, week_ending_date: str, year: int) -> tuple[Optional[int], str]:
        """Look up fiscal week number"""
        if year in self.fiscal_calendar:
            week_num = self.fiscal_calendar[year].get(week_ending_date)
            if week_num:
                return week_num, 'fiscal_calendar'
        
        # Fallback: ISO week
        try:
            date_obj = pd.to_datetime(week_ending_date)
            iso_week = date_obj.isocalendar()[1]
            return iso_week, 'iso_fallback'
        except:
            return None, 'failed'
    
    # Stores that have been permanently closed — skipped silently during parsing
    CLOSED_STORES = {1555, 2818, 2819, 2820, 2875, 3392, 3393}

    def process_file(self, file_path: Path, show_details: bool = False, export_dir: Optional[Path] = None) -> bool:
        """Process a single file with validation"""
        try:
            parsed_data = self.parser.parse_file(str(file_path))
            metadata = parsed_data['metadata']

            # Skip closed stores silently — don't count as failures
            store_num = metadata.get('store_number', '')
            try:
                if int(store_num) in self.CLOSED_STORES:
                    if show_details:
                        print(f"{Colors.YELLOW}⊘ Store {store_num} is closed — skipping{Colors.RESET}")
                    return True  # Return True so it doesn't count as a failure
            except (ValueError, TypeError):
                pass
            
            week_source = 'filename'
            
            # Lookup week if missing
            if not metadata.get('week_number') and metadata.get('week_ending') and metadata.get('year'):
                week_num, source = self._lookup_fiscal_week(metadata['week_ending'], metadata['year'])
                if week_num:
                    parsed_data['metadata']['week_number'] = week_num
                    metadata = parsed_data['metadata']
                    week_source = source
                else:
                    error_msg = f"Cannot determine week for {file_path.name}"
                    if show_details:
                        print(f"{Colors.RED}✗ {error_msg}{Colors.RESET}")
                    self.errors.append({'file': file_path.name, 'error': error_msg})
                    self.failed_stores.append({
                        'store': metadata.get('store_number', 'Unknown'),
                        'file': file_path.name,
                        'error': 'Cannot determine week number'
                    })
                    self.stats['failed'] += 1
                    return False
            
            # Validate parsed data
            validation = self.parser.validate_parsed_data(parsed_data, file_path.name)
            
            # Display validation status
            if show_details:
                complete_indicator = "✓ COMPLETE" if validation['counts']['is_complete_week'] else "⚠ PARTIAL"
                print(f"{Colors.GREEN}✓ Store {metadata.get('store_number')} - Week {metadata.get('week_number')}, {metadata.get('year')} {complete_indicator}{Colors.RESET}")
                counts = validation['counts']
                print(f"  Sales: {counts['sales_revenue']}R + {counts['sales_deductions']}D + {counts['sales_third_party']}3P + {counts['sales_metrics']}M = {counts['sales_total']} total")
                print(f"  Financial: {counts['financial']}, Inventory: {counts['inventory']}, Labor: {counts['labor']}")
            
            # Check validation
            if not validation['valid']:
                error_msg = f"Validation failed: {'; '.join(validation['errors'])}"
                if show_details:
                    print(f"{Colors.RED}  ✗ {error_msg}{Colors.RESET}")
                self.validation_failures.append({
                    'file': file_path.name,
                    'errors': validation['errors'],
                    'warnings': validation['warnings'],
                    'counts': validation['counts']
                })
                self.stats['validation_failed'] += 1
                return False
            
            # Show warnings
            if validation['warnings'] and show_details:
                for warning in validation['warnings']:
                    print(f"{Colors.YELLOW}  ⚠ {warning}{Colors.RESET}")
            
            # Always format data (for both test and process modes)
            try:
                supabase_data = self.parser.to_supabase_format(parsed_data)
            except Exception as e:
                error_msg = f"Data formatting failed: {str(e)}"
                if show_details:
                    print(f"{Colors.RED}  ✗ {error_msg}{Colors.RESET}")
                self.errors.append({'file': file_path.name, 'error': error_msg})
                self.failed_stores.append({
                    'store': metadata.get('store_number', 'Unknown'),
                    'file': file_path.name,
                    'error': f"Data formatting: {str(e)}"
                })
                self.stats['failed'] += 1
                return False
            
            # Only upload if not in test mode
            if not self.dry_run:
                try:
                    self._upload_to_supabase(supabase_data)
                except Exception as e:
                    error_msg = f"Upload failed: {str(e)}"
                    if show_details:
                        print(f"{Colors.RED}  ✗ {error_msg}{Colors.RESET}")
                    self.errors.append({'file': file_path.name, 'error': error_msg})
                    self.failed_stores.append({
                        'store': metadata.get('store_number', 'Unknown'),
                        'file': file_path.name,
                        'error': str(e)
                    })
                    self.stats['failed'] += 1
                    return False
            
            self.stats['processed'] += 1
            
            if week_source == 'iso_fallback':
                self.stats['iso_fallback'] += 1
            elif week_source == 'fiscal_calendar':
                self.stats['fiscal_calendar'] += 1
            
            # Export to Excel if requested
            if export_dir is not None:
                try:
                    excel_path = self.parser.export_to_excel(parsed_data, file_path.name, export_dir)
                    if show_details:
                        print(f"{Colors.CYAN}  → Excel export: {Path(excel_path).name}{Colors.RESET}")
                except Exception as e:
                    if show_details:
                        print(f"{Colors.RED}  ✗ Excel export failed: {str(e)}{Colors.RESET}")
                    self.errors.append({'file': file_path.name, 'error': f"Excel export failed: {str(e)}"})
            
            return True
            
        except Exception as e:
            error_msg = str(e)
            if show_details:
                print(f"{Colors.RED}✗ {file_path.name}: {error_msg}{Colors.RESET}")
            self.errors.append({'file': file_path.name, 'error': error_msg})
            # Try to extract store number from filename
            store_match = re.search(r'(\d{4})', file_path.name)
            store_num = store_match.group(1) if store_match else 'Unknown'
            self.failed_stores.append({
                'store': store_num,
                'file': file_path.name,
                'error': error_msg
            })
            self.stats['failed'] += 1
            return False
    
    def _upload_to_supabase(self, supabase_data: Dict):
        """Upload to Supabase with UNIFIED aggregated tables"""
        tables_config = {
            # Original tables
            'wsr_headers': ['store_number', 'week_ending'],
            'wsr_sales': ['store_number', 'week_ending', 'date', 'shift', 'category'],
            'wsr_inventory': ['store_number', 'week_ending', 'category'],
            'wsr_inventory_unit_costs': ['store_number', 'week_ending', 'item_name'],
            'wsr_labor': ['store_number', 'week_ending', 'date', 'shift', 'labor_type'],
            'wsr_labor_metrics': ['store_number', 'week_ending', 'date', 'shift', 'metric_name'],
            'wsr_financial': ['store_number', 'week_ending', 'date', 'shift', 'financial_category'],
            # Daily aggregated tables ONLY (matching existing database)
            'wsr_sales_daily': ['store_number', 'date'],
            'wsr_labor_daily': ['store_number', 'date'],
            'wsr_financial_daily': ['store_number', 'date', 'line_item'],
            # DMR tables
            'wsr_dmr': ['store_number', 'date', 'shift_num', 'driver_name', 'metric'],
            'wsr_dmr_daily': ['store_number', 'date', 'driver_name'],
            # Labor Cost Summary (daily snapshots)
            'wsr_labor_cost_summary': ['store_number', 'as_of', 'category']
        }
        
        for table_name, conflict_columns in tables_config.items():
            records = supabase_data.get(table_name, [])
            if not records:
                continue
            
            batch_size = 500

            # wsr_labor_cost_summary has two competing unique indexes. Delete only by
            # (store, week, year) — specific enough to avoid wiping other weeks' data.
            if table_name == 'wsr_labor_cost_summary' and records:
                combos = set(
                    (r.get('store_number'), r.get('week_number'), r.get('year'))
                    for r in records
                    if r.get('store_number') and r.get('week_number') and r.get('year')
                )
                for store_num, week_num, year_val in combos:
                    try:
                        self.supabase.table(table_name).delete()\
                            .eq('store_number', store_num)\
                            .eq('week_number', week_num)\
                            .eq('year', year_val)\
                            .execute()
                    except Exception as e:
                        print(f"  ⚠️  Pre-delete failed for store {store_num} week {week_num}: {e}")

            for i in range(0, len(records), batch_size):
                batch = records[i:i+batch_size]
                
                try:
                    # Validate JSON serializability
                    for idx, record in enumerate(batch):
                        try:
                            json.dumps(record)
                        except (TypeError, ValueError) as json_err:
                            for key, value in record.items():
                                try:
                                    json.dumps({key: value})
                                except:
                                    raise Exception(f"Record {idx} field '{key}' not JSON serializable: {type(value)} = {repr(value)}")
                            raise Exception(f"Record {idx} JSON error: {json_err}")
                    
                    # wsr_labor_cost_summary: pre-delete already ran, use plain insert
                    if table_name == 'wsr_labor_cost_summary':
                        response = self.supabase.table(table_name).insert(batch).execute()
                    else:
                        response = self.supabase.table(table_name).upsert(
                            batch,
                            on_conflict=','.join(conflict_columns)
                        ).execute()
                    
                    self.stats[f'{table_name}_records'] += len(batch)
                    
                except Exception as e:
                    error_msg = str(e)
                    if 'message' in error_msg or '404' in error_msg:
                        raise Exception(f"Supabase API error for {table_name}: {error_msg}")
                    raise Exception(f"Failed to upload {table_name}: {error_msg}")
    
    def process_directory(self, directory: Path, show_details: bool = True, export_dir: Optional[Path] = None) -> Dict:
        """Process directory"""
        excel_files = list(directory.glob("*.xls*"))
        
        if not excel_files:
            print(f"{Colors.YELLOW}No Excel files found{Colors.RESET}")
            return {'total': 0, 'processed': 0, 'failed': 0, 'validation_failed': 0}
        
        self.fiscal_calendar = self._build_fiscal_calendar(directory)
        
        print(f"Processing {len(excel_files)} files...\n")
        
        for i, file_path in enumerate(sorted(excel_files), 1):
            self.process_file(file_path, show_details, export_dir)
            
            if not show_details:
                print(f"\rProgress: {i}/{len(excel_files)}", end='', flush=True)
        
        if not show_details:
            print()
        
        return {
            'total': len(excel_files),
            'processed': self.stats['processed'],
            'failed': self.stats['failed'],
            'validation_failed': self.stats.get('validation_failed', 0)
        }




def send_email_notification(export_dir: Path, results: dict):
    """Send email notification with comprehensive audit reports attached"""
    print(f"\n{Colors.BLUE}{'='*60}{Colors.RESET}")
    print(f"{Colors.BLUE}📧 EMAIL NOTIFICATION{Colors.RESET}")
    print(f"{Colors.BLUE}{'='*60}{Colors.RESET}")
    
    try:
        # Check if sophisticated email notifier is available
        if EMAIL_NOTIFIER_AVAILABLE:
            print(f"{Colors.CYAN}Using WSREmailNotifier for detailed reports...{Colors.RESET}")
            
            # Create notifier instance
            notifier = WSREmailNotifier()
            
            # Get processor stats from global scope if available
            stats = getattr(send_email_notification, 'stats', {})
            
            # Send comprehensive email with summary + audit + sample reports
            notifier.send_email(
                stats=stats,
                results=results,
                source_dir=Path('wsr_downloads') if Path('wsr_downloads').exists() else export_dir.parent
            )
            
            print(f"{Colors.GREEN}✓ Detailed email sent successfully!{Colors.RESET}")
            return
            
        # Fallback to basic email if WSREmailNotifier not available
        print(f"{Colors.YELLOW}Using basic email notification (WSREmailNotifier not available)...{Colors.RESET}")
        
        # Get email config from environment
        sender_email = os.getenv('SENDER_EMAIL')
        sender_password = os.getenv('SENDER_PASSWORD')
        recipient_emails = os.getenv('RECIPIENT_EMAILS', '').split(',')
        
        print(f"Checking email configuration...")
        print(f"  SENDER_EMAIL: {'✓ Set' if sender_email else '✗ Not set'}")
        print(f"  SENDER_PASSWORD: {'✓ Set' if sender_password else '✗ Not set'}")
        print(f"  RECIPIENT_EMAILS: {'✓ Set' if recipient_emails and recipient_emails != [''] else '✗ Not set'}")
        
        if not sender_email or not sender_password:
            print(f"{Colors.YELLOW}⚠ Email credentials not configured - skipping email{Colors.RESET}")
            return
        
        if not recipient_emails or recipient_emails == ['']:
            print(f"{Colors.YELLOW}⚠ No recipient emails configured - skipping email{Colors.RESET}")
            return
        
        print(f"\n{Colors.BLUE}📧 Sending email notification...{Colors.RESET}")
        
        # Create message
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = ', '.join(recipient_emails)
        msg['Subject'] = f"WSR Processing Complete - {results['processed']} stores processed"
        
        # Email body
        body = f"""
WSR Processing Complete!

Summary:
- Processed: {results['processed']}/{results['total']} stores
- Failed: {results['failed']} stores
- Validation Failed: {results.get('validation_failed', 0)} stores

"""
        
        if export_dir and export_dir.exists():
            audit_files = list(export_dir.glob('*.xlsx'))
            if audit_files:
                body += f"\nAudit reports attached: {len(audit_files)} file(s)\n"
        
        body += "\n---\nAutomated WSR Processing System"
        
        msg.attach(MIMEText(body, 'plain'))
        
        # Attach audit files
        if export_dir and export_dir.exists():
            audit_files = list(export_dir.glob('*.xlsx'))
            for file_path in audit_files[:10]:  # Limit to 10 files to avoid email size limits
                try:
                    with open(file_path, 'rb') as f:
                        part = MIMEBase('application', 'octet-stream')
                        part.set_payload(f.read())
                    
                    encoders.encode_base64(part)
                    part.add_header('Content-Disposition', f'attachment; filename= {file_path.name}')
                    msg.attach(part)
                except Exception as e:
                    print(f"{Colors.YELLOW}  Warning: Could not attach {file_path.name}: {e}{Colors.RESET}")
            
            if len(audit_files) > 10:
                body_note = f"\n\nNote: Only first 10 of {len(audit_files)} audit files attached due to email size limits."
                msg.attach(MIMEText(body_note, 'plain'))
        
        # Send email
        smtp_server = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
        smtp_port = int(os.getenv('SMTP_PORT', '587'))
        
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.send_message(msg)
        
        print(f"{Colors.GREEN}✓ Email sent to: {', '.join(recipient_emails)}{Colors.RESET}")
        
    except Exception as e:
        print(f"{Colors.RED}✗ Failed to send email: {e}{Colors.RESET}")
        import traceback
        traceback.print_exc()


def main():
    parser = argparse.ArgumentParser(
        description='WSR v4 - DAILY aggregation only (configured for your current database)',
        epilog='Example: python process_wsr_v4_unified.py process "WSR Files"'
    )
    
    parser.add_argument('command', choices=['test', 'process'],
                       help='Command to execute')
    parser.add_argument('path', help='Directory with zips/Excel files')
    parser.add_argument('--keep-extracted', action='store_true',
                       help='Keep extracted files')
    parser.add_argument('--show-errors', action='store_true',
                       help='Show detailed error log at end')
    parser.add_argument('--export', action='store_true',
                       help='Export parsed data to Excel for auditing (works with test command)')
    parser.add_argument('--email', action='store_true',
                       help='Send email notification with audit reports attached')
    
    args = parser.parse_args()
    
    print(f"\n{Colors.BOLD}WSR v4 Processing Tool - SHIFT-LEVEL + DAILY AGGREGATION{Colors.RESET}")
    print("=" * 70)
    
    source_path = Path(args.path)
    if not source_path.exists():
        print(f"{Colors.RED}Error: {source_path} not found{Colors.RESET}")
        sys.exit(1)
    
    try:
        extracted_dir = None
        
        # Handle direct zip file input
        if source_path.is_file() and source_path.suffix.lower() == '.zip':
            print(f"{Colors.BLUE}Input is a zip file, extracting...{Colors.RESET}\n")
            # Create temp directory and extract this zip
            extract_path = Path(tempfile.mkdtemp(prefix="wsr_extract_"))
            
            try:
                with zipfile.ZipFile(source_path, 'r') as zip_ref:
                    file_list = zip_ref.namelist()
                    excel_files = [f for f in file_list if f.endswith(('.xls', '.xlsx'))]
                    
                    print(f"  Found {len(excel_files)} Excel files in zip")
                    print(f"  Extracting to: {extract_path}\n")
                    
                    for i, file_name in enumerate(excel_files, 1):
                        target_path = extract_path / Path(file_name).name
                        
                        if not target_path.exists():
                            zip_ref.extract(file_name, extract_path)
                            extracted_file = extract_path / file_name
                            # Move to root if in subdirectory
                            if extracted_file.parent != extract_path:
                                shutil.move(str(extracted_file), str(target_path))
                        
                        if i % 100 == 0:
                            print(f"  Extracted {i}/{len(excel_files)} files...")
                    
                    print(f"\n{Colors.GREEN}✓ Extracted {len(excel_files)} files{Colors.RESET}\n")
                    
                    # Clean up empty subdirectories
                    for item in extract_path.iterdir():
                        if item.is_dir():
                            try:
                                item.rmdir()
                            except:
                                pass
                
                extracted_dir = extract_path
                working_dir = extracted_dir
                
            except Exception as e:
                print(f"{Colors.RED}Error extracting zip: {e}{Colors.RESET}")
                if extract_path.exists():
                    shutil.rmtree(extract_path)
                sys.exit(1)
        
        # Handle directory input
        elif source_path.is_dir():
            if list(source_path.glob("*.zip")):
                extracted_dir = extract_zips(source_path)
                working_dir = extracted_dir
            else:
                working_dir = source_path
        
        # Handle single Excel file input
        elif source_path.is_file():
            working_dir = source_path.parent
        
        else:
            print(f"{Colors.RED}Error: Invalid path type{Colors.RESET}")
            sys.exit(1)
        
        dry_run = (args.command == 'test')
        processor = WSRProcessorV4(dry_run=dry_run)
        
        # Create audit exports directory if export is requested
        export_dir = None
        if args.export:
            export_dir = Path('audit_exports')
            export_dir.mkdir(exist_ok=True)
            print(f"{Colors.CYAN}Export mode enabled - Excel files will be saved to: {export_dir.absolute()}{Colors.RESET}\n")
        
        if args.command == 'test':
            print(f"{Colors.YELLOW}TEST MODE - No data will be uploaded{Colors.RESET}\n")
        else:
            print(f"{Colors.GREEN}PROCESS MODE - Uploading to Supabase{Colors.RESET}\n")
        
        # If we extracted files, process the directory, otherwise process single file
        if extracted_dir is not None:
            # We extracted from a zip, process the extracted directory
            results = processor.process_directory(working_dir, show_details=True, export_dir=export_dir)
        elif source_path.is_file() and source_path.suffix.lower() in ['.xls', '.xlsx']:
            # Single Excel file
            success = processor.process_file(source_path, show_details=True, export_dir=export_dir)
            results = {'total': 1, 'processed': 1 if success else 0, 'failed': 0 if success else 1, 'validation_failed': 0}
        else:
            # Directory with Excel files
            results = processor.process_directory(working_dir, show_details=True, export_dir=export_dir)
        
        print(f"\n{Colors.BOLD}Summary{Colors.RESET}")
        print("=" * 70)
        print(f"{Colors.GREEN}✓ Processed: {results['processed']}/{results['total']}{Colors.RESET}")
        if results.get('validation_failed', 0) > 0:
            print(f"{Colors.RED}✗ Validation Failed: {results['validation_failed']}/{results['total']}{Colors.RESET}")
        if results['failed'] > 0:
            print(f"{Colors.RED}✗ Upload Failed: {results['failed']}/{results['total']}{Colors.RESET}")
        
        # Display failed stores with details
        if processor.failed_stores:
            print(f"\n{Colors.RED}{'─'*70}")
            print(f"FAILED STORES DETAIL:")
            print(f"{'─'*70}{Colors.RESET}")
            for failure in processor.failed_stores:
                print(f"{Colors.RED}  • Store {failure['store']}: {failure['error']}{Colors.RESET}")
                print(f"{Colors.YELLOW}    File: {failure['file']}{Colors.RESET}")
        
        if not dry_run and processor.stats:
            print(f"\n{Colors.BLUE}Database Records Uploaded:{Colors.RESET}")
            print(f"\n{Colors.CYAN}Original Tables:{Colors.RESET}")
            for table in ['wsr_headers', 'wsr_sales', 'wsr_labor', 'wsr_financial', 'wsr_inventory', 'wsr_inventory_unit_costs']:
                key = f'{table}_records'
                if key in processor.stats:
                    print(f"  {table}: {processor.stats[key]:,}")
            
            print(f"\n{Colors.CYAN}Daily Aggregated Tables:{Colors.RESET}")
            for table in ['wsr_sales_daily', 'wsr_labor_daily', 'wsr_financial_daily', 'wsr_labor_cost_summary']:
                key = f'{table}_records'
                if key in processor.stats:
                    print(f"  {table}: {processor.stats[key]:,}")
            
            print(f"\n{Colors.CYAN}DMR Tables:{Colors.RESET}")
            for table in ['wsr_dmr', 'wsr_dmr_daily']:
                key = f'{table}_records'
                if key in processor.stats:
                    print(f"  {table}: {processor.stats[key]:,}")
        
        if extracted_dir and not args.keep_extracted:
            print(f"\n{Colors.BLUE}Cleaning up temporary files...{Colors.RESET}")
            shutil.rmtree(extracted_dir)
        elif extracted_dir:
            print(f"\n{Colors.CYAN}Extracted files kept at: {extracted_dir}{Colors.RESET}")
        
        # Send email notification if requested
        if args.email and not dry_run:
            print(f"\n{Colors.CYAN}📧 --email flag detected, attempting to send notification...{Colors.RESET}")
            # Store stats in function attribute so email function can access them
            send_email_notification.stats = processor.stats if hasattr(processor, 'stats') else {}
            send_email_notification(export_dir, results)
        elif args.email and dry_run:
            print(f"\n{Colors.YELLOW}⚠ --email flag set but dry_run mode is active{Colors.RESET}")
        else:
            print(f"\n{Colors.YELLOW}⚠ Email notification skipped (--email flag not set){Colors.RESET}")
        
        # Exit code logic:
        # - Exit 0 if no actual failures (parsing/upload errors)
        # - Validation failures are warnings, not errors (data was still uploaded)
        # - Only exit 1 if >50% of stores had validation issues (indicates systemic problem)
        
        actual_failures = results['failed']
        validation_issues = results.get('validation_failed', 0)
        total = results['total']
        
        if actual_failures > 0:
            # Real failures - parser crashed or couldn't upload
            print(f"\n{Colors.RED}❌ Pipeline failed: {actual_failures} stores could not be processed{Colors.RESET}")
            sys.exit(1)
        elif validation_issues > 0:
            validation_pct = (validation_issues / total * 100) if total > 0 else 0
            if validation_pct > 50:
                # More than half failed validation - systemic issue
                print(f"\n{Colors.RED}❌ Pipeline failed: {validation_pct:.1f}% validation failure rate{Colors.RESET}")
                sys.exit(1)
            else:
                # Some validation issues but acceptable
                print(f"\n{Colors.YELLOW}⚠️  Completed with warnings: {validation_issues} stores had validation issues{Colors.RESET}")
                print(f"{Colors.GREEN}✓ Pipeline succeeded - data uploaded for {results['processed']} stores{Colors.RESET}")
                sys.exit(0)
        else:
            # Perfect run
            print(f"\n{Colors.GREEN}✓ Pipeline succeeded - all {total} stores processed{Colors.RESET}")
            sys.exit(0)
        
    except KeyboardInterrupt:
        print(f"\n{Colors.YELLOW}Interrupted by user{Colors.RESET}")
        sys.exit(1)
    except Exception as e:
        print(f"\n{Colors.RED}Fatal Error: {e}{Colors.RESET}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
