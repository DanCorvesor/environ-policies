"""
Quick diagnostic script to check datetime values in the policies CSV
"""

from pathlib import Path

import pandas as pd


def check_dates():
	"""Check the datetime values in the policies CSV file."""

	policies_file = Path('src/data/policies.csv')

	if not policies_file.exists():
		print(f'❌ File not found: {policies_file}')
		return

	print('🔍 CHECKING DATETIME VALUES IN POLICIES CSV')
	print('=' * 50)

	# Read just the first 10 rows to check
	df = pd.read_csv(policies_file, nrows=10)

	print(f'📋 Columns in CSV: {list(df.columns)}')

	# Check if updated_date column exists
	date_columns = [col for col in df.columns if 'date' in col.lower() or 'time' in col.lower()]
	print(f'📅 Date-related columns: {date_columns}')

	for col in date_columns:
		print(f'\n🔍 Column: {col}')
		print(f'   Sample values: {df[col].head().tolist()}')
		print(f'   Data types: {df[col].dtype}')
		print(f'   Null values: {df[col].isna().sum()}/{len(df)}')

		# Try to convert to datetime and see what happens
		try:
			converted = pd.to_datetime(df[col], errors='coerce')
			print(f'   After conversion: {converted.head().tolist()}')
			print(f'   Failed conversions: {converted.isna().sum()}/{len(converted)}')
		except Exception as e:
			print(f'   Conversion error: {e}')


if __name__ == '__main__':
	check_dates()
