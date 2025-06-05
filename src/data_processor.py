import ast
import logging
from pathlib import Path

import pandas as pd
from sqlmodel import Session, create_engine, delete

from .models.models import Company, Policy
from .settings import settings

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataProcessor:
	"""
	A stateful class for reading and cleaning data using pandas.

	"""

	def __init__(self, file_path: str | Path):
		"""

		Args:
			file_path: Path to the data file to process
		"""
		self.file_path = Path(file_path)
		self.raw_data: pd.DataFrame | None = None
		self.cleaned_data: pd.DataFrame | None = None
		self.data_type: str | None = None
		self.pydantic_models: list = []  # Store validated pydantic models

		# Validate file exists
		if not self.file_path.exists():
			raise FileNotFoundError(f'File not found: {self.file_path}')

		# Determine data type based on file path
		if 'companies' in str(self.file_path).lower():
			self.data_type = 'companies'
		elif 'policies' in str(self.file_path).lower():
			self.data_type = 'policies'
		else:
			self.data_type = 'unknown'

		logger.info(f'DataProcessor initialized for: {self.file_path}')
		logger.info(f'Data type detected: {self.data_type}')

	def read_csv(self, **kwargs) -> pd.DataFrame:
		"""
		Read the CSV file specified during initialization.

		Args:
			**kwargs: Additional arguments to pass to pd.read_csv()
		"""
		try:
			logger.info(f'Reading CSV file: {self.file_path}')
			self.raw_data = pd.read_csv(self.file_path, **kwargs)
			logger.info(
				f'Successfully loaded {len(self.raw_data)} rows and '
				f'{len(self.raw_data.columns)} columns'
			)
			return self.raw_data
		except Exception as e:
			logger.error(f'Error reading CSV file: {e}')
			raise

	def _clean_companies_data(self, data: pd.DataFrame) -> pd.DataFrame:
		"""
		Clean companies data according to the Company model.

		Args:
			data: Raw companies dataframe
		"""
		logger.info('Cleaning companies data...')
		cleaned = data.copy()

		# Map column names to match Company model
		column_mapping = {
			'company_id': 'id',
		}
		cleaned = cleaned.rename(columns=column_mapping)

		# Convert datetime columns
		if 'last_login' in cleaned.columns:
			cleaned['last_login'] = pd.to_datetime(cleaned['last_login'], errors='coerce')
			logger.info(
				f'Converted last_login to datetime. '
				f'Invalid dates: {cleaned["last_login"].isna().sum()}'
			)

		# Data validation and cleaning
		# Remove rows with missing critical fields
		critical_fields = ['id', 'name', 'operating_jurisdiction', 'sector']
		missing_critical = cleaned[critical_fields].isna().any(axis=1)
		if missing_critical.any():
			logger.warning(f'Removing {missing_critical.sum()} rows with missing critical fields')
			cleaned = cleaned[~missing_critical]

		# Clean text fields
		text_fields = ['name', 'operating_jurisdiction', 'sector']
		for field in text_fields:
			if field in cleaned.columns:
				cleaned[field] = cleaned[field].astype(str).str.strip()

		# Ensure ID is integer
		if 'id' in cleaned.columns:
			cleaned['id'] = pd.to_numeric(cleaned['id'], errors='coerce').astype('Int64')

		logger.info(f'Companies data cleaned. Final shape: {cleaned.shape}')
		return cleaned

	def _clean_policies_data(self, data: pd.DataFrame) -> pd.DataFrame:
		"""
		Clean policies data according to the Policy model.

		Args:
			data: Raw policies dataframe
		"""
		logger.info('Cleaning policies data...')
		cleaned = data.copy()

		cleaned = cleaned.rename(columns={'updated_date': 'updated_datetime'})

		# Convert date columns with different formats
		if 'published_date' in cleaned.columns:
			# Parse DD/MM/YY format
			cleaned['published_date'] = pd.to_datetime(
				cleaned['published_date'], errors='coerce', dayfirst=True
			)
			invalid_count = cleaned['published_date'].isna().sum()
			logger.info(
				f'Converted published_date (DD/MM/YY): '
				f'{len(cleaned) - invalid_count}/{len(cleaned)} successful'
			)

		if 'updated_date' in cleaned.columns:
			# Parse ISO 8601 timestamps (2025-03-03T10:59:53.464Z)
			cleaned['updated_date'] = pd.to_datetime(
				cleaned['updated_date'], errors='coerce', utc=True
			)
			invalid_count = cleaned['updated_date'].isna().sum()
			logger.info(
				f'Converted updated_date (ISO 8601): '
				f'{len(cleaned) - invalid_count}/{len(cleaned)} successful'
			)

		# Clean list fields (topics and sectors)
		list_fields = ['topics', 'sectors']
		for field in list_fields:
			if field in cleaned.columns:
				cleaned[field] = cleaned[field].apply(self._parse_list_field)
				logger.info(f'Parsed {field} list field')

		# Clean text fields
		text_fields = ['name', 'description', 'geography', 'source_url', 'status']
		for field in text_fields:
			if field in cleaned.columns:
				cleaned[field] = cleaned[field].astype(str).str.strip()

		# Clean HTML tags from description
		if 'description' in cleaned.columns:
			cleaned['description'] = cleaned['description'].str.replace(r'<[^<>]*>', '', regex=True)
			cleaned['description'] = cleaned['description'].str.replace(r'&nbsp;', ' ', regex=True)
			cleaned['description'] = cleaned['description'].str.strip()

		# Validate URLs
		if 'source_url' in cleaned.columns:
			url_pattern = r'^https?://'
			invalid_urls = ~cleaned['source_url'].str.contains(url_pattern, case=False, na=False)
			if invalid_urls.any():
				logger.warning(f'Found {invalid_urls.sum()} rows with invalid URLs')

		# Standardize status values
		if 'status' in cleaned.columns:
			status_mapping = {
				'active': 'active',
				'inactive': 'inactive',
				'draft': 'draft',
				'pending': 'pending',
			}
			cleaned['status'] = cleaned['status'].str.lower().map(status_mapping).fillna('unknown')

		# Remove rows with missing critical fields
		critical_fields = ['id', 'name', 'geography']
		missing_critical = cleaned[critical_fields].isna().any(axis=1)
		if missing_critical.any():
			logger.warning(f'Removing {missing_critical.sum()} rows with missing critical fields')
			cleaned = cleaned[~missing_critical]

		logger.info(f'Policies data cleaned. Final shape: {cleaned.shape}')
		return cleaned

	def _parse_list_field(self, value) -> list[str]:
		"""
		Parse list fields that might be stored as strings.

		Args:
			value: Value to parse (could be string representation of list or actual list)
		"""
		if pd.isna(value) or value == '':
			return []

		if isinstance(value, list):
			return [str(item).strip() for item in value if str(item).strip()]

		if isinstance(value, str):
			try:
				# Try to parse as literal list
				parsed = ast.literal_eval(value)
				if isinstance(parsed, list):
					return [str(item).strip() for item in parsed if str(item).strip()]
				else:
					return [str(parsed).strip()] if str(parsed).strip() else []
			except (ValueError, SyntaxError):
				# If that fails, treat as comma-separated string
				items = [item.strip() for item in value.split(',')]
				return [item for item in items if item]

		return [str(value).strip()] if str(value).strip() else []

	def clean_data(self) -> pd.DataFrame:
		"""
		Clean the loaded data based on the detected data type and corresponding models.
		"""
		if self.raw_data is None:
			raise ValueError('No data loaded. Please call read_csv() first.')

		logger.info('Starting data cleaning process...')

		if self.data_type == 'companies':
			self.cleaned_data = self._clean_companies_data(self.raw_data)
		elif self.data_type == 'policies':
			self.cleaned_data = self._clean_policies_data(self.raw_data)
		else:
			logger.warning(f'Unknown data type: {self.data_type}.')
			self.cleaned_data = None

		logger.info('Data cleaning completed')
		logger.info(
			f'Original shape: {self.raw_data.shape}, Cleaned shape: {self.cleaned_data.shape}'
		)
		return self.cleaned_data

	def validate_and_create_models(self) -> dict[str, any]:
		"""
		Validate cleaned data against the appropriate Pydantic model and store valid models.
		"""
		if self.cleaned_data is None:
			return {'error': 'No cleaned data available'}

		validation_results = {
			'total_rows': len(self.cleaned_data),
			'validation_errors': [],
			'valid_rows': 0,
			'invalid_rows': 0,
		}

		self.pydantic_models = []  # Reset models list

		try:
			if self.data_type == 'companies':
				for idx, row in self.cleaned_data.iterrows():
					try:
						model = Company(**row.to_dict())
						self.pydantic_models.append(model)
						validation_results['valid_rows'] += 1
					except Exception as e:
						validation_results['invalid_rows'] += 1
						validation_results['validation_errors'].append(f'Row {idx}: {str(e)}')

			elif self.data_type == 'policies':
				for idx, row in self.cleaned_data.iterrows():
					try:
						model = Policy(**row.to_dict())
						# print(row.to_dict())
						# print(model)
						self.pydantic_models.append(model)
						validation_results['valid_rows'] += 1
					except Exception as e:
						validation_results['invalid_rows'] += 1
						validation_results['validation_errors'].append(f'Row {idx}: {str(e)}')

		except ImportError:
			validation_results['validation_errors'].append('Could not import models for validation')

		logger.info(f'Created {len(self.pydantic_models)} valid pydantic models')
		return validation_results

	def load_to_sql(self) -> None:
		"""
		Load the cleaned data to SQL database using SQLModel objects and session.add.

		Args:
			if_exists: How to behave if data exists ('replace', 'append')
		"""
		if self.cleaned_data is None:
			raise ValueError('No cleaned data available. Please clean data first.')

		# Get table name and model class based on data type
		if self.data_type == 'companies':
			table_name = settings.companies_table
			model_class = Company
		elif self.data_type == 'policies':
			table_name = settings.policies_table
			model_class = Policy
		else:
			raise ValueError(f'Unknown data type: {self.data_type}')

		try:
			# Create database engine
			engine = create_engine(
				settings.database_url,
				pool_size=settings.db_pool_size,
				max_overflow=settings.db_max_overflow,
				echo=False,
			)

			logger.info(f'Loading data to SQL table: {settings.db_schema}.{table_name}')

			with Session(engine) as session:
				# Clear existing data if replace mode

				logger.info(f'Clearing existing data from {table_name}')
				statement = delete(model_class)
				session.exec(statement)
				session.commit()
				logger.info(f'Cleared existing data from {table_name}')

				# Create SQLModel objects and add to session
				created_objects = []
				for idx, row in self.cleaned_data.iterrows():
					try:
						# Convert row to dict and create SQLModel object
						row_dict = row.to_dict()

						# Handle None values for nullable fields
						for key, value in row_dict.items():
							# Handle different data types properly
							if isinstance(value, list):
								# For lists, keep as-is (they're already processed)
								continue
							elif pd.isna(value):
								# For scalar values, convert NaN to None
								row_dict[key] = None

						db_object = model_class(**row_dict)
						print(db_object.updated_datetime)
						session.add(db_object)
						created_objects.append(db_object)

					except Exception as e:
						logger.error(f'Error creating object for row {idx}: {e}')
						logger.error(f'Row data: {row_dict}')
						raise

				# Commit all objects to database
				logger.info(f'Committing {len(created_objects)} objects to database')
				session.commit()

				logger.info(f'Successfully loaded {len(created_objects)} rows to {table_name}')

				# Verify the data was loaded
				count_result = session.query(model_class).count()
				logger.info(f'Verification: {count_result} rows now in {table_name} table')

		except Exception as e:
			logger.error(f'Error loading data to SQL: {e}')
			raise

	def save_cleaned_data(
		self, output_path: str | Path, file_format: str = 'csv', **kwargs
	) -> None:
		"""
		Save the cleaned data to a file.

		Args:
			output_path: Path to save the cleaned data
			file_format: Format to save ('csv', 'excel', 'json')
			**kwargs: Additional arguments for the save method
		"""
		if self.cleaned_data is None:
			raise ValueError('No cleaned data available. Please clean data first.')

		output_path = Path(output_path)

		try:
			if file_format.lower() == 'csv':
				self.cleaned_data.to_csv(output_path, index=False, **kwargs)
			elif file_format.lower() == 'excel':
				self.cleaned_data.to_excel(output_path, index=False, **kwargs)
			elif file_format.lower() == 'json':
				self.cleaned_data.to_json(output_path, **kwargs)
			else:
				raise ValueError(f'Unsupported file format: {file_format}')

			logger.info(f'Cleaned data saved to: {output_path}')
		except Exception as e:
			logger.error(f'Error saving cleaned data: {e}')
			raise
