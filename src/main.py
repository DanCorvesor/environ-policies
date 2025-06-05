from .data_processor import DataProcessor
from .policy_query import PolicyQueryService
from .ranking import get_top_relevant_companies, print_ranking_report


def main():
	try:
		# Create a DataProcessor instance for companies data
		companies_processor = DataProcessor('src/data/companies.csv')

		# Read and process companies data
		companies_processor.read_csv()
		# Clean companies data
		companies_processor.clean_data()
		# Validate and create pydantic models
		companies_validation = companies_processor.validate_and_create_models()
		print('Validation Results:')
		print(f'   Valid rows: {companies_validation["valid_rows"]}')
		print(f'   Invalid rows: {companies_validation["invalid_rows"]}')
		print(f'   Pydantic models created: {len(companies_processor.pydantic_models)}')

		if companies_validation['validation_errors']:
			print(f'   Sample errors: {companies_validation["validation_errors"][:3]}')

		# Save cleaned companies data
		companies_processor.save_cleaned_data('src/data/cleaned_companies.csv')

		# Load to SQL
		print('🔄 Loading companies data to SQL database...')
		print(f'   Data shape before loading: {companies_processor.cleaned_data.shape}')
		companies_processor.load_to_sql(if_exists='replace')
		print('✅ Companies data loaded to SQL successfully!')

	except Exception as e:
		print(f'❌ Error processing companies data: {e}')

	print('\n' + '=' * 80)

	# Process Policies Data
	print('\n📋 PROCESSING POLICIES DATA')
	print('-' * 50)

	try:
		# Create a new DataProcessor instance for policies data
		policies_processor = DataProcessor('src/data/policies.csv')

		# Read and process policies data
		policies_data = policies_processor.read_csv()
		print(f'📥 Raw data shape: {policies_data.shape}')

		# Clean policies data
		cleaned_policies = policies_processor.clean_data()
		print(f'🧹 Cleaned data shape: {cleaned_policies.shape}')

		# Validate and create pydantic models
		policies_validation = policies_processor.validate_and_create_models()
		print('\n✅ Validation Results:')
		print(f'   Valid rows: {policies_validation["valid_rows"]}')
		print(f'   Invalid rows: {policies_validation["invalid_rows"]}')
		print(f'   Pydantic models created: {len(policies_processor.pydantic_models)}')

		# Save cleaned policies data
		policies_processor.save_cleaned_data('src/data/cleaned_policies.csv')

		# Load to SQL
		print('🔄 Loading policies data to SQL database...')
		print(f'   Data shape before loading: {policies_processor.cleaned_data.shape}')
		policies_processor.load_to_sql()
		print('✅ Policies data loaded to SQL successfully!')

	except Exception as e:
		print(f'❌ Error processing policies data: {e}')

	query_service = PolicyQueryService()

	try:
		print('🔍 Starting policy query test...')

		# Test with a couple of jurisdictions
		test_jurisdictions = ['South Africa', 'Romania']

		for jurisdiction in test_jurisdictions:
			print(f'\n🌍 {jurisdiction}:')

			results = query_service.get_policies_with_avg_update_time(jurisdiction)
			print(f'   Found {len(results)} active policies updated in last 60 days')

			if results:
				# Show first couple of results
				for i, result in enumerate(results[:2]):
					print(f'   • {result.name}')
					print(
						f'     Updated: {result.updated_datetime.strftime("%Y-%m-%d")} | '
						f'Avg geo days: {result.avg_days_since_update:.1f}'
					)

				if len(results) > 2:
					print(f'   ... and {len(results) - 2} more')

	except Exception as e:
		print(f'⚠️ Error testing policy queries: {e}')

	# Demonstrate company ranking functionality
	print('\n' + '=' * 80)
	print('\n🏆 COMPANY RANKING DEMONSTRATION')
	print('-' * 50)

	try:
		# Test ranking with a sample company name
		# You can change this to any company name that exists in your data
		test_company = 'Microsoft'  # Change this to a company that exists in your data

		print(f'🔍 Finding top 3 companies most relevant to "{test_company}"...')

		# Get and display the ranking report
		print_ranking_report(test_company, limit=3)

		# Also demonstrate the programmatic API
		print('\n📊 PROGRAMMATIC API EXAMPLE')
		print('-' * 30)
		relevant_companies = get_top_relevant_companies(test_company, limit=3)

		if relevant_companies:
			print(f'Found {len(relevant_companies)} relevant companies:')
			for company in relevant_companies:
				print(f'  • {company["company_name"]} (Score: {company["relevance_score"]:.1f})')
		else:
			print(f'No relevant companies found for "{test_company}"')
			print('Try with a different company name that exists in your database.')

	except Exception as e:
		print(f'⚠️ Error testing company ranking: {e}')


if __name__ == '__main__':
	main()
