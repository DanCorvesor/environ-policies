from pydantic_settings import BaseSettings


class DatabaseSettings(BaseSettings):
	"""Database configuration settings."""

	# Database connection settings
	db_host: str = 'localhost'
	db_port: int = 5432
	db_name: str = 'environ_policies'
	db_user: str = 'postgres'
	db_password: str = ''

	# Additional database settings
	db_schema: str = 'public'
	companies_table: str = 'companies'
	policies_table: str = 'policies'

	# Connection pool settings
	db_pool_size: int = 5
	db_max_overflow: int = 10

	class Config:
		env_file = '.env'
		env_file_encoding = 'utf-8'
		case_sensitive = False

	@property
	def database_url(self) -> str:
		"""Get the complete database URL."""
		return f'postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}'


# Global settings instance
settings = DatabaseSettings()
