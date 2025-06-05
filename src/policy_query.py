"""
Policy Query Service

Provides functionality to query policies based on customer jurisdiction
and calculate geography-based update statistics using SQLModel ORM.
"""

from datetime import datetime, timedelta

from sqlmodel import Session, and_, create_engine, func, select

from .models.models import Policy
from .settings import settings


class PolicyQueryService:
	"""Service for executing complex policy queries using SQLModel ORM."""

	def __init__(self):
		"""Initialize the service with database connection."""
		self.engine = create_engine(settings.database_url)

	def get_policies_with_avg_update_time(self, customer_jurisdiction: str) -> list:
		"""
		Get policies that match customer jurisdiction and were updated in last 90 days,
		with average update time for same geography over past year.
		"""
		# Date calculations
		ninety_days_ago = datetime.now() - timedelta(days=90)
		one_year_ago = datetime.now() - timedelta(days=365)

		with Session(self.engine) as session:
			# Subquery: Average days since last update for same geography (past year)
			avg_days_subquery = (
				select(
					Policy.geography,
					func.avg(
						func.extract('epoch', func.now() - Policy.updated_datetime) / 86400
					).label('avg_days_since_update'),
				)
				.where(
					and_(
						Policy.updated_datetime >= one_year_ago,
						Policy.updated_datetime.is_not(None),
					)
				)
				.group_by(Policy.geography)
				.subquery()
			)

			# Main query with join to get policies and their geography average
			query = (
				select(
					Policy.id,
					Policy.name,
					Policy.geography,
					Policy.updated_datetime,
					Policy.status,
					avg_days_subquery.c.avg_days_since_update,
				)
				.join(avg_days_subquery, Policy.geography == avg_days_subquery.c.geography)
				.where(
					and_(
						# Match customer's operating jurisdiction (using geography field)
						Policy.geography == customer_jurisdiction,
						# Only active policies
						Policy.status == 'active',
						# Updated in last 90 days
						Policy.updated_datetime >= ninety_days_ago,
						Policy.updated_datetime.is_not(None),
					)
				)
				.order_by(Policy.updated_datetime.desc())
			)

			return session.exec(query).all()
