from datetime import datetime

from sqlalchemy import String
from sqlalchemy.dialects.postgresql import ARRAY
from sqlmodel import Column, Field, SQLModel


# SQLModel table definitions for database
class Company(SQLModel, table=True):
	__tablename__ = 'companies'

	id: int = Field(primary_key=True, nullable=False)
	name: str = Field(nullable=False)
	operating_jurisdiction: str = Field(nullable=False)
	last_login: datetime | None = Field(default=None, nullable=True)
	sector: str = Field(nullable=False)


class Policy(SQLModel, table=True):
	__tablename__ = 'policies'

	id: str = Field(primary_key=True, nullable=False)
	name: str = Field(nullable=False)
	published_date: datetime | None = Field(default=None, nullable=True)
	description: str | None = Field(default=None, nullable=True)
	geography: str = Field(nullable=False)
	source_url: str | None = Field(default=None, nullable=True)
	topics: list[str] | None = Field(default=None, sa_column=Column(ARRAY(String), nullable=True))
	sectors: list[str] | None = Field(default=None, sa_column=Column(ARRAY(String), nullable=True))
	status: str | None = Field(default=None, nullable=True)
	updated_datetime: datetime | None = Field(default=None, nullable=True)
