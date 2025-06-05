-- Initialize the database

GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO public;

-- Create companies table
CREATE TABLE IF NOT EXISTS public.companies (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    operating_jurisdiction VARCHAR(255) NOT NULL,
    last_login TIMESTAMP WITH TIME ZONE,
    sector VARCHAR(255) NOT NULL
);

-- Create policies table with PostgreSQL array types
CREATE TABLE IF NOT EXISTS public.policies (
    id VARCHAR(255) PRIMARY KEY,
    name TEXT NOT NULL,
    published_date DATE,
    description TEXT,
    geography VARCHAR(255) NOT NULL,
    source_url TEXT,
    topics TEXT[],
    sectors TEXT[],
    status VARCHAR(50),
    updated_date TIMESTAMP WITH TIME ZONE
);

CREATE INDEX IF NOT EXISTS idx_companies_sector ON public.companies(sector);
CREATE INDEX IF NOT EXISTS idx_companies_jurisdiction ON public.companies(operating_jurisdiction);
CREATE INDEX IF NOT EXISTS idx_policies_geography ON public.policies(geography);
CREATE INDEX IF NOT EXISTS idx_policies_status ON public.policies(status);
CREATE INDEX IF NOT EXISTS idx_policies_published_date ON public.policies(published_date);

CREATE INDEX IF NOT EXISTS idx_policies_topics_gin ON public.policies USING GIN(topics);
CREATE INDEX IF NOT EXISTS idx_policies_sectors_gin ON public.policies USING GIN(sectors);
