from typing import Dict, List

import pandas as pd
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

from .utils import get_continent, parse_sector_list


def get_top_relevant_policies_rag(company_name: str, limit: int = 3) -> List[Dict]:
    """
    Find the top most relevant policies for a company using sentence transformers for
    sector similarity and geographic filtering (same continent first, then global
    fallback).

    Args:
        company_name: Name of the company to find relevant policies for
        limit: Number of top policies to return (default: 3)

    Returns:
        List of dictionaries containing policy details and relevance scores
    """
    # Read the data files
    companies_df = pd.read_csv('src/data/cleaned_companies.csv')
    policies_df = pd.read_csv('src/data/cleaned_policies.csv')

    # Find the target company
    target_company = companies_df[
        companies_df['name'].str.contains(company_name, case=False, na=False)
    ]

    if target_company.empty:
        print(f"Company '{company_name}' not found")
        return []

    target = target_company.iloc[0]
    print(f'Found target company: {target["name"]}')
    print(f'Company sector: {target["sector"]}')
    print(f'Company jurisdiction: {target["operating_jurisdiction"]}')

    # Get company continent
    company_continent = get_continent(target['operating_jurisdiction'])
    print(f'Company continent: {company_continent}')

    # Process policy sectors and geography
    policies_df['parsed_sectors'] = policies_df['sectors'].apply(parse_sector_list)
    policies_df['sector_text'] = policies_df['parsed_sectors'].apply(
        lambda x: ' '.join(x) if x else ''
    )
    policies_df['continent'] = policies_df['geography'].apply(get_continent)

    # Filter out policies with no sectors
    policies_with_sectors = policies_df[policies_df['sector_text'].str.len() > 0].copy()

    if policies_with_sectors.empty:
        print('No policies found with sector information')
        return []

    print(f'Found {len(policies_with_sectors)} policies with sector information')

    # Initialize sentence transformer model for all policies
    model = SentenceTransformer('all-MiniLM-L6-v2')  # Lightweight but effective model

    # Prepare texts for encoding
    company_sector = target['sector']
    policy_sectors = policies_with_sectors['sector_text'].tolist()

    # Encode company sector and policy sectors
    print('Encoding sectors with sentence transformers...')
    company_embedding = model.encode([company_sector])
    policy_embeddings = model.encode(policy_sectors)

    # Calculate cosine similarity between company sector and all policy sectors
    similarities = cosine_similarity(company_embedding, policy_embeddings).flatten()

    # Add similarity scores to dataframe
    policies_to_rank = policies_with_sectors.copy()
    policies_to_rank['sector_similarity'] = similarities

    # Handle geographic prioritization with smart filling
    if company_continent:
        # Get continental policies
        continental_policies = policies_to_rank[policies_to_rank['continent'] == company_continent]
        non_continental_policies = policies_to_rank[
            policies_to_rank['continent'] != company_continent
        ]

        if len(continental_policies) >= limit:
            # Enough continental policies, use only those
            print(f'Found {len(continental_policies)} policies from {company_continent}')
            top_policies = continental_policies.sort_values(
                'sector_similarity', ascending=False
            ).head(limit)
            geographic_scope = f'Continental ({company_continent})'
        elif len(continental_policies) > 0:
            # Some continental policies, fill remaining with best non-continental
            continental_count = len(continental_policies)
            remaining_slots = limit - continental_count
            print(
                f'Found {continental_count} policies from {company_continent}, '
                f'filling {remaining_slots} slots with best global policies'
            )

            # Get top continental policies (all of them, sorted by sector similarity)
            top_continental = continental_policies.sort_values('sector_similarity', ascending=False)

            # Get top non-continental policies to fill remaining slots
            top_non_continental = non_continental_policies.sort_values(
                'sector_similarity', ascending=False
            ).head(remaining_slots)

            # Combine them - continental first, then non-continental
            top_policies = pd.concat([top_continental, top_non_continental])
            geographic_scope = f'Mixed ({continental_count} continental + {remaining_slots} global)'
        else:
            # No continental policies, use global search
            print(f'No policies found from {company_continent}, searching globally')
            top_policies = policies_to_rank.sort_values('sector_similarity', ascending=False).head(
                limit
            )
            geographic_scope = 'Global (no continental match)'
    else:
        # Company continent unknown, use global search
        print('Company continent unknown, searching globally')
        top_policies = policies_to_rank.sort_values('sector_similarity', ascending=False).head(
            limit
        )
        geographic_scope = 'Global (company continent unknown)'

    # Format results
    results = []
    for rank, (_, policy) in enumerate(top_policies.iterrows(), 1):
        result = {
            'rank': rank,
            'policy_id': policy['id'],
            'policy_name': policy['name'],
            'geography': policy['geography'],
            'continent': policy['continent'],
            'status': policy['status'],
            'published_date': policy['published_date'],
            'policy_sectors': policy['parsed_sectors'],
            'sector_similarity': policy['sector_similarity'],
            'source_url': policy['source_url'],
            'description': policy['description'][:200] + '...'
            if len(str(policy['description'])) > 200
            else policy['description'],
            'geographic_scope': geographic_scope,
        }

        # Add relevance reasons
        reasons = []

        # Geographic relevance
        if company_continent and policy['continent'] == company_continent:
            reasons.append(f'✅ Same continent ({company_continent})')
        elif company_continent and policy['continent']:
            reasons.append(f'🌍 Different continent ({policy["continent"]})')
        elif not company_continent:
            reasons.append('❓ Company continent unknown')
        else:
            reasons.append('❓ Policy continent unknown')

        # Sector similarity (secondary factor)
        if policy['sector_similarity'] > 0.8:
            reasons.append(f'🎯 High semantic similarity ({policy["sector_similarity"]:.3f})')
        elif policy['sector_similarity'] > 0.6:
            reasons.append(f'🎯 Medium semantic similarity ({policy["sector_similarity"]:.3f})')
        else:
            reasons.append(f'🎯 Low semantic similarity ({policy["sector_similarity"]:.3f})')

        # Show which specific sectors matched
        company_sector_lower = company_sector.lower()
        matching_sectors = [
            s
            for s in policy['parsed_sectors']
            if s.lower() in company_sector_lower or company_sector_lower in s.lower()
        ]
        if matching_sectors:
            reasons.append(f'Direct sector match: {", ".join(matching_sectors)}')

        result['relevance_reasons'] = reasons
        results.append(result)

    return results


def print_policy_ranking_report(company_name: str, limit: int = 3) -> None:
    """
    Print a formatted policy ranking report for a company using sentence transformers and
    geographic filtering.

    Args:
        company_name: Name of the company to analyze
        limit: Number of top policies to show
    """
    try:
        # Get companies data for target info
        companies_df = pd.read_csv('src/data/cleaned_companies.csv')

        # Find target company
        target_company = companies_df[
            companies_df['name'].str.contains(company_name, case=False, na=False)
        ]

        if target_company.empty:
            print(f"❌ Company '{company_name}' not found")
            return

        target = target_company.iloc[0]
        company_continent = get_continent(target['operating_jurisdiction'])

        print(f'🔍 Finding top {limit} policies most relevant to company...')

        # Get ranking results
        policies = get_top_relevant_policies_rag(company_name, limit)

        print('\n🏢 POLICY RANKING REPORT (Sentence Transformers + Geographic)')
        print('=' * 65)
        print(f'Target Company: {target["name"]}')
        print(f'Company Sector: {target["sector"]}')
        print(f'Company Jurisdiction: {target["operating_jurisdiction"]}')
        print(f'Company Continent: {company_continent or "Unknown"}')

        if policies:
            print(f'Search Scope: {policies[0]["geographic_scope"]}')

        print(f'\n📊 TOP {limit} MOST RELEVANT POLICIES')
        print('-' * 65)

        if not policies:
            print('No policies found with sector information.')
            return

        for policy in policies:
            print(f'\n#{policy["rank"]} {policy["policy_name"]}')
            print(f'   Similarity Score: {policy["sector_similarity"]:.3f}')
            print(
                f'   Geography: {policy["geography"]} ({policy["continent"] or "Unknown continent"})'  # noqa: E501
            )
            print(f'   Status: {policy["status"]}')
            print(f'   Published: {policy["published_date"]}')
            print(f'   Policy Sectors: {", ".join(policy["policy_sectors"])}')
            print(f'   Why Relevant: {", ".join(policy["relevance_reasons"])}')
            print(f'   Description: {policy["description"]}')
            if policy['source_url']:
                print(f'   URL: {policy["source_url"]}')

    except Exception as e:
        print(f'❌ Error generating policy ranking report: {e}')


def compare_policy_thresholds(company_name: str, limit: int = 5) -> None:
    """
    Compare policy rankings with different similarity thresholds.

    Args:
        company_name: Name of the company to analyze
        limit: Number of policies to show for each threshold
    """
    print(f"🔬 COMPARING SIMILARITY THRESHOLDS for '{company_name}'")
    print('=' * 60)

    thresholds = [0.3, 0.1, 0.05]

    for i, threshold in enumerate(thresholds, 1):
        print(f'\n{i}️⃣ SIMILARITY THRESHOLD: {threshold:.2f}')
        print_policy_ranking_report(company_name, limit)
        if i < len(thresholds):
            print('\n' + '─' * 40)
