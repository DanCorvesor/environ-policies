import ast

import pandas as pd
import pycountry_convert as pc


def get_continent(geography_str: str) -> str | None:
    """
    Map a geography string to its continent using pycountry-convert.

    Args:
        geography_str: Geography string from the data

    Returns:
        Continent name or None if not found
    """
    if pd.isna(geography_str):
        return None

    geography_str = str(geography_str).strip()

    # Handle common variations and abbreviations
    geography_mappings = {
        'USA': 'United States',
        'US': 'United States',
        'UK': 'United Kingdom',
        'UAE': 'United Arab Emirates',
    }

    # Apply common mappings
    if geography_str in geography_mappings:
        geography_str = geography_mappings[geography_str]

    try:
        # Convert country name to continent
        country_alpha2 = pc.country_name_to_country_alpha2(geography_str)
        country_continent_code = pc.country_alpha2_to_continent_code(country_alpha2)
        country_continent_name = pc.convert_continent_code_to_continent_name(country_continent_code)
        return country_continent_name
    except (KeyError, AttributeError, ValueError):
        pass

    return None


def parse_sector_list(sector_str):
    """
    Parse string representation of list back to actual list.

    Args:
        sector_str: String representation of a list (e.g., "['Water', 'Health']")

    Returns:
            List of sector strings
    """
    if pd.isna(sector_str):
        return []
    try:
        # Parse string representation of list
        sector_list = ast.literal_eval(str(sector_str))
        if isinstance(sector_list, list):
            return sector_list
        else:
            return [str(sector_list)]
    except (ValueError, SyntaxError):
        # If parsing fails, treat as single string
        return [str(sector_str)]
