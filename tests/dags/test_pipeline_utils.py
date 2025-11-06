import pytest
import pandas as pd
from datetime import datetime
from plugins.pipeline_utils import transform_data

# Dummy timestamp for testing
TEST_TS = datetime(2025, 1, 1, 12, 0, 0)

@pytest.fixture
def sample_records():
    """ Provides a list of sample records with duplicates."""
    return [
        {
            "id": "1", "title": "Data Analyst", "description": "...",
            "created": "2024-05-10T12:00:00Z",
            "company": {"display_name": "DataCorp"},
            "category": {"label": "Analytics"},
            "salary_min": 50000, "salary_max": 90000,
            "redirect_url": "...", "contract_type": "full_time", "contract_time": "permanent",
            "location": {"area": ["USA", "California", "Pleasant Hill"]},
        },
        {
            "id": "2", "title": "Senior Analyst", "description": "...",
            "created": "2024-05-11T12:00:00Z",
            "company": {"display_name": "DataCorp"},  # Duplicate company
            "category": {"label": "Analytics"},   # Duplicate category
            "salary_min": 80000, "salary_max": 120000,
            "redirect_url": "...", "contract_type": "full_time", "contract_time": "permanent",
            "location": {"area": ["USA", "California", "Pleasant Hill"]}, # Duplicate location
        },
        {
            "id": "3", "title": "Data Scientist", "description": "...",
            "created": "2024-05-12T12:00:00Z",
            "company": {"display_name": "SciCo"},    # New company
            "category": {"label": "Data Science"}, # New category
            "salary_min": 100000, "salary_max": 150000,
            "redirect_url": "...", "contract_type": "part_time", "contract_time": "contract",
            "location": {"area": ["USA", "New York", "New York City"]}, # New location
        }
    ]

def test_transform_returns_five_dataframes(sample_records):
    dfs = transform_data(sample_records, TEST_TS)
    assert isinstance(dfs, dict)
    assert len(dfs) == 5
    assert set(dfs.keys()) == {"jobs", "companies", "locations", "categories", "jobstats"}

def test_jobs_table_has_foreign_keys(sample_records):
    dfs = transform_data(sample_records, TEST_TS)
    jobs_df = dfs["jobs"]
    
    # Check that all job rows are present
    assert len(jobs_df) == 3 
    
    # Check for the new foreign key columns
    expected_cols = {"job_id", "title", "company_name", "category_label", "city", "state"}
    assert expected_cols.issubset(set(jobs_df.columns))
    
    # Check values for the first record
    job_1 = jobs_df[jobs_df['job_id'] == '1'].iloc[0]
    assert job_1['company_name'] == 'DataCorp'
    assert job_1['category_label'] == 'Analytics'
    assert job_1['city'] == 'Pleasant Hill'
    assert job_1['state'] == 'California'

def test_dimension_tables_are_deduplicated(sample_records):
    dfs = transform_data(sample_records, TEST_TS)
    
    companies_df = dfs["companies"]
    locations_df = dfs["locations"]
    categories_df = dfs["categories"]
    
    # We had 3 records, but only 2 unique companies
    assert len(companies_df) == 2
    assert set(companies_df['company_name']) == {"DataCorp", "SciCo"}
    
    # 3 records, 2 unique locations
    assert len(locations_df) == 2
    
    # 3 records, 2 unique categories
    assert len(categories_df) == 2
    assert set(categories_df['category_label']) == {"Analytics", "Data Science"}

def test_jobstats_table_structure(sample_records):
    dfs = transform_data(sample_records, TEST_TS)
    jobstats_df = dfs["jobstats"]
    
    # One row per job
    assert len(jobstats_df) == 3
    
    expected_cols = {"job_id", "contract_type", "contract_time", "posting_week"}
    assert expected_cols.issubset(set(jobstats_df.columns))
    
    job_3_stats = jobstats_df[jobstats_df['job_id'] == '3'].iloc[0]
    assert job_3_stats['contract_type'] == 'part_time'
    assert job_3_stats['posting_week'] == '19' # 2024-05-12 is in week 19