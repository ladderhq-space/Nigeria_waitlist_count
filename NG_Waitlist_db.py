"""
Streamlit Waitlist Dashboard for Nigeria Ladder App Launch
File: streamlit_waitlist_dashboard.py

Features:
- Connects to your database using DATABASE_URL (use environment variable or Streamlit secrets)
- Filters by created_at date range
- Search across columns (id, email, name, phone) or "any"
- Exclude terms (comma-separated) across columns
- Option to include/exclude rows with deleted_at
- Distinct count (by email or id)
- Shows filtered table and allows CSV download

How to run:
1. Install dependencies:
   pip install streamlit pandas sqlalchemy psycopg2-binary
2. Set your DATABASE_URL environment variable, e.g.:
   export DATABASE_URL="postgresql://user:pass@host:5432/dbname"
   (On Windows PowerShell: $env:DATABASE_URL = "..." )
   Or add to .streamlit/secrets.toml as:
   [default]
   DATABASE_URL = "postgresql://..."
3. Run:
   streamlit run streamlit_waitlist_dashboard.py

Note: The app queries the table `support_waitlist` which is expected to have columns:
id, email, name, created_at, updated_at, phone, deleted_at

"""


import streamlit as st
import pandas as pd
import psycopg2
from psycopg2 import pool
import os
from datetime import datetime, date
from contextlib import contextmanager
from sqlalchemy import create_engine
from dotenv import load_dotenv
# Page config
st.set_page_config(
    page_title="Nigeria Ladder Waitlist Dashboard",
    page_icon="🚀",
    layout="wide"
)

load_dotenv()
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_waitlist_data():
    """Load waitlist data from database"""
    try:
        with engine.connect() as conn:
            query = """
                SELECT id, email, name, created_at, updated_at, phone, deleted_at
                FROM support_waitlist
                WHERE deleted_at IS NULL
                ORDER BY created_at DESC
            """
            df = pd.read_sql(query, conn)
            
            # Convert datetime columns
            if 'created_at' in df.columns:
                df['created_at'] = pd.to_datetime(df['created_at'])
            if 'updated_at' in df.columns:
                df['updated_at'] = pd.to_datetime(df['updated_at'])
            
            return df
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

def filter_dataframe(df, search_term, exclude_terms, date_range):
    """Apply filters to the dataframe"""
    filtered_df = df.copy()
    
    # Date filter
    if date_range and 'created_at' in filtered_df.columns:
        start_date, end_date = date_range
        filtered_df = filtered_df[
            (filtered_df['created_at'].dt.date >= start_date) &
            (filtered_df['created_at'].dt.date <= end_date)
        ]
    
    # Search filter
    if search_term:
        search_cols = ['id', 'email', 'name', 'phone']
        mask = filtered_df[search_cols].astype(str).apply(
            lambda x: x.str.contains(search_term, case=False, na=False)
        ).any(axis=1)
        filtered_df = filtered_df[mask]
    
    # Exclusion filter
    if exclude_terms:
        exclude_list = [term.strip() for term in exclude_terms.split(',')]
        for exclude_term in exclude_list:
            if exclude_term:
                search_cols = ['id', 'email', 'name', 'phone']
                mask = ~filtered_df[search_cols].astype(str).apply(
                    lambda x: x.str.contains(exclude_term, case=False, na=False)
                ).any(axis=1)
                filtered_df = filtered_df[mask]
    
    return filtered_df

# Main app
st.title("🚀 Nigeria Ladder Waitlist Dashboard")
st.markdown("---")

# Load data
with st.spinner("Loading waitlist data..."):
    df = load_waitlist_data()

if df.empty:
    st.warning("No data available or unable to connect to database.")
    st.stop()

# Sidebar filters
st.sidebar.header("Filters")

# Date filter
st.sidebar.subheader("📅 Date Range")
if 'created_at' in df.columns and not df['created_at'].isna().all():
    min_date = df['created_at'].min().date()
    max_date = df['created_at'].max().date()
    
    date_range = st.sidebar.date_input(
        "Select date range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
        key="date_range"
    )
    
    if isinstance(date_range, tuple) and len(date_range) == 2:
        date_filter = date_range
    else:
        date_filter = None
        st.sidebar.warning("Please select both start and end dates")
else:
    date_filter = None

# Search filter
st.sidebar.subheader("🔍 Search")
search_term = st.sidebar.text_input(
    "Search by ID, email, name, or phone",
    placeholder="Enter search term..."
)

# Exclusion filter
st.sidebar.subheader("🚫 Exclude")
exclude_terms = st.sidebar.text_area(
    "Exclude entries (comma-separated)",
    placeholder="email@example.com, 12345, John Doe",
    help="Enter terms separated by commas to exclude from the count"
)

# Refresh button
if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

# Apply filters
filtered_df = filter_dataframe(df, search_term, exclude_terms, date_filter)

# Metrics
col1 = st.columns(1)[0]

with col1:
    st.metric(
        "Total Waitlist Count",
        f"{len(df):,}",
        help="Total number of people on the waitlist"
    )


st.markdown("---")

# Display data
st.subheader("📊 Waitlist Data")

# Display options
col1, col2 = st.columns([3, 1])
with col1:
    st.caption(f"Showing {len(filtered_df)} of {len(df)} entries")
with col2:
    show_all = st.checkbox("Show all columns", value=False)

# Format the dataframe for display
display_df = filtered_df.copy()
if 'created_at' in display_df.columns:
    display_df['created_at'] = display_df['created_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
if 'updated_at' in display_df.columns:
    display_df['updated_at'] = display_df['updated_at'].dt.strftime('%Y-%m-%d %H:%M:%S')

if not show_all:
    display_columns = ['id', 'email', 'name', 'phone', 'created_at']
    display_df = display_df[[col for col in display_columns if col in display_df.columns]]

# Display table
st.dataframe(
    display_df,
    use_container_width=True,
    hide_index=True,
    column_config={
        "id": st.column_config.NumberColumn("ID", format="%d"),
        "email": st.column_config.TextColumn("Email", width="medium"),
        "name": st.column_config.TextColumn("Name", width="medium"),
        "phone": st.column_config.TextColumn("Phone", width="small"),
        "created_at": st.column_config.TextColumn("Created At", width="medium"),
    }
)

# Download button
st.download_button(
    label="📥 Download Filtered Data (CSV)",
    data=filtered_df.to_csv(index=False).encode('utf-8'),
    file_name=f"waitlist_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
    mime="text/csv"
)

# Footer
st.markdown("---")
st.caption("Dashboard updates every 5 minutes. Click 'Refresh Data' to update immediately.")



