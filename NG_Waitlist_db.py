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




from typing import List, Dict, Any, Optional
import os
from contextlib import contextmanager

import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

st.set_page_config(page_title="Waitlist Dashboard — Nigeria Launch", layout="wide")

# ----------------------
# Database connection
# ----------------------
def _build_database_url() -> Optional[str]:
    """Resolve the database URL from Streamlit secrets or environment variables."""
    load_dotenv()

    # Prefer Streamlit secrets if available
    secrets = getattr(st, "secrets", None)
    if secrets and "DATABASE_URL" in secrets:
        return secrets["DATABASE_URL"]

    # Fallback to DATABASE_URL env var
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        return db_url

    # Fallback to discrete connection params
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    if all([db_user, db_pass, db_host, db_port, db_name]):
        return f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"

    return None


@st.cache_resource(show_spinner=False)
def get_engine():
    """Create and cache the SQLAlchemy engine."""
    db_url = _build_database_url()
    if not db_url:
        raise RuntimeError("DATABASE_URL (or individual DB_* vars) not configured.")
    return create_engine(db_url, pool_pre_ping=True)


try:
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    st.success("Database connection successful")
except Exception as exc:
    st.error(f"Database connection failed: {exc}")
    st.stop()


@contextmanager
def db_connection():
    """Context manager that yields a live DB connection."""
    with engine.connect() as conn:
        yield conn


# ----------------------
# Query builder and fetcher
# ----------------------
@st.cache_data(ttl=300)
def fetch_waitlist_rows(start_date, end_date, include_deleted: bool, search_col: str, search_term: str, exclude_terms: List[str]) -> pd.DataFrame:
    # Base query
    sql = "SELECT id, email, name, created_at, updated_at, phone, deleted_at FROM support_waitlist WHERE 1=1"
    params: Dict[str, Any] = {}

    # deleted filter
    if not include_deleted:
        sql += " AND deleted_at IS NULL"

    # date filter
    if start_date is not None:
        sql += " AND created_at >= :start_date"
        params['start_date'] = start_date
    if end_date is not None:
        # include the full day by adding one day to end_date if it's a date
        sql += " AND created_at <= :end_date"
        params['end_date'] = end_date

    # search
    if search_term:
        # we'll use ILIKE for case-insensitive pattern matching on text columns
        pattern = f"%{search_term}%"
        if search_col == 'any':
            sql += " AND (CAST(id AS TEXT) ILIKE :pattern OR email ILIKE :pattern OR name ILIKE :pattern OR phone ILIKE :pattern)"
            params['pattern'] = pattern
        else:
            if search_col == 'id':
                # exact match attempt first
                params['id_term'] = search_term
                sql += " AND CAST(id AS TEXT) = :id_term"
            else:
                params['pattern'] = pattern
                sql += f" AND {search_col} ILIKE :pattern"

    # exclusions
    if exclude_terms:
        # construct conditions to exclude any row matching any exclude term across the columns
        exclude_clauses = []
        for i, term in enumerate(exclude_terms):
            key = f'ex_{i}'
            params[key] = term
            # try exact id match or pattern for text fields
            clause = f"(CAST(id AS TEXT) = :{key} OR email ILIKE :{key}_pat OR name ILIKE :{key}_pat OR phone ILIKE :{key}_pat)"
            params[f"{key}_pat"] = f"%{term}%"
            exclude_clauses.append(clause)
        sql += " AND NOT (" + " OR ".join(exclude_clauses) + ")"

    # final order
    sql += " ORDER BY created_at DESC"

    # execute
    with db_connection() as conn:
        df = pd.read_sql(text(sql), conn, params=params)
    return df

# ----------------------
# UI
# ----------------------
st.title("🇳🇬 Nigeria Launch — Waitlist Dashboard")

with st.sidebar:
    st.header("Filters")
    today = pd.Timestamp.now().normalize()

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start date", value=(today - pd.Timedelta(days=30)).date())
    with col2:
        end_date = st.date_input("End date", value=today.date())

    include_deleted = st.checkbox("Include deleted rows (deleted_at not null)", value=False)

    st.subheader("Search")
    search_col = st.selectbox("Search column (or any)", options=['any', 'id', 'email', 'name', 'phone'], index=0)
    search_term = st.text_input("Search term (partial allowed)")

    st.subheader("Exclude")
    exclude_raw = st.text_input("Exclude terms (comma-separated). Matches id (exact) or partial text in email/name/phone.")

    st.subheader("Count distinct by")
    distinct_by = st.selectbox("Distinct count column", options=['email', 'id'], index=0)

    show_table = st.checkbox("Show filtered table", value=True)
    download_csv = st.checkbox("Show CSV download button", value=True)

    st.markdown("---")
    st.caption("Database source: table `support_waitlist`. Make sure your DATABASE_URL points to the correct DB.")

# parse excludes
exclude_terms = [t.strip() for t in exclude_raw.split(',') if t.strip()] if exclude_raw else []

# Convert date inputs to timestamps (keep as strings/ISO for SQL params)
start_ts = pd.to_datetime(start_date).isoformat() if start_date else None
end_ts = pd.to_datetime(end_date).isoformat() if end_date else None

# Fetch rows
with st.spinner("Fetching data..."):
    try:
        df = fetch_waitlist_rows(start_ts, end_ts, include_deleted, search_col, search_term.strip(), exclude_terms)
    except Exception as e:
        st.error(f"Error querying database: {e}")
        st.stop()

# Distinct count
if distinct_by not in df.columns.tolist():
    st.warning(f"Distinct column `{distinct_by}` not found in returned data. Falling back to 'email'.")
    distinct_by = 'email'

distinct_count = df.drop_duplicates(subset=[distinct_by])[distinct_by].nunique()

col_a, col_b = st.columns([1, 3])
with col_a:
    st.metric(label=f"Distinct {distinct_by} count", value=int(distinct_count))
    st.caption(f"Rows fetched: {len(df)} (deleted rows {'included' if include_deleted else 'excluded'})")
with col_b:
    st.write("\n")

# show table
if show_table:
    st.subheader("Filtered rows")
    st.dataframe(df)

# CSV download
if download_csv:
    csv = df.to_csv(index=False)
    st.download_button("Download filtered CSV", data=csv, file_name="waitlist_filtered.csv", mime="text/csv")

# Quick usage tips
st.markdown("---")
st.subheader("Tips")
st.markdown(
    "- Use the Search box to find an id, email, name, or phone.\n- Put comma-separated values in Exclude to remove them from the count (e.g. `spam@example.com, 12345`).\n- Toggle 'Include deleted rows' to include rows with a non-null deleted_at.\n- The distinct count will use the column chosen in 'Distinct count by'."
)

# Footer
st.caption("Built for quick sharing. If you want, I can adapt this to a specific DB engine or add authentication.")
