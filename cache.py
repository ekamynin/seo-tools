import pandas as pd
import streamlit as st

from collaborator_api import fetch_all_sites, parse_site


@st.cache_data(ttl=21600, show_spinner=False, persist="disk")
def fetch_full_catalog(api_key: str) -> pd.DataFrame:
    """Full Collaborator catalog, no metric filters. Shared across all pages.
    TTL 6h, persisted to disk — survives server restarts within the TTL window.
    """
    items, _ = fetch_all_sites(api_key, dr_min=0, traffic_min=0, da_min=0)
    return pd.DataFrame([parse_site(i) for i in items])
