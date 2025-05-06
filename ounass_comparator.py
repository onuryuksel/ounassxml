import streamlit as st
import pandas as pd
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
import os
import re # For cleaning price strings

# --- Configuration ---
FEEDS = {
    "UAE English": "https://feed.atg.digital/prod/ounass-en_ae.xml",
    "KSA English": "https://feed.atg.digital/prod/ounass-en_sa.xml",
    "KWT English": "https://feed.atg.digital/prod/ounass-en_kw.xml",
    "OMN English": "https://feed.atg.digital/prod/ounass-en_om.xml",
    "BHN English": "https://feed.atg.digital/prod/ounass-en_bh.xml",
    "QTR English": "https://feed.atg.digital/prod/ounass-en_qa.xml",
    "UAE Arabic": "https://feed.atg.digital/prod/ounass-ar_ae.xml",
    "KSA Arabic": "https://feed.atg.digital/prod/ounass-ar_sa.xml",
    "KWT Arabic": "https://feed.atg.digital/prod/ounass-ar_kw.xml",
    "OMN Arabic": "https://feed.atg.digital/prod/ounass-ar_om.xml", # Assuming this should be Arabic
    "BHN Arabic": "https://feed.atg.digital/prod/ounass-ar_bh.xml", # Assuming this should be Arabic
    "QTR Arabic": "https://feed.atg.digital/prod/ounass-ar_qa.xml"  # Assuming this should be Arabic
}

SNAPSHOT_DIR = "feed_snapshots"
os.makedirs(SNAPSHOT_DIR, exist_ok=True)

# XML Namespace for Google Shopping Feed tags (e.g., g:id, g:price)
NAMESPACES = {'g': 'http://base.google.com/ns/1.0'}

# --- Helper Functions ---

def clean_price(price_str):
    """
    Cleans a price string (e.g., "49300 AED" or "AED 1,250.00") and converts it to a float.
    Returns None if the price cannot be parsed.
    """
    if not price_str: # Handles None or empty string
        return None
    
    # Regex to find a sequence of digits, dots, or commas (potential price)
    match = re.search(r'[\d\.,]+', price_str)
    if match:
        cleaned_price_str = match.group(0)
        # Remove commas used as thousand separators
        cleaned_price_str = cleaned_price_str.replace(',', '')
        try:
            return float(cleaned_price_str)
        except ValueError:
            # Failed to convert to float (e.g., if multiple dots like "1.2.3")
            return None
    return None # No numeric part found

def parse_xml_feed(xml_content):
    """Parses XML content and returns a list of product dictionaries."""
    products = []
    try:
        root = ET.fromstring(xml_content)
        
        # Determine the main container for items (RSS: channel, Atom: feed directly)
        # And the tag name for items (RSS: item, Atom: entry)
        item_elements = []
        channel = root.find('channel') # Common for RSS-based feeds
        if channel is not None:
            item_elements = channel.findall('item')
        else:
            # Check for Atom feed structure (<feed><entry>...</entry>)
            # Atom namespace URI is 'http://www.w3.org/2005/Atom'
            atom_ns_uri = 'http://www.w3.org/2005/Atom'
            if root.tag.startswith(f'{{{atom_ns_uri}}}'): # Check if root is <feed> in Atom ns
                # When parsing Atom, tags like <entry> and <title> are also in Atom namespace
                item_elements = root.findall(f'{{{atom_ns_uri}}}entry')
            elif root.tag == 'channel' or root.tag == 'feed': # Root itself is the channel/feed
                item_elements = root.findall('item') or root.findall(f'{{{atom_ns_uri}}}entry')
            else:
                # Fallback: items might be directly under the root (less common for full feeds)
                item_elements = root.findall('item')

        for item_el in item_elements:
            # Extract data based on the provided sample and common Google Shopping tags
            # Use NAMESPACES for g:prefixed tags, None for non-prefixed tags.
            
            # For Atom feeds, some standard tags like 'title', 'link' are namespaced.
            # We'll try generic first, then Atom-specific if needed.
            is_atom_entry = item_el.tag.startswith(f'{{{atom_ns_uri}}}') if atom_ns_uri else False
            atom_item_ns = { 'atom': atom_ns_uri } if is_atom_entry else None

            product_id = item_el.findtext('g:id', namespaces=NAMESPACES)
            
            title = item_el.findtext('title') # Sample has <title> without 'g:'
            if title is None and is_atom_entry: # Atom <title> is namespaced
                 title = item_el.findtext('atom:title', namespaces=atom_item_ns)

            link = item_el.findtext('link')   # Sample has <link> without 'g:'
            if link is None and is_atom_entry: # Atom <link> can be more complex (rel="alternate")
                link_element = item_el.find("atom:link[@rel='alternate']", namespaces=atom_item_ns)
                if link_element is not None:
                    link = link_element.get('href')
            
            image_link = item_el.findtext('g:image_link', namespaces=NAMESPACES)
            brand = item_el.findtext('g:brand', namespaces=NAMESPACES)
            
            # Using g:product_type for category as it's standard and present in sample
            category = item_el.findtext('g:product_type', namespaces=NAMESPACES)
            if not category: # Fallback to custom labels if g:product_type is missing
                category = item_el.findtext('g:custom_label_0', namespaces=NAMESPACES)

            price_str = item_el.findtext('g:price', namespaces=NAMESPACES)
            sale_price_str = item_el.findtext('g:sale_price', namespaces=NAMESPACES) # Sample has this empty
            
            price_numeric = clean_price(price_str)
            sale_price_numeric = clean_price(sale_price_str)

            if product_id: # A product ID is essential
                products.append({
                    'product_id': product_id,
                    'title': title,
                    'brand': brand,
                    'category': category,
                    'price': price_numeric,
                    'sale_price': sale_price_numeric, # Will be None if not on sale or empty tag
                    'link': link,
                    'image_link': image_link
                })
    except ET.ParseError as e:
        st.error(f"XML parsing error for the feed: {e}")
        return []
    except Exception as e:
        st.error(f"An unexpected error occurred while processing XML: {e}")
        return []
    return products

@st.cache_data(ttl=3600*4, show_spinner=False) # Cache data for 4 hours
def load_or_fetch_feed_data(feed_key, feed_url):
    """Loads daily snapshot or fetches and parses the feed if snapshot doesn't exist or is outdated."""
    today_str = datetime.now().strftime("%Y-%m-%d")
    # Sanitize feed_key for use in filenames
    safe_feed_key = "".join(c if c.isalnum() else "_" for c in feed_key)
    snapshot_file = os.path.join(SNAPSHOT_DIR, f"{safe_feed_key}_{today_str}.parquet")

    if os.path.exists(snapshot_file):
        try:
            # st.info(f"Loading today's snapshot for {feed_key} from {snapshot_file}...")
            return pd.read_parquet(snapshot_file)
        except Exception as e:
            st.warning(f"Could not load snapshot {snapshot_file}: {e}. Refetching data.")

    # st.info(f"Fetching data for {feed_key} from {feed_url}...")
    try:
        response = requests.get(feed_url, timeout=60) # 60-second timeout
        response.raise_for_status() # Raise an exception for HTTP errors (4xx or 5xx)
        
        # Try decoding with UTF-8, fallback to requests' auto-detected encoding
        try:
            xml_content = response.content.decode('utf-8')
        except UnicodeDecodeError:
            xml_content = response.text 

        products_list = parse_xml_feed(xml_content)
        if not products_list:
            # parse_xml_feed will show an error if parsing fails.
            # If list is empty due to no items, show a warning.
            if not any(st.session_state.get(key, {}).get('type') == 'error' for key in st.session_state): # Avoid double message
                 st.warning(f"No products found or parsed for {feed_key}. The feed might be empty or structured differently than expected.")
            return pd.DataFrame() # Return empty DataFrame

        df = pd.DataFrame(products_list)
        df.to_parquet(snapshot_file, index=False)
        return df
    except requests.exceptions.RequestException as e:
        st.error(f"Could not download feed {feed_key} from {feed_url}: {e}")
        return pd.DataFrame() # Return empty DataFrame on error
    except Exception as e:
        st.error(f"A general error occurred while processing {feed_key} ({feed_url}): {e}")
        return pd.DataFrame()

def get_product_details(df, product_ids_list):
    """Retrieves product details from a DataFrame based on a list of product IDs."""
    if df.empty or not product_ids_list:
        return pd.DataFrame()
    return df[df['product_id'].isin(product_ids_list)]

def to_excel(df_dict):
    """Exports a dictionary of DataFrames to an Excel file in memory."""
    from io import BytesIO
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        for sheet_name, df_data in df_dict.items():
            if not df_data.empty:
                df_data.to_excel(writer, sheet_name=sheet_name, index=False)
    processed_data = output.getvalue()
    return processed_data

# --- Streamlit UI ---
st.set_page_config(layout="wide", page_title="Ounass Assortment Comparator")
st.title("Ounass Assortment Comparison Tool")

st.sidebar.header("Feed Selection")
feed_options = list(FEEDS.keys())

col1_sidebar, col2_sidebar = st.sidebar.columns(2)
selected_feed_A_key = col1_sidebar.selectbox("Feed A (Base)", feed_options, index=0, key="feed_a_select")
selected_feed_B_key = col2_sidebar.selectbox("Feed B (Compare)", feed_options, index=1 if len(feed_options)>1 else 0, key="feed_b_select")

if selected_feed_A_key == selected_feed_B_key:
    st.sidebar.warning("Please select two different feeds for comparison.")
    st.stop()

# Data Loading
with st.spinner(f"Loading data for {selected_feed_A_key}..."):
    df_A = load_or_fetch_feed_data(selected_feed_A_key, FEEDS[selected_feed_A_key])
with st.spinner(f"Loading data for {selected_feed_B_key}..."):
    df_B = load_or_fetch_feed_data(selected_feed_B_key, FEEDS[selected_feed_B_key])

if df_A.empty and df_B.empty:
    st.error("Failed to load data for both selected feeds. Please check feed URLs or try again later.")
    st.stop()
elif df_A.empty:
    st.error(f"Failed to load data for {selected_feed_A_key}. Comparison cannot proceed with this feed.")
    # Optionally, allow viewing df_B if it loaded
    if not df_B.empty:
        st.info(f"Data for {selected_feed_B_key} ({len(df_B)} products) loaded successfully.")
    st.stop()
elif df_B.empty:
    st.error(f"Failed to load data for {selected_feed_B_key}. Comparison cannot proceed with this feed.")
    if not df_A.empty:
        st.info(f"Data for {selected_feed_A_key} ({len(df_A)} products) loaded successfully.")
    st.stop()


st.sidebar.info(f"{selected_feed_A_key}: {len(df_A)} products")
st.sidebar.info(f"{selected_feed_B_key}: {len(df_B)} products")

# --- Filtering ---
st.sidebar.header("Filters")

# Brand Filter
all_brands_A = sorted(df_A['brand'].dropna().unique().tolist()) if 'brand' in df_A.columns and not df_A.empty else []
all_brands_B = sorted(df_B['brand'].dropna().unique().tolist()) if 'brand' in df_B.columns and not df_B.empty else []
combined_brands = sorted(list(set(all_brands_A + all_brands_B)))

selected_brands = st.sidebar.multiselect("Filter by Brand", options=combined_brands, key="brand_filter")

# Category Filter
all_categories_A = sorted(df_A['category'].dropna().unique().tolist()) if 'category' in df_A.columns and not df_A.empty else []
all_categories_B = sorted(df_B['category'].dropna().unique().tolist()) if 'category' in df_B.columns and not df_B.empty else []
combined_categories = sorted(list(set(all_categories_A + all_categories_B)))

selected_categories = st.sidebar.multiselect("Filter by Category", options=combined_categories, key="category_filter")

# Apply filters
df_A_filtered = df_A.copy() if not df_A.empty else pd.DataFrame()
df_B_filtered = df_B.copy() if not df_B.empty else pd.DataFrame()

if selected_brands:
    if 'brand' in df_A_filtered.columns:
        df_A_filtered = df_A_filtered[df_A_filtered['brand'].isin(selected_brands)]
    if 'brand' in df_B_filtered.columns:
        df_B_filtered = df_B_filtered[df_B_filtered['brand'].isin(selected_brands)]

if selected_categories:
    if 'category' in df_A_filtered.columns:
        df_A_filtered = df_A_filtered[df_A_filtered['category'].isin(selected_categories)]
    if 'category' in df_B_filtered.columns:
        df_B_filtered = df_B_filtered[df_B_filtered['category'].isin(selected_categories)]

# --- Comparison ---
ids_A = set(df_A_filtered['product_id']) if not df_A_filtered.empty and 'product_id' in df_A_filtered.columns else set()
ids_B = set(df_B_filtered['product_id']) if not df_B_filtered.empty and 'product_id' in df_B_filtered.columns else set()

products_only_in_A_ids = list(ids_A - ids_B)
products_only_in_B_ids = list(ids_B - ids_A)
products_in_both_ids = list(ids_A.intersection(ids_B))

df_only_in_A = get_product_details(df_A_filtered, products_only_in_A_ids)
df_only_in_B = get_product_details(df_B_filtered, products_only_in_B_ids)

st.subheader(f"Comparison Results: {selected_feed_A_key} vs {selected_feed_B_key}")
filter_info = []
if selected_brands: filter_info.append(f"Brand(s): {', '.join(selected_brands)}")
if selected_categories: filter_info.append(f"Category(s): {', '.join(selected_categories)}")
if filter_info:
    st.markdown(f"**Active Filters:** {', '.join(filter_info)}")
else:
    st.markdown("**Active Filters:** None")


tab1_title = f"Only in {selected_feed_A_key} ({len(df_only_in_A)})"
tab2_title = f"Only in {selected_feed_B_key} ({len(df_only_in_B)})"
tab3_title = f"In Both Feeds ({len(products_in_both_ids)})"

tab1, tab2, tab3 = st.tabs([tab1_title, tab2_title, tab3_title])

with tab1:
    st.dataframe(df_only_in_A)
    if not df_only_in_A.empty:
        excel_data_A = to_excel({f"Only_in_{selected_feed_A_key.replace(' ', '_')}": df_only_in_A})
        st.download_button(
            label=f"Download List (Excel)",
            data=excel_data_A,
            file_name=f"only_in_{selected_feed_A_key.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="download_A"
        )

with tab2:
    st.dataframe(df_only_in_B)
    if not df_only_in_B.empty:
        excel_data_B = to_excel({f"Only_in_{selected_feed_B_key.replace(' ', '_')}": df_only_in_B})
        st.download_button(
            label=f"Download List (Excel)",
            data=excel_data_B,
            file_name=f"only_in_{selected_feed_B_key.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="download_B"
        )

with tab3:
    st.write(f"Total {len(products_in_both_ids)} products are present in both feeds (after filters).")
    # Optionally, display a sample or all common products
    # if products_in_both_ids:
    #     df_in_both = get_product_details(df_A_filtered, products_in_both_ids)
    #     st.dataframe(df_in_both.head(20)) # Show first 20 common products

st.sidebar.markdown("---")
st.sidebar.caption("Ounass Feed Comparator v1.0")

# To run this app:
# 1. Save this code as a .py file (e.g., ounass_comparator.py).
# 2. Ensure you have the necessary libraries: pip install streamlit pandas requests xlsxwriter pyarrow
# 3. Open your terminal, navigate to the file's directory, and run: streamlit run ounass_comparator.py