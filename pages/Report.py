import streamlit as st
import pandas as pd
import datetime
import re
from io import BytesIO
import plotly.express as px

# --- PAGE CONFIGURATION ---
st.set_page_config(page_title="PIM Weekly Analysis Dashboard", page_icon=":material/analytics:", layout="wide")

# --- CONSTANTS & MAPPINGS ---
COUNTRY_MAP = {
    "KE": "Kenya", "UG": "Uganda", "NG": "Nigeria", "GH": "Ghana",
    "MA": "Morocco", "MO": "Morocco", "EG": "Egypt", "CI": "Ivory Coast",
    "SN": "Senegal", "ZA": "South Africa"
}

# --- HELPER FUNCTIONS ---
def parse_file_metadata(filename):
    """Extracts country, date, and week number from the filename."""
    prefix = filename[:2].upper()
    country = COUNTRY_MAP.get(prefix, "Unknown Country")
    
    date_obj = None
    week_num = None
    match = re.search(r'\d{4}-\d{2}-\d{2}', filename)
    if match:
        date_obj = datetime.datetime.strptime(match.group(), '%Y-%m-%d')
        week_num = date_obj.isocalendar()[1]
        
    return country, date_obj, week_num

def get_col(df, possible_names):
    """Safely find a column name ignoring exact case."""
    for name in possible_names:
        if name in df.columns:
            return name
    return None

def generate_excel_report(daily_summary, seller_stats, top_reasons, top_categories, metadata):
    """Creates a professional multi-sheet Excel file with a Cover Page."""
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        # 1. Cover Sheet
        cover_df = pd.DataFrame(list(metadata.items()), columns=['Metric', 'Value'])
        cover_df.to_excel(writer, sheet_name='Cover Sheet', index=False)
        
        # Format Cover Sheet
        worksheet = writer.sheets['Cover Sheet']
        worksheet.set_column('A:A', 25)
        worksheet.set_column('B:B', 35)

        # 2. Data Sheets
        daily_summary.to_excel(writer, sheet_name='Daily & Weekly Summary')
        seller_stats.to_excel(writer, sheet_name='Top Rejected Sellers')
        top_reasons.to_excel(writer, sheet_name='Top Rejection Reasons')
        top_categories.to_excel(writer, sheet_name='Top Rejected Categories')
        
    return output.getvalue()

def load_demo_data():
    """Generates dummy data for the Try Demo feature."""
    dates = pd.date_range(end=datetime.date.today(), periods=7).tolist() * 50
    statuses = ['Approved'] * 280 + ['Rejected'] * 70
    reasons = ['Duplicate product', 'Restricted brands', 'Missing COLOR', 'Missing Weight/Volume', 'Generic BRAND Issues'] * 14
    sellers = ['Tech Hub', 'Fashion Pro', 'Daily Deals', 'Gadget Kings', 'Home Goods'] * 70
    categories = ['Electronics', 'Clothing', 'Home', 'Beauty', 'Sports'] * 70
    
    df = pd.DataFrame({
        'Date': dates, 'Status': statuses, 'FLAG': reasons, 
        'SellerName': sellers, 'CATEGORY': categories
    })
    df['Day'] = df['Date'].dt.strftime('%A')
    df['Country'] = "Demo Country"
    return df, "Demo Country", datetime.date.today().isocalendar()[1]

# --- MAIN UI ---
st.title(":material/monitoring: PIM Weekly Export Analyzer")
st.markdown("Upload your `ProductSets` files (CSV or Excel) to generate a professional performance report.")

# Main Uploader Area
col_upload, col_demo = st.columns([4, 1])
with col_upload:
    uploaded_files = st.file_uploader("Select files to process", type=["csv", "xlsx", "xls"], accept_multiple_files=True, label_visibility="collapsed")
with col_demo:
    st.markdown("<br>", unsafe_allow_html=True)
    use_demo = st.button(":material/science: Load Sample Data", use_container_width=True)

# --- DATA PROCESSING ---
master_df = pd.DataFrame()
primary_country, primary_week = "Unknown", "N/A"

if use_demo:
    master_df, primary_country, primary_week = load_demo_data()
    st.success(f":material/check_circle: Sample Demo Data loaded successfully.")

elif uploaded_files:
    all_data = []
    primary_country, _, primary_week = parse_file_metadata(uploaded_files[0].name)
    
    with st.spinner(":material/hourglass_empty: Processing files..."):
        for file in uploaded_files:
            df = pd.read_csv(file, low_memory=False) if file.name.endswith('.csv') else pd.read_excel(file) 
            country, file_date, week_num = parse_file_metadata(file.name)
            
            if file_date:
                df['Date'] = file_date
                df['Day'] = file_date.strftime('%A')
                df['Country'] = country
                all_data.append(df)
        
        if all_data:
            master_df = pd.concat(all_data, ignore_index=True)
            st.success(f":material/check_circle: Data loaded successfully for **{primary_country}** (Week {primary_week})")
        else:
            st.error(":material/error: Could not find valid YYYY-MM-DD dates in the filenames.")

# --- DASHBOARD RENDERING ---
if not master_df.empty:
    status_col = get_col(master_df, ['Status', 'STATUS', 'status'])
    seller_col = get_col(master_df, ['SellerName', 'SELLER_NAME', 'seller_name', 'Seller'])
    flag_col = get_col(master_df, ['FLAG', 'Flag', 'flag', 'Reason'])
    cat_col = get_col(master_df, ['CATEGORY', 'Category', 'category'])
    
    if status_col:
        # Create Tabs
        tab_exec, tab_deepdive, tab_data = st.tabs([
            ":material/summarize: Executive Summary", 
            ":material/troubleshoot: Rejection Deep-Dive", 
            ":material/table_chart: Data Explorer"
        ])
        
        # Calculate Base Metrics
        daily_summary = master_df.groupby(['Day', status_col]).size().unstack(fill_value=0)
        days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        daily_summary = daily_summary.reindex(days_order).fillna(0).astype(int)
        daily_summary['Daily Total'] = daily_summary.sum(axis=1)
        
        weekly_approved = daily_summary.get('Approved', pd.Series(0)).sum()
        weekly_rejected = daily_summary.get('Rejected', pd.Series(0)).sum()
        weekly_total = daily_summary['Daily Total'].sum()
        rejection_rate = (weekly_rejected / weekly_total * 100) if weekly_total > 0 else 0
        
        daily_summary.loc['Weekly Total'] = daily_summary.sum()
        rejected_df = master_df[master_df[status_col] == 'Rejected']

        # === TAB 1: EXECUTIVE SUMMARY ===
        with tab_exec:
            # Top-Level Metrics
            c1, c2, c3 = st.columns(3)
            c1.metric("Total Processed", f"{int(weekly_total):,}")
            c2.metric("Total Approved", f"{int(weekly_approved):,}")
            c3.metric("Total Rejected", f"{int(weekly_rejected):,}", f"{rejection_rate:.1f}% Rate", delta_color="inverse")

            st.divider()

            col_chart, col_table = st.columns([3, 2])
            with col_chart:
                st.markdown("#### :material/show_chart: Daily Processing Trend")
                trend_data = master_df.groupby(['Date', status_col]).size().reset_index(name='Count')
                fig_trend = px.line(trend_data, x='Date', y='Count', color=status_col, markers=True, 
                                    color_discrete_map={"Approved": "#2e7d32", "Rejected": "#d32f2f"})
                fig_trend.update_layout(xaxis_title="", yaxis_title="Products Processed", margin=dict(l=0, r=0, t=30, b=0))
                st.plotly_chart(fig_trend, use_container_width=True)

            with col_table:
                st.markdown("#### :material/calendar_today: Daily Breakdown")
                st.dataframe(daily_summary, use_container_width=True)

        # === TAB 2: REJECTION DEEP-DIVE ===
        with tab_deepdive:
            if rejected_df.empty:
                st.info(":material/info: No rejected products found in this dataset. Great job!")
            else:
                c_pie, c_bar = st.columns(2)
                
                with c_pie:
                    st.markdown("#### :material/donut_large: Rejection Reasons Breakdown")
                    if flag_col:
                        reason_counts = rejected_df[flag_col].value_counts().reset_index()
                        reason_counts.columns = ['Reason', 'Count']
                        fig_pie = px.pie(reason_counts, values='Count', names='Reason', hole=0.4)
                        fig_pie.update_layout(margin=dict(l=0, r=0, t=30, b=0))
                        st.plotly_chart(fig_pie, use_container_width=True)
                    else:
                        st.warning("FLAG/Reason column not found.")

                with c_bar:
                    st.markdown("#### :material/storefront: Top 5 Rejected Sellers (with Rates)")
                    if seller_col:
                        # Calculate accurate rejection rates per seller
                        seller_stats = master_df.groupby(seller_col)[status_col].value_counts().unstack(fill_value=0)
                        seller_stats['Total Submitted'] = seller_stats.sum(axis=1)
                        seller_stats['Rejected'] = seller_stats.get('Rejected', 0)
                        seller_stats['Rejection Rate (%)'] = (seller_stats['Rejected'] / seller_stats['Total Submitted'] * 100).round(1)
                        
                        top_5_sellers = seller_stats.sort_values(by='Rejected', ascending=False).head(5).reset_index()
                        
                        fig_seller = px.bar(top_5_sellers, x='Rejected', y=seller_col, orientation='h', 
                                            text='Rejection Rate (%)', color_discrete_sequence=['#ef5350'])
                        fig_seller.update_layout(yaxis={'categoryorder':'total ascending'}, margin=dict(l=0, r=0, t=30, b=0), xaxis_title="Total Rejected Items")
                        fig_seller.update_traces(texttemplate='%{text}% Rate', textposition='outside')
                        st.plotly_chart(fig_seller, use_container_width=True)
                    else:
                        st.warning("Seller column not found.")

                st.divider()
                c_cat, c_rea = st.columns(2)
                with c_cat:
                    st.markdown("#### :material/category: Top 5 Rejected Categories")
                    if cat_col:
                        top_categories = rejected_df[cat_col].value_counts().head(5).reset_index()
                        top_categories.columns = ['Category', 'Rejected Count']
                        st.dataframe(top_categories, use_container_width=True, hide_index=True)
                    else:
                        st.warning("Category column not found.")

                with c_rea:
                    st.markdown("#### :material/report: Top 5 Rejection Reasons (Data)")
                    if flag_col:
                        top_reasons = rejected_df[flag_col].value_counts().head(5).reset_index()
                        top_reasons.columns = ['Reason', 'Rejected Count']
                        st.dataframe(top_reasons, use_container_width=True, hide_index=True)

        # === TAB 3: DATA EXPLORER ===
        with tab_data:
            st.markdown("#### :material/filter_alt: Filter & Search Data")
            filter_c1, filter_c2 = st.columns(2)
            
            with filter_c1:
                selected_status = st.multiselect("Filter by Status", master_df[status_col].dropna().unique())
            with filter_c2:
                if seller_col:
                    seller_search = st.text_input("Search for a specific Seller", placeholder="Type seller name...")
            
            # Apply filters
            filtered_df = master_df.copy()
            if selected_status:
                filtered_df = filtered_df[filtered_df[status_col].isin(selected_status)]
            if seller_col and seller_search:
                filtered_df = filtered_df[filtered_df[seller_col].astype(str).str.contains(seller_search, case=False, na=False)]
                
            st.dataframe(filtered_df, use_container_width=True)

        # === DOWNLOAD REPORT ===
        st.divider()
        st.markdown("#### :material/download: Export Formal Report")
        
        # Prepare metadata for cover sheet
        report_metadata = {
            "Report Generated On": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
            "Country / Region": primary_country,
            "Reporting Week": f"Week {primary_week}",
            "Total Products Processed": weekly_total,
            "Total Approved": weekly_approved,
            "Total Rejected": weekly_rejected,
            "Overall Rejection Rate": f"{rejection_rate:.1f}%"
        }
        
        # Ensure data objects exist for export
        safe_seller_stats = seller_stats if seller_col else pd.DataFrame(["N/A"])
        safe_top_reasons = reason_counts if flag_col else pd.DataFrame(["N/A"])
        safe_top_cats = top_categories if cat_col else pd.DataFrame(["N/A"])
        
        report_data = generate_excel_report(daily_summary, safe_seller_stats, safe_top_reasons, safe_top_cats, report_metadata)
        download_filename = f"{primary_country}_Week{primary_week}_ExecutiveReport.xlsx"
        
        st.download_button(
            label="Download Complete Executive Report (Excel)",
            data=report_data,
            file_name=download_filename,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            type="primary"
        )
    else:
        st.error(":material/error: Could not locate the 'Status' column in the uploaded files.")
