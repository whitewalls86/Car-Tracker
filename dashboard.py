import sqlite3
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
from datetime import date, timedelta
from config import DB_PATH

st.set_page_config(layout="wide")
st.title("🚘 Car Market Summary Dashboard")

@st.cache_data(ttl=60)
def load_cleaned():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM cleaned_listings", conn, parse_dates=['first_seen', 'last_seen'])
    conn.close()
    return df

df = load_cleaned()

conn = sqlite3.connect(DB_PATH)
price_history = pd.read_sql_query("SELECT * FROM price_history", conn, parse_dates=['date'])
conn.close()

latest_prices = price_history.sort_values("date").drop_duplicates("vin", keep="last")
prev_prices = price_history.groupby("vin").nth(-2).reset_index()

price_drops = latest_prices.merge(prev_prices, on="vin", suffixes=("_latest", "_prev"))
price_drops = price_drops[price_drops['price_latest'] < price_drops['price_prev']]
price_drops = price_drops.merge(
    df[['vin', 'year', 'make', 'model', 'trim', 'price', 'discount', 'discount_rate', 'dealer', 'location',
        'implied_msrp', 'msrp']],
    on="vin", how="left"
)

today = pd.to_datetime(date.today())
yesterday = today - timedelta(days=1)

models = sorted(df['model'].dropna().unique())
selected_models = st.multiselect("Select Models", models, default=models[:4])
scopes = st.radio("Select Scope", ["all", "local", "national"], horizontal=True)
available_years = sorted(df['year'].dropna().unique())
selected_years = st.multiselect("Select Year(s)", available_years, default=available_years)

if selected_models:
    df = df[df['model'].isin(selected_models)]
if scopes != "all":
    df = df[df['search_scope'] == scopes]
if selected_years:
    df = df[df['year'].isin(selected_years)]


st.header("🚨 Well-Priced New or Recently Discounted Vehicles")
reference_discounts = df.groupby(['year', 'model', 'trim'])['discount'].mean().reset_index().rename(columns={'discount': 'avg_discount'})

alerts_base = df[df['first_seen'].dt.date == today.date()].copy()
alerts_base = alerts_base.merge(reference_discounts, on=['year', 'model', 'trim'], how='left')
alerts_base['alert_type'] = "new listing"
alerts_new = alerts_base[alerts_base['discount'] > 1.1 * alerts_base['avg_discount']]

price_drops = price_drops.merge(reference_discounts, on=['year', 'model', 'trim'], how='left')
price_drops['alert_type'] = "price drop"
alerts_dropped = price_drops[price_drops['discount'] > 1.1 * price_drops['avg_discount']]

alerts_combined = pd.concat([alerts_new, alerts_dropped], ignore_index=True).drop_duplicates('vin')
alerts_combined['has_true_values'] = alerts_combined.apply(
    lambda row: pd.notnull(row['price']) and pd.notnull(row['msrp']),
    axis=1
)

filter_true_values = st.checkbox("Show only listings with actual MSRP and price", value=False)
if filter_true_values:
    alerts_combined = alerts_combined[alerts_combined['has_true_values']]

alerts_display = alerts_combined[['vin', 'year', 'make', 'model', 'trim', 'price', 'implied_msrp', 'discount', 'discount_rate', 'avg_discount', 'dealer', 'location', 'alert_type', 'has_true_values']].copy()

styled_alerts = alerts_display.style.format({
    'price': '${:,.0f}',
    'implied_msrp': '${:,.0f}',
    'discount': '${:,.0f}',
    'avg_discount': '${:,.0f}',
    'discount_rate': '{:.0%}'
})

st.dataframe(styled_alerts)

st.header("📤 Vehicles Sold Yesterday")
vin_status = df.groupby(['vin'])['last_seen'].max().reset_index()
vin_status['days_since_seen'] = (today - vin_status['last_seen']).dt.days
sold_yesterday = vin_status[vin_status['last_seen'].dt.date == yesterday.date()]
sold_info = sold_yesterday.merge(df[['vin', 'model', 'trim', 'price', 'discount']], on='vin', how='left')
sold_summary = sold_info.groupby(['model', 'trim']).agg(
    vehicles_sold=('vin', 'count'),
    avg_discount=('discount', 'mean'),
    avg_price=('price', 'mean')
).reset_index()

styled_sold = sold_summary.style.format({
    'avg_discount': '${:,.0f}',
    'avg_price': '${:,.0f}'
})

st.dataframe(styled_sold)

st.header("📥 Vehicles Added Today")
added_today = df[df['first_seen'].dt.date == today.date()]
added_summary = added_today.groupby(['model', 'trim']).agg(
    vehicles_added=('vin', 'count'),
    avg_discount=('discount', 'mean'),
    avg_price=('price', 'mean')
).reset_index()

styled_added = added_summary.style.format({
    'avg_discount': '${:,.0f}',
    'avg_price': '${:,.0f}'
})

st.dataframe(styled_added)

st.header("💰 Discount Summary for 2025 Vehicles")
df_2025 = df[df['year'] == '2025']
summary_2025 = df_2025.groupby(['model', 'trim']).agg(
    avg_price=('price', 'mean'),
    avg_discount=('discount', 'mean'),
    avg_implied_days=('implied_days_on_market', 'mean'),
    avg_actual_days=('days_on_market', 'mean')
).reset_index()

styled_2025 = summary_2025.style.format({
    'avg_discount': '${:,.0f}',
    'avg_price': '${:,.0f}'
})

st.dataframe(styled_2025)

st.header("📊 Summary by Model / Trim")
grouped = df.groupby(['year', 'make', 'model', 'trim'])
summary = grouped.agg({
    'vin': 'nunique',
    'calculated_days_on_market': 'mean',
    'price': 'mean',
    'discount': 'mean',
    'discount_rate': 'mean'
}).reset_index().rename(columns={
    'vin': 'vehicles_seen',
    'calculated_days_on_market': 'avg_days_on_lot',
    'price': 'avg_price',
    'discount': 'avg_discount',
    'discount_rate': 'avg_discount_rate'
})

styled_summary = summary.style.format({
    'avg_discount': '${:,.0f}',
    'avg_price': '${:,.0f}',
    'avg_discount_rate': '{:.0%}'
})

st.dataframe(styled_summary)


st.header("📉 Removal Ratio")
active_cutoff = pd.to_datetime(df['last_seen'].max())
vin_seen = df.groupby(['vin'])['last_seen'].max().reset_index()
vin_seen['days_since_seen'] = (active_cutoff - vin_seen['last_seen']).dt.days
vin_seen['status'] = vin_seen['days_since_seen'].apply(lambda x: 'removed' if x > 1 else 'active')
vin_seen = vin_seen.merge(df[['vin', 'model', 'trim', 'year', 'first_seen']].drop_duplicates(), on='vin', how='left')

# Count listings added today
vin_seen['added_today'] = vin_seen['first_seen'].dt.date == today.date()

# Group and calculate ratios
removal_ratio = vin_seen.groupby(['year', 'model', 'trim', 'status'])['vin'].count().unstack(fill_value=0)
removal_ratio['added_today'] = vin_seen[vin_seen['added_today']].groupby(['year', 'model', 'trim'])['vin'].count()
removal_ratio['added_today'] = removal_ratio['added_today'].fillna(0).astype(int)
removal_ratio['removed_ratio'] = removal_ratio['removed'] / removal_ratio['active'].replace(0, 1)
removal_ratio['net_ratio'] = removal_ratio['added_today'] / removal_ratio['removed'].replace(0, 1)

styled_removal = removal_ratio.reset_index().style.format({
    'removed_ratio': '{:.0%}',
    'net_ratio': '{:.2f}'
})

st.dataframe(styled_removal)

st.header("📈 Price Trend Over Time")
trend_data = price_history.merge(df[['vin', 'model', 'trim']], on='vin', how='inner')
trend_data = trend_data.groupby(['date', 'model', 'trim'])['price'].mean().reset_index()

for model in selected_models:
    fig, ax = plt.subplots(figsize=(10, 4))
    model_data = trend_data[trend_data['model'] == model]
    for trim in model_data['trim'].unique():
        trim_data = model_data[model_data['trim'] == trim]
        ax.plot(trim_data['date'], trim_data['price'], label=trim)
    ax.set_title(f"{model} Price Trend")
    ax.set_ylabel("Price ($)")
    ax.legend()
    st.pyplot(fig)
#
