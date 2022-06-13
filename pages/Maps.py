import geopandas as GSPD
import folium
import requests
import streamlit as st
import pandas as pd
from Data_loading_and_analysis import get_data, init_connection
"""# Здесь мы будем рисвать карты"""

st.set_page_config(layout='wide')

with st.echo(code_location='below'):

    """Получим данные"""
    conn = init_connection()
    delivery_data = get_data("""select delivery2_user_id as user_id
                                , (case when delivery2_promocode is null then 0 else 1 end) as promo_flg
                                , delivery2_products as products, delivery2_vendor_name as vendor
                                , delivery2_price_client_rub as spent
                                , delivery2_latitude as lat
                                , delivery2_longitude as lon
                                from delivery_full""")
    st.dataframe(delivery_data[:100])




