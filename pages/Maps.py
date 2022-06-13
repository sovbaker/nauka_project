import geopandas as GSPD
import folium
import requests
import streamlit as st
import pandas as pd
import psycopg2
from shapely.wkt import loads
"""# Здесь мы будем рисвать карты"""



with st.echo(code_location='below'):
    @st.experimental_singleton
    def init_connection():
        return psycopg2.connect(**st.secrets["postgres"])


    conn = init_connection()


    # Perform query.
    # Uses st.experimental_memo to only rerun when the query changes or after 10 min.
    @st.experimental_memo(ttl=600)
    def run_query(query):
        with conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall()


    @st.experimental_singleton()
    def get_data(query):
        return pd.read_sql(query, conn)
    """Получим данные"""
    conn = init_connection()
    delivery_data = get_data("""select delivery2_user_id as user_id
                                , (case when delivery2_promocode is null then 0 else 1 end) as promo_flg
                                , delivery2_products as products, delivery2_vendor_name as vendor
                                , delivery2_price_client_rub as spent
                                , delivery2_latitude as lat
                                , delivery2_longitude as lon
                                from delivery_full""")

    """С помощью геопанадаса создадим геодатафрейм, а с помощью api osm-boundaries получим районы заказов"""
    geodata = GSPD.GeoDataFrame(delivery_data, geometry=GSPD.points_from_xy(delivery_data['lon'], delivery_data['lat']))

    districts_pd=pd.read_csv('mos_poly.csv')
    geodistricts = GSPD.GeoDataFrame(districts_pd, geometry=districts_pd['geometry'].apply(loads))

    geodata=geodata.sjoin(geodistricts, how='left')
    st.write(geodata)