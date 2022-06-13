import geopandas as GSPD
import folium
import requests
import streamlit as st
import pandas as pd
import psycopg2
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
    st.write(delivery_data[:100])

    """С помощью геопанадаса создадим геодатафрейм, а с помощью api osm-boundaries получим районы заказов"""
    geodata=GSPD.GeoDataFrame(delivery_data, geometry=GSPD.points_from_xy(delivery_data['lon'], delivery_data['lat']))

    r = requests.get('https://osm-boundaries.com/Download/Submit?apiKey=e9b4f2bc50eb80add2476d56fca94483&db=osm20220404&osmIds=-1257484,-2162195,-1255987,-364001,-1257218,-1275551,-1257786,-1275608,-1275627,-1255942,-1319060,-1319142,-1319263,-1319245,-1298976,-1319078,-1255576,-1255602,-1299106,-1299013,-1255775,-1255704,-574667,-364551,-1299031,-1255680,-446087,-446271,-1250618,-445284,-446086,-445282,-446272,-1250619,-1252407,-1250724,-1252448,-1252424,-446084,-446085,-1252465,-1250526,-446083&minAdminLevel=8&maxAdminLevel=8&format=GeoJSON&srid=4326').text
    st.write(r)
    districts=GSPD.GeoDataFrame(r)
    st.dataframe(districts)





