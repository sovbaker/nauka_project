import altair as alt
import folium
import geopandas as gpd
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import pydeck as pdk
import streamlit as st
from folium.plugins import FastMarkerCluster
from geopy import distance
from shapely.geometry import Point
from streamlit_echarts import st_echarts
from streamlit_folium import folium_static
from supabase import create_client, Client
import psycopg2
import rtree
import pygeos
import requests

st.set_page_config(layout="wide")
with st.echo(code_location='below'):
    """
    >Ты есть то, что ты ешь
    Это цитата какого-то умного древнего грека, которая к мне кажется хорошо описывает мой проект
    В прошлый раз я делал анализ данных слива Яндекс Еды. В этот раз мой проект будет основывааться на сливе данных
    Деливери клаб. Я попытаюсь найти связя между тем, сколько люди тратят на еду и тем как они голосуют на выборах
    
    ### Часть 1: Сбор и хранение данных
    В файлах проекта вы найдете ноут из jupiter, где я собирал данные. Чтоб вы несильно утруждались читая его здесь я
    вкратце опишу, что происходило с данными.
    
    **Для тех, кто на самом деле считает строки кода:** 
    - Во 1) вы душнила, 
    - во 2) кода в юпитере много, но я там не помечал заимстования, поэтому весь важный код оттуда будет здесь 
    
    
    
    
    """


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
    def get_data():
        return pd.read_sql("""select * from yandex_full_mos limit 100000""", conn)


#     initial_df = get_data()
#     df = initial_df.copy(deep=True)
#     st.write(len(df))
# with st.echo(code_location='below'):
#     """
#         После этого добавим некоторую дополнительную информацию для наших заказов
#         День недели, время дня, административный округ, расстояние до центра Москвы
#     """
#     # Берем дни недели и часы
#     ## FROM (DZ 13)
#     df['yandex_created_at'] = pd.to_datetime(df['yandex_created_at'], utc=True)
#     df['day_of_week'] = df['yandex_created_at'].dt.day_name()
#     df['Time'] = df['yandex_created_at'].dt.hour
#     df['Times_of_Day'] = 'null'
#     df['Times_of_Day'].mask((df['Time'] >= 6) & (df['Time'] <= 12), 'утро', inplace=True)
#     df['Times_of_Day'].mask((df['Time'] > 12) & (df['Time'] <= 18), 'день', inplace=True)
#     df['Times_of_Day'].mask((df['Time'] > 18) & (df['Time'] <= 23), 'вечер', inplace=True)
#     df['Times_of_Day'].mask(df['Times_of_Day'] == 'null', 'ночь', inplace=True)
#     df['Times_of_Day'] = df['Times_of_Day'].astype(str)
#     ##END
#     @st.experimental_singleton()
#     def get_geodf():
#         return gpd.GeoDataFrame(df, geometry = gpd.points_from_xy(df['yandex_longitude'], df['yandex_latitude']))
#
#
#
#     geodata=get_geodf()
#     geodata['distance_from_center']=geodata['geometry'].distance(Point(37.621211, 55.753544))
#     st.write(geodata[:100])
#     @st.experimental_singleton()
#     def get_distance():
#         return geodata['geometry'].distance(Point(37.621211, 55.753544))
#
#     dist = get_distance()
#     geodata['distance_from_center'] = dist
#     @st.experimental_singleton()
#     def get_districts():
#         # Здесь мы получаем данные о полигонах московских административных округов и районов
#         # source (http://osm-boundaries.com)
#         districts_df = gpd.read_file('OSMB-2e42e582a7ff472b5bebcd7e903c5a69ffc81025.geojson')
#         return geodata.sjoin(districts_df[['local_name', 'geometry']])
#
#     moscow_districts = get_districts()
#     del geodata
#     st.write(moscow_districts)
#
#     """Я хочу анализировать только Москву, поэтому удалю заказы не из Москвы"""
#     moscow_districts.dropna(subset=['district'], inplace=True)
#     moscow_districts['day_of_week'].mask(moscow_districts['day_of_week'] == 'Friday', 'Пятница', inplace=True)
#     moscow_districts['day_of_week'].mask(moscow_districts['day_of_week'] == 'Monday', 'Понедельник', inplace=True)
#     moscow_districts['day_of_week'].mask(moscow_districts['day_of_week'] == 'Tuesday', 'Вторник', inplace=True)
#     moscow_districts['day_of_week'].mask(moscow_districts['day_of_week'] == 'Wednesday', 'Среда', inplace=True)
#     moscow_districts['day_of_week'].mask(moscow_districts['day_of_week'] == 'Thursday', 'Четверг', inplace=True)
#     moscow_districts['day_of_week'].mask(moscow_districts['day_of_week'] == 'Saturday', 'Суббота', inplace=True)
#     moscow_districts['day_of_week'].mask(moscow_districts['day_of_week'] == 'Sunday', 'Воскресенье', inplace=True)
#
#     df_final=moscow_districts
#     sorter = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье']
#     sorterIndex = dict(zip(sorter, range(len(sorter))))
#     df_final['day_of_week_id'] = df_final['day_of_week'].map(sorterIndex)
#     sorter2 = ['утро', 'день', 'вечер', 'ночь']
#     sorterIndex2 = dict(zip(sorter2, range(len(sorter2))))
#     df_final['time_id'] = df_final['Times_of_Day'].map(sorterIndex2)
#     df_final.sort_values(['day_of_week_id', 'time_id'], inplace=True)
#
#     """
#     #### Теперь будем рисовать. Давайте сначала просто посмотрим, как наши заказы выглядят на карте
#      """
#
#     m = folium.Map(location=[55.753544, 37.621211], zoom_start=10, width=1200)
#     FastMarkerCluster(
#         data=[[lat, lon] for lat, lon in zip(df_final['location_latitude'], df_final['location_longitude'])]
#         , name='Заказы').add_to(m)
#
#     folium_static(m, width=1200)
#     """#### Давайте посмотрим на заказы в разрезе муниципалитета, административного округа по среднему чеку и по количеству"""
#     col1, col2 = st.columns(2)
#     with col1:
#         option1 = st.selectbox('Какое деление вы хотите выбрать?', ('Округа', 'Районы'))
#     with col2:
#         option2 = st.selectbox('Как вы хотите их сравнить?', ('Количество заказов', 'Средний чек'))
#
#     if option1 == 'Районы':
#         df_municipalities = (df_final.groupby(['district'], as_index=False)
#                              .agg({'id': 'count', 'amount_charged': 'mean'})
#                              .merge(moscow_geometry_df, on='district', how='left'))
#         geopandas.GeoDataFrame(moscow_geometry_df[['district', 'okrug', 'geometry']]) \
#             .to_file("moscow_geometry.geojson",
#                      driver='GeoJSON')
#         geojson = 'moscow_geometry.geojson'
#         if option2 == 'Количество заказов':
#             merge_col = ['district', 'id']
#             scale = (df_municipalities['id'].quantile((0.5, 0.6, 0.7, 0.8))).tolist()
#             legend = 'Количество заказов'
#         else:
#             merge_col = ['district', 'amount_charged']
#             scale = (df_municipalities['amount_charged'].quantile((0.5, 0.6, 0.7, 0.8))).tolist()
#             legend = 'Средний чек'
#         keys = 'feature.properties.district'
#         ##FROM (https://towardsdatascience.com/folium-and-choropleth-map-from-zero-to-pro-6127f9e68564)
#         # tooltip = folium.features.GeoJson(
#         #     data=df_municipalities.dropna(),
#         #     name=legend,
#         #     smooth_factor=2,
#         #     style_function=lambda x: {'color': 'black', 'fillColor': 'transparent', 'weight': 0.5},
#         #     tooltip=folium.features.GeoJsonTooltip(
#         #         fields=[
#         #             'district',
#         #             'amount_charged',
#         #             'id'],
#         #         aliases=[
#         #             'Район:',
#         #             "Средний чек:",
#         #             "Кол-во заказов:",
#         #         ],
#         #         localize=True,
#         #         sticky=False,
#         #         labels=True,
#         #         style="""
#         #                     background-color: #F0EFEF;
#         #                     border: 2px solid black;
#         #                     border-radius: 3px;
#         #                     box-shadow: 3px;
#         #                 """,
#         #         max_width=800, ), highlight_function=lambda x: {'weight': 3, 'fillColor': 'grey'})
#         ## END
#     elif option1 == 'Округа':
#         df_municipalities = (df_final.groupby(['okrug'], as_index=False)
#                              .agg({'id': 'count', 'amount_charged': 'mean'})
#                              .merge(geopandas.read_file('okruga.geojson')
#                                     , left_on='okrug'
#                                     , right_on='local_name'
#                                     , how='left'))
#         geojson = 'okruga.geojson'
#         if option2 == 'Количество заказов':
#             merge_col = ['okrug', 'id']
#             scale = (df_municipalities['id'].quantile((0.3, 0.5, 0.6, 0.7, 0.8))).tolist()
#             legend = 'Количество заказов'
#         else:
#             merge_col = ['okrug', 'amount_charged']
#             scale = (df_municipalities['amount_charged'].quantile((0.3, 0.5, 0.6, 0.7, 0.8))).tolist()
#             legend = 'Средний чек'
#         keys = 'feature.properties.local_name'
#         ##FROM (https://towardsdatascience.com/folium-and-choropleth-map-from-zero-to-pro-6127f9e68564)
#         # tooltip = folium.features.GeoJson(
#         #     data=df_municipalities.dropna(),
#         #     name=legend,
#         #     smooth_factor=2,
#         #     style_function=lambda x: {'color': 'black', 'fillColor': 'transparent', 'weight': 0.5},
#         #     tooltip=folium.features.GeoJsonTooltip(
#         #         fields=[
#         #             'okrug',
#         #             'amount_charged',
#         #             'id'],
#         #         aliases=[
#         #             'Адм. округ:',
#         #             "Средний чек:",
#         #             "Кол-во заказов:",
#         #         ],
#         #         localize=True,
#         #         sticky=False,
#         #         labels=True,
#         #         style="""
#         #                             background-color: #F0EFEF;
#         #                             border: 2px solid black;
#         #                             border-radius: 3px;
#         #                             box-shadow: 3px;
#         #                         """,
#         #         max_width=800),
#         #     highlight_function=lambda x: {'weight': 3, 'fillColor': 'grey'}
#         # )
#         ##END
#
#     map = folium.Map(location=[55.753544, 37.621211], zoom_start=10, width=1200)
#
#     cho = folium.Choropleth(geo_data=geojson, data=df_municipalities, columns=merge_col
#                             , key_on=keys
#                             , fill_color='YlOrRd'
#                             , nan_fill_color="White"
#                             , legend_name=legend
#                             , tooltip='amount_charged'
#
#                             ).add_to(map)
#     folium_static(map, width=1200)
#
#     '''#### Теперь давайте посмотрим на заказы в разрезе дня недели и времени дня'''
#     df_weekday_time = df_final.groupby(['day_of_week', 'Times_of_Day'], as_index=False) \
#         .agg({'id': 'count', 'amount_charged': 'mean'})
#
#     # df_weekday_time['day_of_week'].mask(df_weekday_time['day_of_week'] == 'Friday', 'Пятница', inplace=True)
#     # df_weekday_time['day_of_week'].mask(df_weekday_time['day_of_week'] == 'Monday', 'Понедельник', inplace=True)
#     # df_weekday_time['day_of_week'].mask(df_weekday_time['day_of_week'] == 'Tuesday', 'Вторник', inplace=True)
#     # df_weekday_time['day_of_week'].mask(df_weekday_time['day_of_week'] == 'Wednesday', 'Среда', inplace=True)
#     # df_weekday_time['day_of_week'].mask(df_weekday_time['day_of_week'] == 'Thursday', 'Четверг', inplace=True)
#     # df_weekday_time['day_of_week'].mask(df_weekday_time['day_of_week'] == 'Saturday', 'Суббота', inplace=True)
#     # df_weekday_time['day_of_week'].mask(df_weekday_time['day_of_week'] == 'Sunday', 'Воскресенье', inplace=True)
#     ## FROM (http://blog.quizzicol.com/2016/10/03/sorting-dates-in-python-by-day-of-week/)
#     sorter = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье']
#     sorterIndex = dict(zip(sorter, range(len(sorter))))
#     df_weekday_time['day_of_week_id'] = df_weekday_time['day_of_week'].map(sorterIndex)
#
#     sorter2 = ['утро', 'день', 'вечер', 'ночь']
#     sorterIndex2 = dict(zip(sorter2, range(len(sorter2))))
#     df_weekday_time['time_id'] = df_weekday_time['Times_of_Day'].map(sorterIndex2)
#     df_weekday_time.sort_values(['day_of_week_id', 'time_id'], inplace=True)
#     ## END
#
#     fig1 = go.Figure(data=[go.Bar(name='Количество заказов', x=
#     df_weekday_time[df_weekday_time['day_of_week'] == list(df_weekday_time['day_of_week'].unique())[0]]['Times_of_Day']
#                                   , y=df_weekday_time[
#             df_weekday_time['day_of_week'] == list(df_weekday_time['day_of_week'].unique())[0]]['id'], yaxis='y'
#                                   , offsetgroup=1)
#         , go.Bar(name='Cредний чек', x=df_weekday_time[
#             df_weekday_time['day_of_week'] == list(df_weekday_time['day_of_week'].unique())[0]]['Times_of_Day']
#                  , y=df_weekday_time[
#                 df_weekday_time['day_of_week'] == list(df_weekday_time['day_of_week'].unique())[0]]['amount_charged']
#                  , yaxis="y2", offsetgroup=2)])
#
#     frames = []
#     steps = []
#     for days in list(df_weekday_time['day_of_week'].unique())[1:]:
#         df_weekday_time_day = df_weekday_time[df_weekday_time['day_of_week'] == days]
#         frames.append(go.Frame(data=[go.Bar(name='Количество заказов', x=df_weekday_time_day['Times_of_Day']
#                                             , y=df_weekday_time_day['id'], yaxis='y'
#                                             , offsetgroup=1)
#             , go.Bar(name='Cредний чек', x=df_weekday_time_day['Times_of_Day']
#                      , y=df_weekday_time_day['amount_charged'], yaxis="y2", offsetgroup=2)], name=days))
#     for days in list(df_weekday_time['day_of_week'].unique()):
#         step = dict(
#             label=days,
#             method="animate",
#             args=[[days]]
#         )
#         steps.append(step)
#     sliders = [dict(
#         currentvalue={"prefix": "День недели: ", "font": {"size": 16}},
#         len=0.9,
#         x=0.1,
#         pad={"b": 10, "t": 50},
#         steps=steps,
#     )]
#     fig1.update_layout(title="Количество заказов и средний чек по дням недели",
#                        barmode='group',
#
#                        xaxis_title="Время дня",
#                        yaxis=dict(
#                            title="Количество заказов"
#                        ),
#                        yaxis2=dict(
#                            title="Средний чек",
#                            overlaying="y",
#                            side="right"
#                        ),
#                        ##FROM (https://habr.com/ru/post/502958/)
#                        updatemenus=[dict(direction="left",
#                                          pad={"r": 10, "t": 80},
#                                          x=0.1,
#                                          xanchor="right",
#                                          y=0,
#                                          yanchor="top",
#                                          showactive=False,
#                                          type="buttons",
#                                          buttons=[dict(label="►", method="animate", args=[None, {"fromcurrent": True}]),
#                                                   dict(label="❚❚", method="animate",
#                                                        args=[[None], {"frame": {"duration": 0, "redraw": False},
#                                                                       "mode": "immediate",
#                                                                       "transition": {"duration": 0}}])])],
#                        ##END
#                        legend_x=1.12, width=1200
#                        )
#
#     fig1.layout.sliders = sliders
#     fig1.frames = frames
#     st.plotly_chart(fig1)
#
#     """#### Теперь давайте посмотрим на тоже самое на карте"""
#
#
#     ## From (https://github.com/streamlit/demo-uber-nyc-pickups/blob/main/streamlit_app.py)
#     def get_map(data):
#         st.pydeck_chart(
#             pdk.Deck(
#                 map_style="mapbox://styles/mapbox/light-v9",
#                 initial_view_state={
#                     "latitude": 55.753544,
#                     "longitude": 37.621211,
#                     "zoom": 10,
#                     "pitch": 50,
#                 },
#                 layers=[
#                     pdk.Layer(
#                         "HexagonLayer",
#                         data=data,
#                         get_position=["location_longitude", "location_latitude"],
#                         radius=120,
#                         elevation_scale=4,
#                         elevation_range=[0, 1000],
#                         pickable=True,
#                         extruded=True,
#                     ),
#                 ],
#             ), use_container_width=True
#         )
#
#
#     ## End
#
#     day = st.select_slider('Выберете день недели', df_final['day_of_week'].unique())
#     ## если что это не я дурак, а пандас, потому что не находил иначе нужную часть датасета
#     if day == 'Понедельник':
#         query1 = 'day_of_week=="Понедельник" and Times_of_Day=="утро"'
#         query2 = 'day_of_week=="Понедельник" and Times_of_Day=="день"'
#         query3 = 'day_of_week=="Понедельник" and Times_of_Day=="вечер"'
#         query4 = 'day_of_week=="Понедельник" and Times_of_Day=="ночь"'
#     elif day == 'Вторник':
#         query1 = 'day_of_week=="Вторник" and Times_of_Day=="утро"'
#         query2 = 'day_of_week=="Вторник" and Times_of_Day=="день"'
#         query3 = 'day_of_week=="Вторник" and Times_of_Day=="вечер"'
#         query4 = 'day_of_week=="Вторник" and Times_of_Day=="ночь"'
#     elif day == 'Среда':
#         query1 = 'day_of_week=="Среда" and Times_of_Day=="утро"'
#         query2 = 'day_of_week=="Среда" and Times_of_Day=="день"'
#         query3 = 'day_of_week=="Среда" and Times_of_Day=="вечер"'
#         query4 = 'day_of_week=="Среда" and Times_of_Day=="ночь"'
#     elif day == 'Четверг':
#         query1 = 'day_of_week=="Четверг" and Times_of_Day=="утро"'
#         query2 = 'day_of_week=="Четверг" and Times_of_Day=="день"'
#         query3 = 'day_of_week=="Четверг" and Times_of_Day=="вечер"'
#         query4 = 'day_of_week=="Четверг" and Times_of_Day=="ночь"'
#     elif day == 'Пятница':
#         query1 = 'day_of_week=="Пятница" and Times_of_Day=="утро"'
#         query2 = 'day_of_week=="Пятница" and Times_of_Day=="день"'
#         query3 = 'day_of_week=="Пятница" and Times_of_Day=="вечер"'
#         query4 = 'day_of_week=="Пятница" and Times_of_Day=="ночь"'
#     elif day == 'Суббота':
#         query1 = 'day_of_week=="Суббота" and Times_of_Day=="утро"'
#         query2 = 'day_of_week=="Суббота" and Times_of_Day=="день"'
#         query3 = 'day_of_week=="Суббота" and Times_of_Day=="вечер"'
#         query4 = 'day_of_week=="Суббота" and Times_of_Day=="ночь"'
#     elif day == 'Воскресенье':
#         query1 = 'day_of_week=="Воскресенье" and Times_of_Day=="утро"'
#         query2 = 'day_of_week=="Воскресенье" and Times_of_Day=="день"'
#         query3 = 'day_of_week=="Воскресенье" and Times_of_Day=="вечер"'
#         query4 = 'day_of_week=="Воскресенье" and Times_of_Day=="ночь"'
#
#     morning, day = st.columns(2)
#     with morning:
#         """#### Утро"""
#         df_morn = df_final.query(query1).dropna(subset=["location_longitude", "location_latitude"])
#         get_map(df_morn)
#         """#### Вечер"""
#         df_day = df_final.query(query3).dropna(subset=["location_longitude", "location_latitude"])
#         get_map(df_day)
#     with day:
#         """#### День"""
#         df_eve = df_final.query(query2).dropna(subset=["location_longitude", "location_latitude"])
#         get_map(df_eve)
#         """#### Ночь"""
#         df_night = df_final.query(query4).dropna(subset=["location_longitude", "location_latitude"])
#         get_map(df_night)
#
#     """### Теперь давайте посмотрим на зависимость среднего чека и количество заказов от расстояния до центра"""
#     df_dist = df_final.query('distance_from_center<30')
#     df_dist['distance_from_center'] = df_dist['distance_from_center'].round(1)
#     coll1, coll2 = st.columns(2)
#     with coll1:
#         """#### По среднему чеку"""
#         ## From(https://altair-viz.github.io/gallery/poly_fit_regression.html
#         base = alt.Chart(df_dist).mark_circle(color="black").encode(
#             alt.X("distance_from_center"), alt.Y("amount_charged"))
#         polynomial_fit = [
#             base.transform_regression(
#                 "distance_from_center", "amount_charged", method="poly", order=order,
#                 as_=["distance_from_center", str(order)]
#             )
#                 .mark_line()
#                 .transform_fold([str(order)], as_=["degree", "amount_charged"])
#                 .encode(alt.Color("degree:N"))
#             for order in [1, 3, 5]]
#         # end
#         st.altair_chart(alt.layer(base, *polynomial_fit))
#     with coll2:
#         """#### Количество заказов"""
#
#         df_dist_2 = df_dist.groupby('distance_from_center', as_index=False).agg({'id': 'count'})
#         df_dist_2['distance_from_center'] = np.round(df_dist_2['distance_from_center'], 0)
#         base = alt.Chart(df_dist.groupby('distance_from_center', as_index=False).agg({'id': 'count'})).mark_circle(
#             color="black").encode(
#             alt.X("distance_from_center"), alt.Y("id"))
#         polynomial_fit = [
#             base.transform_regression(
#                 "distance_from_center", "count", method="poly", order=order,
#                 as_=["distance_from_center", str(order)]
#             )
#                 .mark_line()
#                 .transform_fold([str(order)], as_=["degree", "id"])
#                 .encode(alt.Color("degree:N"))
#             for order in [1, 3, 5]]
#         # end
#         st.altair_chart(alt.layer(base, *polynomial_fit))
#
#     """### Посмотрим пользователи каких устройств больше пользуются Яндекс Едой"""
#     df_final['os'] = 'Other'
#     df_final['os'].mask(df_final['user_agent'].str.lower().str.contains('ios'), 'IOS', inplace=True)
#     df_final['os'].mask(df_final['user_agent'].str.lower().str.contains('iphone'), 'IOS', inplace=True)
#     df_final['os'].mask(df_final['user_agent'].str.lower().str.contains('macintosh'), 'Mac OS X', inplace=True)
#     df_final['os'].mask(df_final['user_agent'].str.lower().str.contains('windows'), 'Windows', inplace=True)
#     df_final['os'].mask(df_final['user_agent'].str.lower().str.contains('android'), 'Android', inplace=True)
#     df_os = df_final.groupby('os', as_index=False).agg({'id': 'count'})
#     df_os.rename(columns={'os': 'name', 'id': 'value'}, inplace=True)
#     ## From (https://share.streamlit.io/andfanilo/streamlit-echarts-demo/master/app.py)
#     options = {
#         "tooltip": {"trigger": "item"},
#         "legend": {"top": "5%", "left": "center"},
#         "series": [
#             {
#                 "name": "Количество заказов",
#                 "type": "pie",
#                 "radius": ["40%", "70%"],
#                 "avoidLabelOverlap": False,
#                 "itemStyle": {
#                     "borderRadius": 10,
#                     "borderColor": "#fff",
#                     "borderWidth": 2,
#                 },
#                 "label": {"show": False, "position": "center"},
#                 "emphasis": {
#                     "label": {"show": True, "fontSize": "40", "fontWeight": "bold"}
#                 },
#                 "labelLine": {"show": False},
#                 "data": df_os.to_dict('records'),
#             }
#         ],
#     }
#     st_echarts(
#         options=options, height="500px",
#     )
#     ## END
#     """### А пользователи каких устройств больше платят в среднем?"""
#     df_os_charge = df_final.groupby('os', as_index=False).agg({'amount_charged': 'mean'})
#     ##From (https://echarts.apache.org/examples/en/editor.html?c=bar-simple&lang=js)
#     options = {
#         "tooltip": {"trigger": "item"},
#         'xAxis': {
#             'type': 'category',
#             'data': df_os_charge['os'].to_list()
#         },
#         'yAxis': {
#             'type': 'value'
#         },
#         "series": [
#             {
#                 "data": df_os_charge['amount_charged'].to_list(),
#                 'type': 'bar'
#             }
#         ],
#     }
#     ##End
#     st_echarts(
#         options=options, height="500px",
#     )

