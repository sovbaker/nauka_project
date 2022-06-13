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

st.markdown("""
# Еда и выборы

>Ты есть то, что ты ешь

Это цитата какого-то умного древнего грека, которая как мне кажется, хорошо описывает мой проект

В прошлый раз я делал анализ данных слива Яндекс Еды. В этот раз мой проект будет основывааться на сливе данных
Деливери клаб. 

Я попытаюсь найти связя между тем, сколько люди тратят на еду и тем как они голосуют на выборах

### Сбор данных
В файлах проекта вы найдете ноут из jupiter, где я собирал данные. Чтоб вы несильно утруждались читая его здесь я
вкратце опишу, что происходило с данными.

**Для тех, кто на самом деле считает строки кода:** 
- Во 1) вы душнила 
- во 2) кода в юпитере много, но я там не помечал заимстования, поэтому весь важный код оттуда будет здесь 
За основу были взяты данные delivery club
```python
##FROM откуда-то со стэковерфлоу
import glob
import pandas as pd

# Get CSV files list from a folder
path = '/Users/olegbaranov/Downloads/csv/delivery2_full'
csv_files = glob.glob(path + "/*.csv")

# Read each CSV file into DataFrame
# This creates a list of dataframes
df_list = (pd.read_csv(file, dtype={'phone_number':'int'}, on_bad_lines='skip', low_memory=False) for file in csv_files)

# Concatenate all DataFrames
big_df_delivery= pd.concat(df_list, ignore_index=True)
##
```   
Берем данные только из Москвы и группируем по user_id телефону и адресу, чтоб получить уникальных пользователей
```python
df_for_address_parse=big_df_delivery
.query('delivery2_address_city=="Москва"')
.groupby(['delivery2_user_id'
    , 'phone_number'
    , 'delivery2_address_street'
    , 'delivery2_address_building'
    ,'delivery2_address_flat_number'], as_index=False)['delivery2_price_client_rub'].sum()
```
#### Хотим спарсить с сайта ЦИК участковые избирательные комиссии для адресов
мы хотим привести адрес к виду, близкому тому что хочет ЦИК, так как он не любит например адреса с корпусами
, строениями, а также со словом набережная.
Для этого мы берем и с помощью ***продвинутых возможностей pandas*** и ***регулярных выражений (для решения задачи
, для которой трудно придумать простое решение без регулярных выражений)*** выделяем номер дома, корпус или строение
```python 
df_for_address_parse=(df_for_address_parse
                  .merge((df_for_address_parse
                        .delivery2_address_building
                        .str.extract(r'(\d?\d?\d?\d?\d?)([кcстр]?)(\d?\d?\d?\d?\d?\d?)')
                        .rename(columns={0:'bulding_num', 1:'suffix', 2:'suffix_num'}))
                        , left_index=True, right_index=True))
```
##### Неоптимальный вариант выполнения этой задачи с помощью web scrapping
```python
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
driver.get('http://www.cikrf.ru/digital-services/naydi-svoy-izbiratelnyy-uchastok/')
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import ElementClickInterceptedException, NoSuchElementException, StaleElementReferenceException
from time import sleep, time
    
tnow=time()
for idx, row in df_for_address_parse.iterrows():
    city='Город Москва, '
    street=str(row.delivery2_address_street)+', '
    if len(row.suffix)!=1:
        building='д. '+str(row.bulding_num)
    else:
        building='д. '+str(row.bulding_num)+' '+str(row.suffix)+'. '+str(row.suffix_num)
    flat=', '+'кв. ' + str(row.delivery2_address_flat_number)
    try:
        try:
            search_element=driver.find_element(By.XPATH, "//*[@class='form-control address-select2 address-selected']")
            search_element.clear()
            sleep(0.1)
            search_element.send_keys(city+street+building+flat)
            sleep(0.5)
            search_element.send_keys(Keys.ENTER)
            search_button=driver.find_element(By.ID, 'send')
            try:
                search_button.click()
                sleep(1)
                if len (driver.find_elements(By.XPATH, "//h4[@class='digital-subtitle']"))>2:
                    result=driver.find_elements(By.XPATH, "//h4[@class='digital-subtitle']")[2].text
                else:
                    search_element.clear()
                    search_element.send_keys(city+street+building)
                    sleep(0.3)
                    search_element.send_keys(Keys.ARROW_DOWN)
                    search_element.send_keys(Keys.ARROW_DOWN)
                    sleep(0.3)
                    search_element.send_keys(Keys.ENTER)
                    sleep(0.3)
                    search_button.click()
                    sleep(1)
                    result_list=driver.find_elements(By.XPATH, "//h4[@class='digital-subtitle']")
                    for x in result_list:
                        if len(x.text)>0:
                            result=x.text
                        else:
                            result='null'
            except ElementClickInterceptedException:
                search_element.clear()
                search_element.send_keys(city+street+building)
                sleep(0.3)
                search_element.send_keys(Keys.ARROW_DOWN)
                search_element.send_keys(Keys.ARROW_DOWN)
                sleep(0.3)
                search_element.send_keys(Keys.ENTER)
                sleep(0.3)
                try:
                    search_button.click()
                    sleep(1)
                    result_list=driver.find_elements(By.XPATH, "//h4[@class='digital-subtitle']")
                    if len(result_list)>0:
                        for x in result_list:
                            if len(x.text)>0:
                                result=x.text
                            else:
                                result='null'
                    else:
                        result='null'
                except ElementClickInterceptedException:
                    result='null'
        except NoSuchElementException:
            search_element=driver.find_element(By.XPATH, "//*[@class='form-control address-select2']")
            search_element.clear()
            sleep(0.1)
            search_element.send_keys(city+street+building+flat)
            sleep(0.5)
            search_element.send_keys(Keys.ENTER)
            search_button=driver.find_element(By.ID, 'send')
            try:
                search_button.click()
                sleep(1)
                if len (driver.find_elements(By.XPATH, "//h4[@class='digital-subtitle']"))>2:
                    result=driver.find_elements(By.XPATH, "//h4[@class='digital-subtitle']")[2].text
                else:
                    search_element.clear()
                    search_element.send_keys(city+street+building)
                    sleep(0.3)
                    search_element.send_keys(Keys.ARROW_DOWN)
                    search_element.send_keys(Keys.ARROW_DOWN)
                    sleep(0.3)
                    search_element.send_keys(Keys.ENTER)
                    sleep(0.3)
                    search_button.click()
                    sleep(1)
                    result_list=driver.find_elements(By.XPATH, "//h4[@class='digital-subtitle']")
                    for x in result_list:
                        if len(x.text)>0:
                            result=x.text
                        else:
                            result='null'
            except ElementClickInterceptedException:
                search_element.clear()
                sleep(0.3)
                search_element.send_keys(city+street+building)
                sleep(0.3)
                search_element.send_keys(Keys.ARROW_DOWN)
                search_element.send_keys(Keys.ARROW_DOWN)
                sleep(0.3)
                search_element.send_keys(Keys.ENTER)
                sleep(0.3)
                try:
                    search_button.click()
                    sleep(1)
                    result_list=driver.find_elements(By.XPATH, "//h4[@class='digital-subtitle']")
                    if len(result_list)>0:
                        for x in result_list:
                            if len(x.text)>0:
                                result=x.text
                            else:
                                result='null'
                    else:
                        result='null'
                except ElementClickInterceptedException:
                    result='null'
    except:
        result='null'
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
        driver.get('http://www.cikrf.ru/digital-services/naydi-svoy-izbiratelnyy-uchastok/')
    df_for_address_parse.at[idx, 'UIK']=result
    driver.refresh()
    sleep(0.5)
    print(idx, time()-tnow)
```

Почему неоптимальный? Потому что он парсит один адрес примерно 5 секунд, чтоб спарсить УИКи на весь датасет
придется ждать 15 дней

Делает он это примерно так:
""")
##видосик
st.markdown("""
##### Оптимальный вариант
Когда мы пишем в ЦИК адрес он обращается к своему api и делает запрос, чтобы получить участок, однако
к сожалению получилось так, что через него не получается достать уик
, так как там стоит защита от ддос атак и он не дает подключиться модулю request и банит по ip

##### Не очень оптимальный, но хотя бы рабочий вариант
В итоге пришлось делать так. Через парсинг сайта мосгоризберкома, который не банит модуль requests
```python
# Пошаманили с помощью регулярных выражений с улицами
ulitsy =df_for_address_parse.delivery2_address_street.str.lower()
.str.extract(r'(\w?\w?\w?\w?\w?\w?\w?
\w?\w?\w?\w?\w?\w?\w?\w?\w?\w?)
(\s?)(\w?\w?\w?\w?\w?\w?\w?\w?\w?\w?
\w?\w?\w?\w?\w?\w?\w?\w?\w?\w?\w?\w?\w?\w?\w?
\w?\w?)(\s?)(\w?\w?\w?\w?\w?\w?\w?\w?\w?\w?\w?\w?
\w?\w?\w?\w?\w?\w?\w?\w?\w?\w?\w?)(\s?)
(\w?\w?\w?\w?\w?\w?\w?\w?\w?\w?\w?\w?\w?\w?\w?\w?\w?\w?\w?\w?\w?\w?\w?)')

ulitsy[[0,2]]=ulitsy[[0,2]].mask((ulitsy[2]=='улица')|(ulitsy[2]=='проспект')|(ulitsy[2]=='шоссе')
|(ulitsy[2]=='проезд')|(ulitsy[2]=='пр-д')
|(ulitsy[2]=='пр-кт')|(ulitsy[2]=='ул')|(ulitsy[2]=='переулок'), ulitsy[[2,0]].values)
ulitsy.rename(columns={0:'type', 1:'space', 2:'nazv', 3:'add_name', 4:'add_name_2', 5:'add_name_3', 6:'add_name_4'}
, inplace=True)
df_for_address_parse=df_for_address_parse.merge(ulitsy, how='left', left_index=True, right_index=True)

# Скрипт, который парсит сайт Мосгоризберкома
from time import sleep, time 
from requests.exceptions import RequestException
import requests
from bs4 import BeautifulSoup
tnow=time()
null_cnt=0
all_cnt=0

for idx, row in df_for_address_parse
    [(df_for_address_parse['UIK']=='null')&(df_for_address_parse['parse_flg']!=1)][7000:].iterrows():
    try:
        if row.str.contains(r'[\bулица\b]', regex=True).any():
            street=str(row.nazv)+row.add_name+str(row.add_name_2).strip(' ')+str(row.add_name_3)+row.add_name_4
        else:
            street=row.type+row.space+row.nazv+row.add_name+row.add_name_2+row.add_name_3+row.add_name_4
    
        if len(street)<3:
            street=row.delivery2_address_street
        if len(row.suffix)!=1:
            building=str(row.bulding_num)
        elif row.suffix=='к':
            building=str(row.bulding_num)+' '+'корп'+'. '+str(row.suffix_num)
        else:
            building=str(row.bulding_num)+' '+str(row.suffix)+'. '+str(row.suffix_num)
        flat=', '+'кв. ' + str(row.delivery2_address_flat_number)
        full_address=street+' '+building
        q={'name':street}
    
        r=requests.get('http://mosgorizbirkom.ru/precinct/address/byname/', params=q, timeout=0.4)
        r.encoding = 'utf-8'               
        soup=BeautifulSoup(r.text)
        if soup.find('a')==None:
            result='null'
        else:
            a = soup.find('a')['href'].rstrip('search/')+'/children/byname'
            q={'name':building}
            sleep(0.1)
            r2=BeautifulSoup(requests.get('http://mosgorizbirkom.ru/'+a, params=q, timeout=0.5).text)
            if r2.find('a')==None:
                result='null'
            else:
                a2=r2.find('a')['href'].rstrip('search/').strip('/precinct/address/')
                sleep(0.1)
                r_final= BeautifulSoup(requests.get('http://mosgorizbirkom.ru/precinct/precinct/boundary/byaddress/'+a2, timeout=0.5).text)
                if r_final.find('p', class_='value')==None:
                    result='null'
                else:
                    result=r_final.find('p', class_='value').get_text()
    except RequestException:
        sleep(0.2)
        result='null'
        continue
    if result=='null':
        null_cnt+=1
    all_cnt+=1
    df_for_address_parse.at[idx, 'UIK']=result
    df_for_address_parse.at[idx, 'parse_flg']=1
    if idx%10==0:
        print(all_cnt, null_cnt,time()-tnow)
    if all_cnt%100==0:
        df_for_address_parse.to_csv('df_for_address_parse.csv')
```
Таким образом, получаем УИКи для пользователей Delivery

##### Теперь мы хотим узнать как эти люди голосовали 

Для этого опять воспользуемся селениумом и спарсим
систему ГАС выборы с результатами голосвания на выборах в Госдуму в 2021 году

1. Получаем список районов с ссылками на сводные таблицы с результатом
```python
driver.get('http://www.moscow-city.vybory.izbirkom.ru/region/region/region/izbirkom?action=show&root=1000259&tvd=100100225883701&vrn=100100225883172&prver=0&pronetvd=null&region=77&sub_region=77&type=464&report_mode=null')
from selenium.webdriver.common.by import By
moscow=driver.find_elements(By.XPATH, "//*[@id='100100225883696']/ul/li/ul/li/a")
res_dict={}
for district in moscow:
    if 'район' in district.text:
        res_dict[district.text]=district.get_attribute('href')
```
2. По этим ссылкам парсим таблицы 

```python 
res_dict_tables={}
for key, value in res_dict.items():
    driver.get(value)
    sleep(1)
    table=pd.read_html(driver.page_source)[5]
    table.to_csv(key+'.csv')
    res_dict_tables[key]=table
    
```
Вот и все теперь у нас есть данные о том как голосовали люди
Теперь по шаманим немного с этими данными, чтоб получить одну таблицу из  УИКов и результатов кандидатов 

Кроме того, добавим информацию о том является ли кандидат рекомендацией Умного Голосования
```python 
df_list=[]
for district, table in res_dict_tables.items():
    frame=table.drop(list(range(12)), axis=0).drop('Unnamed: 0', axis=1).rename(columns={'Unnamed: 1':'Candidate'}).melt(id_vars=['Candidate'])
    frame['district']=district
    df_list.append(frame)
candidates_and_uiks=pd.concat(df_list)
umg_list = pd.read_html('<table class="c5"><tbody><tr class="c4"><td class="c1" colspan="1" rowspan="1"><p class="c3"><span class="c6">196 Город Москва – Бабушкинский</span></p></td><td class="c9" colspan="1" rowspan="1"><p class="c3"><span class="c6">Рашкин Валерий Федорович</span></p></td><td class="c7" colspan="1" rowspan="1"><p class="c3"><span class="c6">КПРФ</span></p></td></tr><tr class="c13"><td class="c1" colspan="1" rowspan="1"><p class="c3"><span class="c6">197 Город Москва – Кунцевский</span></p></td><td class="c9" colspan="1" rowspan="1"><p class="c3"><span class="c6">Лобанов Михаил Сергеевич</span></p></td><td class="c7" colspan="1" rowspan="1"><p class="c3"><span class="c6">КПРФ</span></p></td></tr><tr class="c4"><td class="c1" colspan="1" rowspan="1"><p class="c3"><span class="c6">198 Город Москва – Ленинградский</span></p></td><td class="c9" colspan="1" rowspan="1"><p class="c3"><span class="c6">Брюханова Анастасия Андреевна</span></p></td><td class="c7" colspan="1" rowspan="1"><p class="c3"><span class="c6">самовыдвижение</span></p></td></tr><tr class="c13"><td class="c1" colspan="1" rowspan="1"><p class="c3"><span class="c6">199 Город Москва – Люблинский</span></p></td><td class="c9" colspan="1" rowspan="1"><p class="c3"><span class="c6">Гуличева Елена Геннадьевна</span></p></td><td class="c7" colspan="1" rowspan="1"><p class="c3"><span class="c6">КПРФ</span></p></td></tr><tr class="c4"><td class="c1" colspan="1" rowspan="1"><p class="c3"><span class="c6">200 Город Москва – Медведковский</span></p></td><td class="c9" colspan="1" rowspan="1"><p class="c3"><span class="c6">Парфенов Денис Андреевич</span></p></td><td class="c7" colspan="1" rowspan="1"><p class="c3"><span class="c6">КПРФ</span></p></td></tr><tr class="c13"><td class="c1" colspan="1" rowspan="1"><p class="c3"><span class="c6">201 Город Москва – Нагатинский</span></p></td><td class="c9" colspan="1" rowspan="1"><p class="c3"><span class="c6">Удальцова Анастасия Олеговна</span></p></td><td class="c7" colspan="1" rowspan="1"><p class="c3"><span class="c6">КПРФ</span></p></td></tr><tr class="c4"><td class="c1" colspan="1" rowspan="1"><p class="c3"><span class="c6">202 Город Москва – Новомосковский</span></p></td><td class="c9" colspan="1" rowspan="1"><p class="c3"><span class="c6">Махницкий Данил Павлович</span></p></td><td class="c7" colspan="1" rowspan="1"><p class="c3"><span class="c6">Новые люди</span></p></td></tr><tr class="c4"><td class="c1" colspan="1" rowspan="1"><p class="c3"><span class="c6">203 Город Москва – Орехово-Борисовский</span></p></td><td class="c9" colspan="1" rowspan="1"><p class="c3"><span class="c6">Петров Виталий Владимирович</span></p></td><td class="c7" colspan="1" rowspan="1"><p class="c3"><span class="c6">КПРФ</span></p></td></tr><tr class="c13"><td class="c1" colspan="1" rowspan="1"><p class="c3"><span class="c6">204 Город Москва – Перовский</span></p></td><td class="c9" colspan="1" rowspan="1"><p class="c3"><span class="c6">Курганский Сергей Борисович</span></p></td><td class="c7" colspan="1" rowspan="1"><p class="c3"><span class="c6">КПРФ</span></p></td></tr><tr class="c4"><td class="c1" colspan="1" rowspan="1"><p class="c3"><span class="c6">205 Город Москва – Преображенский</span></p></td><td class="c9" colspan="1" rowspan="1"><p class="c3"><span class="c6">Обухов Сергей Павлович</span></p></td><td class="c7" colspan="1" rowspan="1"><p class="c3"><span class="c6">КПРФ</span></p></td></tr><tr class="c13"><td class="c1" colspan="1" rowspan="1"><p class="c3"><span class="c6">206 Город Москва – Тушинский</span></p></td><td class="c9" colspan="1" rowspan="1"><p class="c3"><span class="c6">Гребенник Андрей Вадимович</span></p></td><td class="c7" colspan="1" rowspan="1"><p class="c3"><span class="c6">КПРФ</span></p></td></tr><tr class="c13"><td class="c1" colspan="1" rowspan="1"><p class="c3"><span class="c6">207 Город Москва – Ховринский</span></p></td><td class="c9" colspan="1" rowspan="1"><p class="c3"><span class="c6">Ульянченко Иван Викторович</span></p></td><td class="c7" colspan="1" rowspan="1"><p class="c3"><span class="c6">КПРФ</span></p></td></tr><tr class="c13"><td class="c1" colspan="1" rowspan="1"><p class="c3"><span class="c6">208 Город Москва – Центральный</span></p></td><td class="c9" colspan="1" rowspan="1"><p class="c3"><span class="c6">Митрохин Сергей Сергеевич</span></p></td><td class="c7" colspan="1" rowspan="1"><p class="c3"><span class="c6">Яблоко</span></p></td></tr><tr class="c4"><td class="c1" colspan="1" rowspan="1"><p class="c3"><span class="c6">209 Город Москва – Черемушкинский</span></p></td><td class="c9" colspan="1" rowspan="1"><p class="c3"><span class="c6">Николаев Игорь Алексеевич</span></p></td><td class="c7" colspan="1" rowspan="1"><p class="c3"><span class="c6">Яблоко</span></p></td></tr><tr class="c13"><td class="c1" colspan="1" rowspan="1"><p class="c3"><span class="c6">210 Город Москва – Чертановский</span></p></td><td class="c9" colspan="1" rowspan="1"><p class="c3"><span class="c6">Таранцов Михаил Александрович</span></p></td><td class="c7" colspan="1" rowspan="1"><p class="c3"><span class="c6">КПРФ</span></p></td></tr></tbody></table>')[0]
umg_list.rename(columns={0:'Okrug', 1:"Candidate", 2:'party'}, inplace=True)
umg_list['umg_flg']=1
df_vybory=candidates_and_uiks.merge(umg_list, on='Candidate', how='left')
vybory = df_vybory.sort_values('value', ascending=False).drop_duplicates(['uik', 'district'])
```
В итоге мы получили датафрейм где, для каждого УИКа мы знаем победил ли на нем кандидат Умного голосования 

### Хранение данных
Все данные будут храниться в базе данных sql 


""")

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

