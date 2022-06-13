import streamlit as st
import psycopg2


st.set_page_config(layout="wide", page_title='Введение и получение данных')

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
st.video('https://youtu.be/u8uD3s_Z1iI')
st.markdown("""
##### Оптимальный вариант
Когда мы пишем в ЦИК адрес он обращается к своему api и делает запрос, чтобы получить участок, однако
к сожалению получилось так, что через него не получается достать уик
, так как там стоит защита от ддос атак и он не дает подключиться модулю request и банит по ip

##### Не очень оптимальный, но хотя бы рабочий вариант
В итоге пришлось делать так. Через **недокументированное api** сайта мосгоризберкома, который не банит модуль requests
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
Все данные будут храниться в базе данных postgresql от supabase подробнее о том, как оттуда выгражаются данные 
на следующей странице 

""")


