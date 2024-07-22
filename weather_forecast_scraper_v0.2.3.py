'''
WEATHER FORECAST DATA SCRAPER

Extracts weather forecast data from defined sources:
  - Weather.com (https://www.weather.com/) - webscraped
  - Wunderground (https://www.wunderground.com/) -webscraped
  - Weather.gov (https://forecast.weather.gov) -webscraped
  - Aeris - API queried

Dependencies to Install:
  - selenium 4.10.0
  - webdriver_manager 3.8.6
  - BeatifulSoup4 4.12.2
  - pytz 2022.6
  - pandas 1.5.1

Output:
  - CSV files with data from each source stored in a folder named CSV on the same file path
    where this script will be run. Creates a new folder 'CSV' if it does not yet exists.


Version History:
  - v0.1
    - Start of script history.
    - Browser driver support available for Chrome, (Firefox and IE, to follow)
    - Scraping support for the following websites: Weather.com, Weather.gov, Wunderground, and Aeris
    - Geographic support for data scraping for Miami, FL only
    - Run time @ ~1 min

  - v0.2
    - Expanded geographic support to 41 locations
    - Added multithreading for run time reduction (currently @ ~5.5 mins)
    - Added time scraped columns to the .csv outputs of web scraped data (except Aeris)
    - Added forecast date for Wunderground dataset
    - Total output files: 4 (one per source, with each file containing the collated data per location)
    - Added logging functionality (incomplete)

  - v0.2.1
    - Improved logging
    - Added logic to rescrape URLs that failed.
    - Drop some columns on the Aeris query to reduce output file size.

  - v0.2.2
    - Changed the date format from mm/dd/yyyy to dd/mm/yyy on the data sourced from wunderground and weather.gov

  - v0.2.3
    - Changed the Aeris queries into syncronous process and separated the requerying logic of failed requests into
      a different loop.
'''

import time
import logging
import random
import re
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, wait
import requests
import pandas as pd
from pytz import timezone
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

####################################
## Initial Variables
####################################
now = datetime.now()
now_string = now.strftime("%Y%m%d %H%M")

main_dir = os.path.dirname(__file__)
output_dir = main_dir + '/CSV'
logging_dir = main_dir + '/logs'
os.makedirs(output_dir, exist_ok=True)
os.makedirs(logging_dir, exist_ok=True)

logging.basicConfig(level=logging.WARNING, 
                    filename=logging_dir + f'/webscraper_log_{now_string}.log',
                    filemode='w',
                    format='%(asctime)s :: %(message)s' )

FAILED_URLS       = []
FAILED_URLS_AERIS = []

####################################
## HTML Object Getter Functions
####################################

def initialize_driver(browser='randomize'):
    '''
    Initialize the driver for the browser specified in the argument. Supported browsers are Chrome, Firefox and IE.
    If no arguments are passed, then it assigns a random browser between the supported browsers.

    :param browser: <String> The supported browsers, 'chrome', 'firefox', 'ie
    :return: The driver object of the specified browser.
    '''

    if browser == 'randomize':
        agents = ['chrome', 'firefox', 'ie']
        agent_num = random.randint(0,2)
        browser = agents[agent_num]

    try:
      match browser:
          case 'chrome':
              options = webdriver.ChromeOptions()
              options.add_argument('--headless')
              options.add_argument('--ignore-certificate-errors')
              options.add_argument('--ignore-ssl-errors')
              options.add_argument('--log-level=3')

              driver = webdriver.Chrome(
                        service=ChromeService(ChromeDriverManager().install()),
                        options=options)

          case 'firefox':
              pass
          case 'ie':
              pass
          case _:
              raise ValueError(f'Browser {browser} is not supported.')
    except Exception as e:
        print('[Initialization Error]', e)
        logging.exception('[Initialization Error]')
  
    return driver

def get_html_object(driver, selector, url):

    '''
    Gets the html page source object from a specified URL.
    
    :param driver: the driver object to be used (see initialize_driver)
    :param url: the URL of the webpage source to extract
    :param selector: CSS selector string of the HTML element where data is to be scraped, used to wait for that
    element to load before proceeding to the next steps.
    :return: The driver with the loaded page
    '''

    try:
      driver.get(url)
      wait_driver = WebDriverWait(driver, 30)
      wait_driver.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
    except Exception as e:
      print(f'[Page Load Error {loc}]')
      print('Page Load Timed Out.')
      logging.exception(f'[Load Error {loc}]')

    return driver


#########################################
## Data Extraction Functions
#########################################

def parse_weather_data_wunderground(page):
    '''
    Extracts weather data from a given html document.
    Only works from sources coming from Wunderground.com.

    :param page: The HTML string object
    :return: Returns a pandas dataframe containing the scraped data.
    '''
    content = BeautifulSoup(page, 'html.parser')

    # Get time and date
    format = '%d-%m-%Y %H:%M'
    date = datetime.now(timezone('EST5EDT'))
    date_string = date.strftime(format)

    # Get Forecast Date
    forecast_date_text = content.select_one('#forecast-title-short').text

    # Get table data
    table = content.css.select('#hourly-forecast-table')
    table_data = table[0].find('tbody').find_all('tr')

    forecast_data = []
    for row in table_data:
        data = row.find_all('td')

        forecast_date  = datetime.strptime(forecast_date_text[-5:] + '/2023', '%m/%d/%Y').strftime('%d/%m/%Y')
        forecast_time  = data[0].find('span').text
        forecast_cond  = data[1].find('span').text
        forecast_temp  = data[2].find('span').text
        forecast_feel  = data[3].find('span').text
        forecast_prcpt = data[4].find('span').text
        forecast_amnt  = data[5].find('span').text
        forecast_cloud = data[6].find('span').text
        forecast_dewpt = data[7].find('span').text
        forecast_humid = data[8].find('span').text
        forecast_wind  = data[9].find('span').text
        forecast_pres  = data[10].find('span').text

        data = {
            'date'        : forecast_date,
            'hour'        : forecast_time,
            'condition'   : forecast_cond,
            'temperature' : re.search(r'^[0-9]*', forecast_temp).group(),
            'feels like'  : re.search(r'^[0-9]*', forecast_feel).group(),
            'precip(%)'   : re.search(r'^[0-9]*', forecast_prcpt).group(),
            'amount'      : re.search(r'^[0-9]*(\.[0-9]*)*', forecast_amnt).group(),
            'cloud cover': re.search(r'^[0-9]*', forecast_cloud).group(),
            'dew point'   : re.search(r'^[0-9]*', forecast_dewpt).group(),
            'humidity'    : re.search(r'^[0-9]*', forecast_humid).group(),
            'wind'        : re.search(r'^[0-9]*', forecast_wind).group(),
            'pressure'    : re.search(r'^[0-9]*(\.[0-9]*)*', forecast_pres).group()
        }

        forecast_data.append(data)
    
    forecast_data_df = pd.DataFrame.from_dict(forecast_data)
    forecast_data_df['time_scraped_est_edt'] = date_string

    return forecast_data_df

def parse_weather_data_weather_gov(page):
    '''
    Extracts weather data from a given html document.
    Only works from sources coming from Weather.gov.

    :param page: The HTML string object
    :return: Returns a pandas dataframe containing the scraped data.
    '''

    # Get time and date
    format = '%d-%m-%Y %H:%M'
    date = datetime.now(timezone('EST5EDT'))
    date_string = date.strftime(format)

    #parse data
    content = BeautifulSoup(page, 'html.parser')
    tables = content.css.select('table')
    table_rows = tables[7].css.select('tr')

    forecast_date_1  = [x.text for x in table_rows[1]]
    forecast_hour_1  = [x.text for x in table_rows[2]]
    forecast_temp_1  = [x.text for x in table_rows[3]]
    forecast_dewpt_1 = [x.text for x in table_rows[4]]
    forecast_hindx_1 = [x.text for x in table_rows[5]]
    forecast_swind_1 = [x.text for x in table_rows[6]]
    forecast_wdir_1  = [x.text for x in table_rows[7]]
    forecast_gust_1  = [x.text for x in table_rows[8]]
    forecast_covr_1  = [x.text for x in table_rows[9]]
    forecast_prec_1  = [x.text for x in table_rows[10]]
    forecast_humid_1 = [x.text for x in table_rows[11]]
    forecast_rain_1  = [x.text for x in table_rows[12]]
    forecast_thun_1  = [x.text for x in table_rows[13]]

    forecast_date_2  = [x.text for x in table_rows[15]]
    forecast_hour_2  = [x.text for x in table_rows[16]]
    forecast_temp_2  = [x.text for x in table_rows[17]]
    forecast_dewpt_2 = [x.text for x in table_rows[18]]
    forecast_hindx_2 = [x.text for x in table_rows[19]]
    forecast_swind_2 = [x.text for x in table_rows[20]]
    forecast_wdir_2  = [x.text for x in table_rows[21]]
    forecast_gust_2  = [x.text for x in table_rows[22]]
    forecast_covr_2  = [x.text for x in table_rows[23]]
    forecast_prec_2  = [x.text for x in table_rows[24]]
    forecast_humid_2 = [x.text for x in table_rows[25]]
    forecast_rain_2  = [x.text for x in table_rows[26]]
    forecast_thun_2  = [x.text for x in table_rows[27]]

    forecast_1 = {
        "date"                 : fill_date_weather_gov(forecast_date_1[1:], ''),
        "hour"                 : forecast_hour_1[1:],
        "temperature (F)"      : forecast_temp_1[1:],
        "dew point (F)"        : forecast_dewpt_1[1:],
        "heat index (F)"       : forecast_hindx_1[1:],
        "surface wind (mph)"   : forecast_swind_1[1:],
        "wind dir"             : forecast_wdir_1[1:],
        "gust"                 : forecast_gust_1[1:],
        "sky cover (%)"        : forecast_covr_1[1:],
        "precip potential (%)" : forecast_prec_1[1:],
        "rel humidity (%)"     : forecast_humid_1[1:],
        "rain"                 : forecast_rain_1[1:],
        "thunder"              : forecast_thun_1[1:]
    }

    forecast_2 = {
        "date"                 : fill_date_weather_gov(forecast_date_2[1:], forecast_1['date'][23]),
        "hour"                 : forecast_hour_2[1:],
        "temperature (F)"      : forecast_temp_2[1:],
        "dew point (F)"        : forecast_dewpt_2[1:],
        "heat index (F)"       : forecast_hindx_2[1:],
        "surface wind (mph)"   : forecast_swind_2[1:],
        "wind dir"             : forecast_wdir_2[1:],
        "gust"                 : forecast_gust_2[1:],
        "sky cover (%)"        : forecast_covr_2[1:],
        "precip potential (%)" : forecast_prec_2[1:],
        "rel humidity (%)"     : forecast_humid_2[1:],
        "rain"                 : forecast_rain_2[1:],
        "thunder"              : forecast_thun_2[1:]
    }


    forecast_1_df = pd.DataFrame.from_dict(forecast_1)
    forecast_2_df = pd.DataFrame.from_dict(forecast_2)
    forecast_df = pd.concat([forecast_1_df, forecast_2_df], ignore_index=True)
    forecast_df['time_scraped_est_edt'] = date_string

    return forecast_df

def parse_weather_data_weather_com(page):
    '''
    Extracts weather data from a given html document.
    Only works from sources coming from Weather.com.

    :param page: The HTML string object
    :return: Returns a pandas dataframe containing the scraped data.
    '''
    
    # Get time and date
    format = '%d-%m-%Y %H:%M'
    date = datetime.now(timezone('EST5EDT'))
    date_string = date.strftime(format)

    
    # parse data
    content = BeautifulSoup(page, 'html.parser')

    content = content.css.select('h2#currentDateId0')[0]

    forecast_data = {
        'date'           : [],
        'hour'           : [],
        'temperature (C)': [],
        'condition'      : [],
        'rain (%)'       : [],
        'wind speed'     : [],
        'wind direction' : [],
        'feels like (C)' : [],
        'humidity (%)'   : [],
        'uv index'       : [],
        'cloud cover'    : [],
        'rain amount'    : []
    }

    date = ''
    while content != None:
        if (content.name == 'h2'):
            now = datetime.now(timezone('EST5EDT'))
            date = re.search(r',.*' ,content.text).group()[2:] + ", " + now.strftime('%Y')
            date = datetime.strptime(date, '%B %d, %Y').strftime('%d-%m-%Y')
            
        elif (content.name == 'details'):
            
            details_summary = content.find('div', attrs={'data-testid': True})
            hour = details_summary.find('h3').text
            cond = details_summary.select_one('div[data-testid="wxIcon"] span').text
            temp = details_summary.select_one('div[data-testid="detailsTemperature"] span').text[:-1]
            rpct = details_summary.select_one('div[data-testid="Precip"] span').text[:-1]
            feel = content.select_one('li[data-testid="FeelsLikeSection"] div span[data-testid="TemperatureValue"]').text[:-1]
            wind_content = content.select_one('li[data-testid="WindSection"] div span[data-testid="Wind"]').text
            wind = re.search(r'[0-9]* km/h', wind_content).group()[:-5]
            wdir = re.search(r'^[A-Z]*\s', wind_content).group()[:-1]
            humd = content.select_one('li[data-testid="HumiditySection"] div span[data-testid="PercentageValue"]').text[:-1]
            uvin = content.select_one('li[data-testid="uvIndexSection"] div span[data-testid="UVIndexValue"]').text
            cloud = content.select_one('li[data-testid="CloudCoverSection"] div span[data-testid="PercentageValue"]').text[:-1]
            ramt = content.select_one('li[data-testid="AccumulationSection"] div span[data-testid="AccumulationValue"]').text[:-3]

            forecast_data['date'].append(date)
            forecast_data['hour'].append(hour)
            forecast_data['temperature (C)'].append(temp)
            forecast_data['condition'].append(temp)
            forecast_data['rain (%)'].append(rpct)
            forecast_data['wind speed'].append(wind)
            forecast_data['wind direction'].append(wdir)
            forecast_data['feels like (C)'].append(feel)
            forecast_data['humidity (%)'].append(humd)
            forecast_data['uv index'].append(uvin)
            forecast_data['cloud cover'].append(cloud)
            forecast_data['rain amount'].append(ramt)

        content = content.next_sibling

    forecast_data_df = pd.DataFrame.from_dict(forecast_data)
    forecast_data_df['time_scraped_est_edt'] = date_string

    return forecast_data_df

def parse_weather_data_aeris(data):
    '''
    Transforms the JSON response from the Aeris API to a pandas dataframe.
    Only works from sources coming from Aeris API.

    :param data: The JSON response from the Aeris API request
    :return: Returns a pandas dataframe containing the scraped data.
    '''

    periods = data['response'][0]['periods']
    forecast_data_df = pd.DataFrame.from_dict(periods)

    return forecast_data_df

#############################################
## Helper Functions
#############################################

def click_next_page_wunderground(driver):
    '''
    Simulate a click to a link or a button to navigate to another page and extracts the html
    object on that page. Specifically for Wunderground.com page.

    :param driver: the driver object to be used (see initialize_driver)
    '''
    
    try:
        wait = WebDriverWait(driver, 30)
        wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'div#nextForecasts button.next-day')))
        button = driver.find_element(By.CSS_SELECTOR, 'div#nextForecasts button.next-day')
        driver.execute_script('arguments[0].click();', button)
        time.sleep(7)
    except Exception:
        print(f'[Page Load Error {loc}]')
        print('Page Load Timed Out.')
        logging.exception(f'[Load Error {loc}]')
         
    return(driver)

def fill_date_weather_gov(dates, start_date):
    '''
      Fill in the empty dates on the date array of the weather.gov forecast data.

      :param dates: Date array to be filled
      :param start_date: Initial date value to be used as a filler
      :return: Date array with filled dates
    '''
    fill_date = start_date
    for i, date in enumerate(dates):
        if(dates[i] != ''):
            fill_date = datetime.strptime(dates[i] + '/' + str(datetime.now().year), '%m/%d/%Y').strftime('%d/%m/%Y')
            dates[i] = fill_date
        else:
            dates[i] = fill_date
    
    return dates

######################################
## Scraper Functions
######################################

def scrape_wunderground(url, loc):
    '''
    Main scraping function. This function scrapes data given a URL and a location name.
    This is only for Wunderground.com URL sources.

    :param URL: Source URL from Wunderground.com that contains the hourly forecast data.
    :param loc: location/weather station name
    :return: DataFrame containing the scraped data 
    '''

    return_data = pd.DataFrame()

    try:
        #Wunderground
        print(f'Scraping Wunderground.com on {loc}')

        # print(f'[Wunderground {loc}] Initializing Driver...')
        driver = initialize_driver('chrome')

        # print(f'[Wunderground {loc}] Extracting HTML Object...')
        web_page = get_html_object(driver, 'table#hourly-forecast-table', url)
        # print(f'[Wunderground {loc}] Scraping Data...')
        wunder_today = parse_weather_data_wunderground(web_page.page_source)

        # print(f'[Wunderground {loc}] Extracting HTML Object (Next Page)...')
        web_page_next = click_next_page_wunderground(web_page)
        # print(f'[Wunderground {loc}] Scraping Data')
        wunder_tomorrow = parse_weather_data_wunderground(web_page_next.page_source)

        wunder_data_all = pd.concat([wunder_today, wunder_tomorrow])
        wunder_data_all['WS'] = loc

        return_data = wunder_data_all

    except Exception as e:
        print(f'[Wunderground.com Scraping Error {loc}]', e)
        logging.exception(f'[Scraping Error {loc}]')

        if url not in FAILED_URLS:
            FAILED_URLS.append((url, loc, 'Wunderground.com'))

    return return_data

def scrape_weather_gov(url, loc):
    '''
    Main scraping function. This function scrapes data given a URL and a location name.
    This is only for Weather.gov URL sources.

    :param URL: Source URL from Weather.gov that contains the hourly forecast data.
    :param loc: location/weather station name
    :return: DataFrame containing the scraped data 
    '''

    return_data = pd.DataFrame()

    try:
        # Weather.gov
        print(f'Scraping Weather.gov on {loc}')

        # print(f'[Weather.gov {loc}] Initializing Driver...')
        driver = initialize_driver('chrome')

        # print(f'[Weather.gov {loc}] Extracting HTML Object...')
        web_page = get_html_object(driver, 'table', url)
        # print(f'[Weather.gov {loc}] Scraping data...')
        weather_gov = parse_weather_data_weather_gov(web_page.page_source)
        weather_gov['WS'] = loc

        return_data = weather_gov

    except Exception as e:
        print(f'[Weather.gov Scraping Error {loc}]', e)
        logging.exception(f'[Scraping Error {loc}]')

        if url not in FAILED_URLS:
            FAILED_URLS.append((url, loc, 'Weather.gov'))

    return return_data

def scrape_weather_com(url, loc):
    '''
    Main scraping function. This function scrapes data given a URL and a location name.
    This is only for Weather.com URL sources.

    :param URL: Source URL from Weather.com that contains the hourly forecast data.
    :param loc: location/weather station name
    :return: DataFrame containing the scraped data 
    '''

    return_data = pd.DataFrame()

    try:
        # Weather.com
        print(f'Scraping Weather.com on {loc}')
        # print(f'[Weather.com {loc}] Initializing Driver...')
        driver = initialize_driver('chrome')

        # print(f'[Weather.com {loc}] Extracting HTML Object...')
        web_page = get_html_object(driver, 'h2#currentDateId0', url)
        # print(f'[Weather.com {loc}] Scraping Data...')
        weather_com = parse_weather_data_weather_com(web_page.page_source)
        weather_com['WS'] = loc

        return_data = weather_com

    except Exception as e:
        print(f'[Weather.com Scraping Error {loc}]', e)
        logging.exception(f'[Scraping Error {loc}]')

        if url not in FAILED_URLS:
            FAILED_URLS.append((url, loc, 'Weather.com'))

    return return_data

def scrape_aeris(url_loc):
    '''
    Main scraping function. This function scrapes data given a URL and a location name.
    This is only for Aeris API sources.

    :param url_loc: list of (url, loc) tuples.
    :return: DataFrame containing the scraped data 
    '''

    return_data = []

    # Aeris
    for tup in url_loc:
        url, loc = tup

        try:
            print(f'[Aeris {loc}] Getting Aeris Data...')
            response = requests.get(url, timeout=30)

            print(f'[Aeris {loc}] Transforming JSON to DataFrame...')
            aeris = parse_weather_data_aeris(response.json())
            aeris = aeris[['timestamp',
                          'dateTimeISO',
                          'tempC',
                          'tempF',
                          'feelslikeF',
                          'dewpointF',
                          'humidity',
                          'pressureMB',
                          'pressureIN',
                          'windDir',
                          'windDirDEG',
                          'windSpeedMPH',
                          'windGustMPH',
                          'precipMM',
                          'precipIN',
                          'precipRateMM',
                          'precipRateIN',
                          'pop',
                          'visibilityKM',
                          'visibilityMI',
                          'sky',
                          'weather',
                          'weatherPrimary',
                          'spressureMB',
                          'spressureIN'
                        ]]
            aeris['WS'] = loc
            
            return_data.append(aeris)

        except Exception as e:
            print(f'[Aeris Scraping Error {loc}]', e)
            logging.exception(f'[Aeris Scraping Error {loc}]')

            if url not in FAILED_URLS:
                FAILED_URLS_AERIS.append((url, loc, 'Aeris'))
    
        # time.sleep(1)

    return return_data


#########################################
## Main
#########################################

if __name__ == '__main__':
    init_drive = initialize_driver(browser='chrome')
    start = time.perf_counter()
    loc_df = pd.read_excel(main_dir + '/Assets/loc_v0.2.xlsx')
    urls = list(loc_df[['Wunderground.com', 'Weather.gov', 'Weather.com', 'WS']].itertuples(index=False))

    weather_data_wunderground = []
    weather_data_weather_gov = []
    weather_data_weather_com = []
    weather_data_aeris = []

    with ThreadPoolExecutor(max_workers=12) as executor:

        for wunderground_url, weather_gov_url, weather_com_url, loc in urls:
            weather_data_wunderground.append(
                executor.submit(scrape_wunderground, wunderground_url, loc)
            )
            weather_data_weather_gov.append(
                executor.submit(scrape_weather_gov, weather_gov_url, loc)
            )
            weather_data_weather_com.append(
                executor.submit(scrape_weather_com, weather_com_url, loc)
            )

            time.sleep(7)

        weather_data_aeris = scrape_aeris(list(loc_df[['Aeris', 'WS']].itertuples(index=False)))
        

    wait(weather_data_wunderground)
    wait(weather_data_weather_gov)
    wait(weather_data_weather_com)
    # wait(weather_data_aeris)

    weather_data_wunderground_results = [x.result() for x in weather_data_wunderground if isinstance(x.result(), pd.DataFrame)]
    weather_data_weather_gov_results  = [x.result() for x in weather_data_weather_gov if isinstance(x.result(), pd.DataFrame)]
    weather_data_weather_com_results  = [x.result() for x in weather_data_weather_com if isinstance(x.result(), pd.DataFrame)]
    weather_data_aeris_results = weather_data_aeris

    print(f'Rescraping {len(FAILED_URLS)} URLs')
    logging.warning(f'There are {len(FAILED_URLS)} failed URLs')

    rescrape_count = 0
    while FAILED_URLS and rescrape_count < 100:
        rescrape_count += 1
        try: 
            url_to_scrape, loc, source = FAILED_URLS.pop(0)
            
            match source:
                case 'Wunderground.com':
                    weather_data_wunderground_results.append(scrape_wunderground(url_to_scrape, loc))
                case 'Weather.gov':
                    weather_data_weather_gov_results.append(scrape_weather_gov(url_to_scrape, loc))
                case 'Weather.com':
                    weather_data_weather_com_results.append(scrape_weather_com(url_to_scrape, loc))

        except Exception as exc:
            print(f'[Re-scraping Error {source} {loc}]', exc)
            logging.exception(f'[Re-scraping Error {source} {loc}]')

            if url_to_scrape not in FAILED_URLS:
                FAILED_URLS.append((url_to_scrape, loc, source))

        time.sleep(7)

    print(f'Rescraping {len(FAILED_URLS_AERIS)} failed Aeris URLs')
    logging.warning(f'There are {len(FAILED_URLS_AERIS)} failed Aeris URLs')
    
    rescrape_count = 0
    while FAILED_URLS_AERIS and rescrape_count < 100:
        try:
            url_to_scrape, loc, source = FAILED_URLS_AERIS.pop(0)

            rescrape_result = scrape_aeris([(url_to_scrape, loc)])

            weather_data_aeris_results.append(rescrape_result)

        except Exception as exc:
            print(f'[Re-scraping Error {source} {loc}]', exc)
            logging.exception(f'[Re-scraping Error {source} {loc}]')

            if url_to_scrape not in FAILED_URLS:
                FAILED_URLS.append((url_to_scrape, loc, source))
    

    try:
        print('[Wunderground] Saving Output to CSV...')
        weather_data_wunderground_df = pd.concat(weather_data_wunderground_results)
        weather_data_wunderground_df.to_csv(output_dir + f'/wunderground_forecast_{now_string}.csv', index=False)
        print(f'[Wunderground] Saved to "wunderground_forecast_{now_string}.csv"\n\n')
        logging.warning(f'Saved {len(weather_data_wunderground_df["WS"].unique())} weather stations')


        print('[Weather.gov] Saving Output to CSV...')
        weather_data_weather_gov_df = pd.concat(weather_data_weather_gov_results)
        weather_data_weather_gov_df.to_csv(output_dir + f'/weather_gov_forecast_{now_string}.csv', index=False)
        print(f'[Weather.gov] Saved to "weather_gov_forecast_{now_string}.csv"\n\n')
        logging.warning(f'Saved {len(weather_data_weather_gov_df["WS"].unique())} weather stations')

        print('[Weather.com] Saving Output to CSV...')
        weather_data_weather_com_df = pd.concat(weather_data_weather_com_results)
        weather_data_weather_com_df.to_csv(output_dir + f'/weather_com_forecast_{now_string}.csv', index=False)
        print(f'[Weather.com] Saved to "weather_com_forecast_{now_string}.csv"\n\n')
        logging.warning(f'Saved {len(weather_data_weather_com_df["WS"].unique())} weather stations')

        print('[Aeris] Saving Output to CSV...')
        weather_data_aeris_df = pd.concat([x for x in weather_data_aeris_results if isinstance(x, pd.DataFrame)])
        weather_data_aeris_df.to_csv(output_dir + f'/aeris_forecast_{now_string}.csv', index=False)
        print(f'[Aeris] Saved to "aeris_forecast_{now_string}.csv"\n\n')
        logging.warning(f'Saved {len(weather_data_aeris_df["WS"].unique())} weather stations')

    except Exception as exc:
        print('[SaveError]', exc)
        logging.exception('[Error Saving Files to CSV]')

    print(f'Script execution runtime: {(time.perf_counter() - start)/60} mins')
    logging.warning(f'Script execution runtime: {(time.perf_counter() - start)/60} mins')

