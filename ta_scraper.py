# -*- coding: utf-8 -*-

# pylint: disable=fixme
# pylint: disable=redefined-outer-name
# pylint: disable=W0702
# pylint: disable=line-too-long
# pylint: disable=invalid-name
# pylint: disable=E1123

'''
    scraper class for scraping tripadvisor website for reviews and images
'''

__author__ = 'arka'

__license__ = "MIT"
__version__ = "1.1.0"
__maintainer__ = "Arkaprava Ghosh"
__email__ = "arkaprava.mail@gmail.com"
__status__ = "Development"

from datetime import datetime
import os
import sys
import time
import logging
import re
from urllib.parse import urlencode, urlsplit, parse_qs
import requests
import json
import csv
import math
from operator import itemgetter


import selenium
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.keys import Keys
from subprocess import CREATE_NO_WINDOW

from webdriver_manager.chrome import ChromeDriverManager

from pyjsparser import parse 

from db import DBManager

from qgis.PyQt.QtCore import QObject, pyqtSignal

from vector import Vector3d


DRIVER_VERSION = 107


op = webdriver.ChromeOptions()

op.add_argument('--headless')
op.add_argument('--ignore-certificate-errors-spki-list')
op.add_argument('--ignore-ssl-errors')
op.add_argument('--log-level=3')
op.add_experimental_option("excludeSwitches", ["enable-logging"])
op.add_argument('--disable-gpu')
op.add_argument('--no-sandbox')
op.add_argument("--disable-extensions")
op.add_experimental_option("useAutomationExtension", False)
op.add_argument("--proxy-server='direct://'")
op.add_argument("--proxy-bypass-list=*")
op.add_argument("--start-maximized")




class TAapi(QObject):
    finished = pyqtSignal(list)
    progress = pyqtSignal(int)
    addMessage = pyqtSignal(str)
    addError = pyqtSignal(str)
    total = pyqtSignal(int)
    apiUsage = pyqtSignal(int, float)

    __BASE_URL__ = 'https://www.tripadvisor.in'
    __MAPS_BASE_URL__ = 'https://maps.googleapis.com/maps/api/place/findplacefromtext/json'

    __SEARCH_RESULTS_APPEAR_WAIT__ = 20
    __SEARCH_BOX_APPEAR_WAIT__ = 10
    __SEARCH_FILTER_APPEAR_WAIT__ = 10
    __REVIEW_TAB_APPEAR_WAIT__ = 10
    __SCROLL_PAUSE_TIME__ = 5
    __CLICKABLE_WAIT_TIME__ = 5
    __IMAGE_LOAD_WAIT__ = 5

    __PLACES_API_BILLING_RATE__ = 17
    __PLACES_API_MONTHLY_ALLOWANCE__ = 200

    __IMAGES_MAX_RES__ = 2400

    PLACES_MAX = 5
    REVIEWS_MAX = 50

    EARTH_RADIUS = 6_371_000

    def __init__(self, location, lat, lng, radius, apiKey, dbName, tableName, maxPlaces, maxReviews, csvFilePath):
        QObject.__init__(self)
        
        self.location = location
        self.lat = lat
        self.lng = lng
        self.radius = radius

        self.apiKey = apiKey

        self.dbName = dbName
        self.tableName = tableName

        self.PLACES_MAX = maxPlaces
        self.REVIEWS_MAX = maxReviews

        self.PLACES_SO_FAR = 0
        self.REVIEWS_SO_FAR = 0

        self.csvFilePath = csvFilePath

        self.running = None

        # override logger to add signalling 
        class SignalLogger(logging.Logger):
            def __init__(self, name, level=logging.INFO):
                super().__init__(name, level)

            def _set_outer_instance(self, instance):
                self._outer_instance = instance

            def info(self, msg, *args, **kwargs):
                super().info(msg, *args, **kwargs)
                self._outer_instance.addMessage.emit(str(msg))

            def warning(self, msg, *args, **kwargs):
                super().warning(msg, *args, **kwargs)
                self._outer_instance.addMessage.emit(str(msg))

            def error(self, msg, *args, **kwargs):
                super().error(msg, *args, **kwargs)
                self._outer_instance.addError.emit(str(msg)) 

        logging.setLoggerClass(SignalLogger)
        logFilePath = os.path.normpath(os.path.join(os.path.dirname(__file__), ".log"))

        logging.basicConfig(
            filename=logFilePath,
            format='%(asctime)s - %(levelname)s - %(message)s',
            filemode='a',
            level=logging.INFO
        )

        try:
            os.system(f"attrib +h {logFilePath}")
        except:
            pass

        self.logger = logging.getLogger('tripadvisor')
        self.logger._set_outer_instance(self)

        # create a db manager instance
        try:
            self.dbm = DBManager(self.dbName, self.tableName, logging=self.logger)
        except:
            self.logger.error("mongodb error", exc_info=True)

        # load local variables
        self.localVars = self.__read_local_vars()

        if self.localVars is not None and 'MAPS_API_USAGE' in self.localVars:
            self.mapsApiUsage = int(self.localVars['MAPS_API_USAGE'])

            # reset api usage to 0 every month
            lastMonth = int(self.localVars['TIMESTAMP'].strip().split('-')[1])
            currMonth = datetime.now().month
            if lastMonth != currMonth:
                self.mapsApiUsage = 0
        else:
            self.mapsApiUsage = 0


        # select best version for selenium webdriver
        # TODO: fix this in future updates

        self.selenium_version = self.__get_selenium_version()
        self.driver_version =DRIVER_VERSION

        try:
            if self.selenium_version == 3:
                self.driver = webdriver.Chrome(executable_path=os.path.join(os.path.dirname(__file__), 'exe', f"chromedriver_v{self.driver_version}.exe"), options=op)
            elif self.selenium_version == 4:
                chromeService = Service(os.path.join(os.path.dirname(__file__), 'exe', f"chromedriver_v{self.driver_version}.exe"))
                chromeService.creationflags = CREATE_NO_WINDOW
                self.driver = webdriver.Chrome(service=chromeService, options=op)
            browser_version = self.__get_browser_version()

            if browser_version > self.driver_version:
                self.driver.close()
                self.driver.quit()

                self.driver_version = browser_version
                # op.arguments.remove('--headless')
                
                if self.selenium_version == 3:
                    self.driver = webdriver.Chrome(executable_path=os.path.join(os.path.dirname(__file__), 'exe', f"chromedriver_v{self.driver_version}.exe"), options=op)
                elif self.selenium_version == 4:
                    chromeService = Service(os.path.join(os.path.dirname(__file__), 'exe', f"chromedriver_v{self.driver_version}.exe"))
                    chromeService.creationflags = CREATE_NO_WINDOW
                    self.driver = webdriver.Chrome(service=chromeService, options=op)
        except:
            self.__cleanup()
            self.logger.error("error loading chromedriver", exc_info=True)
        else:
            self.logger.info(f"loaded chromedriver version {self.driver_version}")

        # self.selenium_version = self.__get_selenium_version()

        # try:
        #     if self.selenium_version == 3:
        #         self.driver = webdriver.Chrome(
        #             ChromeDriverManager().install(), 
        #             options=op
        #         )
        #     elif self.selenium_version == 4:
        #         chromeService = Service(ChromeDriverManager().install())
        #         chromeService.creationflags = CREATE_NO_WINDOW
        #         self.driver = webdriver.Chrome(service=chromeService, options=op)
        # except:
        #     self.__cleanup()
        #     self.logger.error("error loading chromedriver", exc_info=True)
        # else:
        #     self.logger.info(f"loaded chromedriver version")


    def __cleanup(self):
        self.logger.info("cleaning up")

        # store local vars
        self.__store_local_vars()

        # quit driver
        if hasattr(self, 'driver'):
            self.driver.close()
            self.driver.quit()
            del self.driver

        # kill any stray chromedriver instances forcefully
        if hasattr(self, 'driver_version'):
            os.system(f"taskkill /IM chromedriver_v{self.driver_version}.exe /F")
        else:
            os.system(f"taskkill /IM chromedriver.exe /F")

    def __read_local_vars(self):
        localFilePath = os.path.normpath(os.path.join(os.path.dirname(__file__), "scraper.dat"))

        if os.path.exists(localFilePath) and os.path.isfile(localFilePath):
            with open(localFilePath, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                return {key: val for key, val in [line.strip('\n').split('=') for line in lines]}
        else:
            return dict()
    
    def __store_local_vars(self):
        localFilePath = os.path.normpath(os.path.join(os.path.dirname(__file__), "scraper.dat"))
        self.localVars['TIMESTAMP'] = datetime.now().strftime("%d-%m-%Y")

        '''
            hidden files cannot be read, since createFile system call
            does support hidden flag but c bindings for open() in python 
            don't provide us a way to send those flags as parameters
        '''
        # unhide file
        try:
            os.system(f"attrib -h {localFilePath}")
        except:
            pass

        with open(localFilePath, 'w') as f:
            f.write("\n".join([f"{key}={val}" for key, val in self.localVars.items()]))
            # hide file
            try:
                os.system(f"attrib +h {localFilePath}")
            except:
                pass

    def __get_selenium_version(self):
        version = selenium.__version__
        return int(version.split('.', maxsplit=1)[0])

    def __get_browser_version(self):
        if 'browserVersion' in self.driver.capabilities:
            version = self.driver.capabilities['browserVersion']
        else:
            version = self.driver.capabilities['version']

        return int(version.split('.')[0])

    def __scroll_to_end(self):
        # Get scroll height
        last_height = self.driver.execute_script("return document.body.scrollHeight")

        while True:
            # Scroll down to bottom
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

            # Wait to load page
            time.sleep(self.__SCROLL_PAUSE_TIME__)

            # Calculate new scroll height and compare with last scroll height
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

    def __scroll_to_elem(self, elem):
        try:
            self.driver.execute_script(
                "arguments[0].scrollIntoView(true);",
                elem
            )
        except:
            # self.logger.warning("error in scrolling to element", exc_info=True)
            pass

    def __scrape_places_content(self, page=1):
        if self.PLACES_SO_FAR > self.PLACES_MAX:
            self.logger.warning('result search exceeded max pages limit. aborting search.')
            return []

        self.__scroll_to_end()

        # Checkpoint 4
        if not self.running:
            self.__halt_error()
            return

        try:
            WebDriverWait(self.driver, self.__SEARCH_RESULTS_APPEAR_WAIT__) \
                .until(EC.presence_of_all_elements_located((By.XPATH, "//div[contains(@class, 'result-card')]")))
        except:
            self.logger.warning("Loading results took too much time. Aborting.", exc_info=True)
            sys.exit()
        else:
            place_results_contents = self.driver.find_elements(by=By.XPATH, value="//div[contains(@class, 'result-content-columns')]") 
            place_results_urls = [
                {
                    'name': place_results_content.find_element(by=By.XPATH, value=".//div[@class='result-title']/span[1]").text,
                    'url': self.__BASE_URL__ + \
                            parse(place_results_content.get_attribute('onclick'))['body'][0]['expression']['arguments'][3]['value'],
                    'page': page
                }for place_results_content in place_results_contents
            ]
            self.PLACES_SO_FAR += len(place_results_urls)

        # Checkpoint 5
        if not self.running:
            self.__halt_error()
            return

        try:
            pagination = self.driver.find_element(by=By.XPATH, value="//div[contains(@class, 'ui_pagination')]")
            next_button = WebDriverWait(pagination, self.__CLICKABLE_WAIT_TIME__) \
                .until(EC.element_to_be_clickable((By.XPATH, ".//a[contains(@class, 'ui_button nav next')]")))

            if next_button.is_enabled():
                next_page = next_button.get_attribute('data-page')
                if next_page is not None:
                    self.logger.info(f"going to page {next_page} of results")
                    self.driver.execute_script(
                        "arguments[0].click();",
                        next_button
                    )
                    return place_results_urls + self.__scrape_places_content(page=page+1)
                else:
                    return place_results_urls
            else:
                # self.logger.info("next page button not clickable")
                return place_results_urls
        except:
            # self.logger.info("no pagination available.", exc_info=True)
            return place_results_urls

    def __scrape_review_for_images_things(self, container):
        # Checkpoint 11
        if not self.running:
            self.__halt_error()
            return []
        try:
            image_container = container.find_element(by=By.XPATH, value=".//div[contains(@class, 'pDrIj')]")
            self.__scroll_to_elem(image_container)

            image_elems = WebDriverWait(image_container, self.__IMAGE_LOAD_WAIT__) \
                .until(EC.presence_of_all_elements_located((By.TAG_NAME, "img")))

            sources = [self.__upgrade_image_url(image_elem.get_attribute('src')) for image_elem in image_elems]
        except:
            # self.logger.warning("image either not available or error loading image.")
            return []
        else:
            return sources

    def __scrape_review_for_images_places(self, container):
        # Checkpoint 11
        if not self.running:
            self.__halt_error()
            return []
        try:
            image_container = container.find_element(by=By.XPATH, value=".//div[contains(@class, 'LblVz')]")
            self.__scroll_to_elem(image_container)

            image_elems = WebDriverWait(image_container, self.__IMAGE_LOAD_WAIT__) \
                .until(EC.presence_of_all_elements_located((By.TAG_NAME, "img")))

            sources = [self.__upgrade_image_url(image_elem.get_attribute('src')) for image_elem in image_elems]
        except:
            # self.logger.warning("image either not available or error loading image.")
            return []
        else:
            return sources

    def __scrape_review_for_text_things(self, container):
        # Checkpoint 10
        if not self.running:
            self.__halt_error()
            return dict()

        # find rating
        try:
            rating_ui = container.find_element(by=By.XPATH, value=".//span[contains(@class, 'ui_bubble_rating')]")
            class_list = rating_ui.get_attribute('class')
            rating_class = re.findall('bubble_[0-9]+', class_list)

            if len(rating_class) > 0:
                rating = int(rating_class[0][len('bubble_'):])
            else:
                rating = None
        except:
            rating = None

        # review text
        try:
            text = container.find_element(by=By.XPATH, value=".//div[contains(@class, 'fIrGe')]/q/span").text
            text = text.replace("\n", " ")
        except:
            text = None

        # review title
        try:
            title = container.find_element(by=By.XPATH, value=".//div[contains(@class, 'KgQgP')]/a/span/span").text
        except:
            title = None

        # review date
        try:
            date = container.find_element(by=By.XPATH, value=".//span[contains(@class, 'teHYY')]").text[len("Date of experience:"):]
            month, year = date.strip().split()
            year = int(year.strip())
        except:
            month, year = None, None

        return {
            'rating': rating,
            'title': title,
            'text': text,
            'month': month,
            'year': year
        }

    def __scrape_review_for_text_places(self, container):
        # Checkpoint 10
        if not self.running:
            self.__halt_error()
            return dict()
        
        # find rating
        try:
            rating_ui = container.find_element(by=By.XPATH, value=".//*[name()='svg'][contains(@class, 'UctUV')]")
            rating = rating_ui.get_attribute('aria-label')
            rating = int(float(rating[:3]) * 10)
        except:
            rating = None

        # review text
        try:
            text = container.find_element(by=By.XPATH, value=".//div[contains(@class, 'biGQs _P pZUbB KxBGd')]/span").text
            text = text.replace("\n", " ")
        except:
            text = None

        # review title
        try:
            title = container.find_element(by=By.XPATH, value=".//div[contains(@class, 'biGQs _P fiohW qWPrE ncFvv fOtGX')]/a/span").text
        except:
            title = None

        # review date
        try:
            date = container.find_element(by=By.XPATH, value=".//div[contains(@class, 'TreSq')]/div").text[len("Written"):]
            date, month, year = date.strip().split()
            date = int(date.strip())
            month = int(month.strip())
            year = int(year.strip())
        except:
            date, month, year = None, None, None

        return {
            'rating': rating,
            'title': title,
            'text': text,
            'date': date,
            'month': month,
            'year': year
        }

    def __scrape_reviews_things(self, page=1):
        if self.REVIEWS_SO_FAR > self.REVIEWS_MAX:
            self.logger.warning('reviews search exceeded max pages limit. aborting search.')
            return []

        self.__scroll_to_end()

        # Checkpoint 8
        if not self.running:
            self.__halt_error()
            return []

        try:
            reviewTab = WebDriverWait(self.driver, self.__REVIEW_TAB_APPEAR_WAIT__) \
                .until(EC.presence_of_element_located((By.XPATH, "//div[@class='FTCTN']")))
        except:
            self.logger.warning("could not load reviews. aborting.", exc_info=True)
            return []
        else:
            # Checkpoint 9
            if not self.running:
                self.__halt_error()
                return []

            try:
                WebDriverWait(reviewTab, self.__REVIEW_TAB_APPEAR_WAIT__) \
                    .until(EC.presence_of_all_elements_located((By.XPATH, ".//div[contains(@class, 'lgfjP')]")))
            except:
                self.logger.warning("could not load reviews. aborting.", exc_info=True)
                return []
            else:
                reviewContainers = reviewTab.find_elements(by=By.XPATH, value=".//div[contains(@class, 'lgfjP')]")

                # expand all read more buttons
                for reviewContainer in reviewContainers:
                    read_more_btn = reviewContainer.find_element(by=By.XPATH, value="//span[contains(@class, 'Ignyf')]")
                    if 'more' in read_more_btn.text and read_more_btn.is_enabled():
                        self.driver.execute_script(
                            "arguments[0].click();",
                            read_more_btn
                        )
                
                # scrape review title, body and date and images if any
                place_reviews = [
                    {
                        'metadata': self.__scrape_review_for_text_things(reviewContainer),
                        'images': self.__scrape_review_for_images_things(reviewContainer)
                    }
                    for reviewContainer in list(reviewContainers)
                ]
                self.REVIEWS_SO_FAR += len(place_reviews)

                # Checkpoint 12
                if not self.running:
                    self.__halt_error()
                    return []

                # go to next page
                try:
                    pagination = reviewTab.find_element(by=By.XPATH, value="//div[contains(@class, 'ui_pagination')]")
                    next_button = WebDriverWait(pagination, self.__CLICKABLE_WAIT_TIME__) \
                        .until(EC.element_to_be_clickable((By.XPATH, "//*[contains(@class, 'ui_button nav next')]")))

                    if next_button.is_enabled():
                        if next_button.tag_name == 'a':
                            self.driver.execute_script(
                                "arguments[0].click();",
                                next_button
                            )
                            return place_reviews + self.__scrape_reviews_things(page=page+1)
                        else:
                            return place_reviews
                    else:
                        # self.logger.info("next page button not clickable")
                        return place_reviews
                except:
                    # self.logger.info("no pagination available.", exc_info=True)
                    return place_reviews

    def __scrape_reviews_places(self, page=1):
        if self.REVIEWS_SO_FAR > self.REVIEWS_MAX:
            self.logger.warning('reviews search exceeded max pages limit. aborting search.')
            return []

        self.__scroll_to_end()

        # Checkpoint 8
        if not self.running:
            self.__halt_error()
            return []

        try:
            reviewTab = WebDriverWait(self.driver, self.__REVIEW_TAB_APPEAR_WAIT__) \
                .until(EC.presence_of_element_located((By.XPATH, "//div[@class='LbPSX']")))
        except:
            self.logger.warning("could not load reviews. aborting.", exc_info=True)
            return []
        else:
            try:
                reviewContainers = reviewTab.find_elements(by=By.XPATH, value="./div")
            except:
                self.logger.warning("could not load reviews. aborting.", exc_info=True)
                return []
            else:
                pagination = reviewContainers[-1]
                reviewContainers = reviewContainers[:-1]

                # expand all read more buttons
                for reviewContainer in reviewContainers:
                    read_more_btn = reviewContainer.find_element(by=By.XPATH, value="//div[contains(@class, 'lszDU')]/button/span")
                    if 'more' in read_more_btn.text:
                        self.driver.execute_script(
                            "arguments[0].click();",
                            read_more_btn
                        )

                # scrape review title, body and date and images if any
                place_reviews = [
                    {
                        'metadata': self.__scrape_review_for_text_places(reviewContainer),
                        'images': self.__scrape_review_for_images_places(reviewContainer)
                    }
                    for reviewContainer in list(reviewContainers)
                ]
                self.REVIEWS_SO_FAR += len(place_reviews)

                # Checkpoint 12
                if not self.running:
                    self.__halt_error()
                    return []

                # go to next page
                try:
                    next_button = WebDriverWait(pagination, self.__CLICKABLE_WAIT_TIME__) \
                        .until(EC.element_to_be_clickable((By.XPATH, "//div[contains(@class, 'UCacc')]/a")))

                    if next_button.get_attribute('aria-label') == 'Next page':
                        self.driver.execute_script(
                            "arguments[0].click();",
                            next_button
                        )
                        return place_reviews + self.__scrape_reviews_places(page=page+1)
                    else:
                        # self.logger.info("next page button not clickable")
                        return place_reviews
                except:
                    # self.logger.info("no pagination available.", exc_info=True)
                    return place_reviews

    def __upgrade_image_url(self, url):
        try:
            split = urlsplit(url)
            queryParams = dict(parse_qs(split.query))

            for key, val in queryParams.items():
                queryParams[key] = val[0]
            
            if 'w' in queryParams:
                queryParams['w'] = self.__IMAGES_MAX_RES__
                return f"{split.scheme}://{split.netloc}{split.path}?{urlencode(queryParams)}"
            else:
                return url
        except:
            return url 

    def __get_coords(self, place):
        # Checkpoint 13
        if not self.running:
            self.__halt_error()
            return None, None

        queryFields = ['geometry', 'place_id']
        pathParams = {
            'input': place,
            'inputtype': 'textquery',
            'fields': ','.join(queryFields),
            'locationbias': f"circle:{self.radius}@{self.lat},{self.lng}",
            'key': self.apiKey
        }
        headers = {}

        url = self.__MAPS_BASE_URL__ + '?' + urlencode(pathParams)

        try:
            res = requests.get(url, headers=headers)
            data = res.json()

            # update usage
            self.mapsApiUsage += 1

            # check bill
            bill = self.__PLACES_API_BILLING_RATE__ * self.mapsApiUsage / 1000
            if bill > self.__PLACES_API_MONTHLY_ALLOWANCE__:
                self.addError.emit("places API allowance exceeded")

            self.localVars['MAPS_API_USAGE'] = self.mapsApiUsage
            self.apiUsage.emit(self.mapsApiUsage, bill)


            if data['status'] == "OK":
                candidate = data["candidates"][0]
                geometry = candidate["geometry"]
                place_id = candidate['place_id']

                self.logger.info(f"fetching coordinates for {place}")

                return geometry["location"], place_id
            else:
                return None, None
        except:
            self.logger.warning("error fetching location info", exc_info=True)
            return None, None

    def __clean_reviews(self, review):
        return all([val is not None for val in itemgetter('title', 'text', 'month', 'year')(review['metadata'])])

    def __filter_results_coords(self, result):
        '''
            using spherical cosines formula applied on a spherical geodesic as the approximate distance
            could have used haversine's formula (a bit more numerically robust but theoretically equivalent)
            does not give exact distance for long distances due to earth being an oblate spheroid
        '''
        lat, lng = itemgetter('lat', 'lng')(result['coords'])

        lat1, lng1 = self.lat * math.pi / 180, self.lng * math.pi / 180
        lat2, lng2 = lat * math.pi / 180, lng * math.pi / 180

        # calculate distance from origin
        # step 1: convert to cartesian coordinates
        pos1 = Vector3d(self.EARTH_RADIUS * math.cos(lat1) * math.cos(lng1), self.EARTH_RADIUS * math.cos(lat1) * math.sin(lng1), self.EARTH_RADIUS * math.sin(lat1))
        pos2 = Vector3d(self.EARTH_RADIUS * math.cos(lat2) * math.cos(lng2), self.EARTH_RADIUS * math.cos(lat2) * math.sin(lng2), self.EARTH_RADIUS * math.sin(lat2))

        # step 2: calculate real angle
        real_angle = pos1.angle(pos2)

        # d = self.EARTH_RADIUS * real_angle (works pretty good as an approximation)
        spherical_distance = self.EARTH_RADIUS * real_angle

        # allow 10% tolerance while clipping radius
        return spherical_distance <= self.radius * 1.1

    def __clean_results(self, result):
        # filter based on coordinates
        reviews = list(filter(self.__filter_results_coords, reviews))

        name, url, reviews = itemgetter('name', 'url', 'reviews')(result)
        reviews = list(filter(self.__clean_reviews, reviews))

        return len(reviews) > 0 and \
                all([val is not None for val in (name, url)]) and \
                all([len(val) != 0 for val in (name, url)])

    def __scrape(self):
        try:
            self.driver.get(self.__BASE_URL__)
        except:
            self.__cleanup()
            self.logger.error(f"chromedriver faced error while loading {self.__BASE_URL__}")

        # Checkpoint 1
        if not self.running:
            self.__halt_error()
            return

        # type the location in the search bar
        try:
            searchBar = WebDriverWait(self.driver, self.__SEARCH_BOX_APPEAR_WAIT__) \
                .until(EC.presence_of_element_located((By.XPATH, "//input[contains(@class, 'qjfqs')][@placeholder='Where to?']"))) 
        except:
            self.logger.warning(f"/GET {self.__BASE_URL__} - Loading took too much time. Aborting.", exc_info=True)
        else:
            searchBar.send_keys(self.location)
            searchBar.send_keys(Keys.ENTER)

        # Checkpoint 2
        if not self.running:
            self.__halt_error()
            return

        # switch to the things-to-do tab
        try:
            searchFilterList = WebDriverWait(self.driver, self.__SEARCH_FILTER_APPEAR_WAIT__) \
                .until(EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'ui_tabs')][@id='search-filters']/ul[1]")))
        except:
            self.logger.warning(f"/GET {self.driver.current_url} - Loading took too much time. Aborting.", exc_info=True)
        else:
            filters = searchFilterList.find_elements(by=By.XPATH, value=".//li/a")
            search_filter = list(filter(lambda a: a.text == 'Things to do', filters))[0]
            self.driver.execute_script("arguments[0].click();", search_filter)

        # Checkpoint 3
        if not self.running:
            self.__halt_error()
            return

        place_results = self.__scrape_places_content()

        # TODO: remove debug statements in prod
        # place_results = [
        #     {
        #         'name': 'Darjeeling Tea Garden',
        #         'url': 'https://www.tripadvisor.in/Attraction_Review-g304557-d3705683-Reviews-Tea_Garden-Darjeeling_Darjeeling_District_West_Bengal.html',
        #         'page': 1
        #     },
        #     {
        #         'name': 'Darjeeling Toy Train', 
        #         'url': 'https://www.tripadvisor.in/Attraction_Review-g304557-d3171246-Reviews-Darjeeling_Toy_Train-Darjeeling_Darjeeling_District_West_Bengal.html', 
        #         'page': 1
        #     }
        # ]

        
        self.logger.info(f"{len(place_results)} results loaded")
        self.total.emit(len(place_results))

        results = []

        # Checkpoint 6
        if not self.running:
            self.__halt_error()
            return

        for place_result in place_results:
            name = place_result['name']
            url = place_result['url']
            page = place_result['page']

            # Checkpoint 7
            if not self.running:
                self.__halt_error()
                return

            # check cache for url (unique id)
            result = self.dbm.query(url)
            if result is not None:
                mode = result['mode']
                reviews = result['reviews']
                place_id = result['place_id']
                geometry = result['coords']

                result.pop('_id', None)
            else:
                mode = 'place'

                try:
                    self.driver.get(url)
                except:
                    self.__cleanup()
                    self.logger.error(f"chromedriver faced error while loading {url}")

                try:
                    WebDriverWait(self.driver, self.__REVIEW_TAB_APPEAR_WAIT__) \
                        .until(EC.presence_of_element_located((By.XPATH, "//div[@class='FTCTN']")))
                except:
                    try:
                        WebDriverWait(self.driver, self.__REVIEW_TAB_APPEAR_WAIT__) \
                            .until(EC.presence_of_element_located((By.XPATH, "//div[@class='LbPSX']")))
                    except:
                        self.logger.warning(f"/GET {self.__BASE_URL__} - Loading took too much time. Aborting.", exc_info=True)
                        reviews = []
                    else:
                        reviews = self.__scrape_reviews_places()
                else:
                    reviews = self.__scrape_reviews_things()
                    mode = 'todo'

                geometry, place_id = self.__get_coords(name)

            place_result['reviews'] = reviews
            place_result['mode'] = mode

            place_result['place_id'] = place_id
            place_result['coords'] = geometry

            self.logger.info(f"{name}: {len(reviews)} downloaded")

            results.append(place_result)

            self.progress.emit(len(results))

        # print(results)

        # self.logger.info("dumping data to results.json")
        # json.dump({'results': results}, open(os.path.join(os.path.dirname(__file__), "results.json"), 'w'), indent = 4)
        
        # cleanup results
        results = list(filter(self.__clean_results, results))

        # Checkpoint 14
        if not self.running:
            self.__halt_error()
            return []

        self.logger.info("inserting data to mongodb...")
        self.dbm.insert(results)
        self.logger.info("inserted data to mongodb")

        # Checkpoint 15
        if not self.running:
            self.__halt_error()
            return []

        with open(self.csvFilePath, 'w') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'name', 
                'url', 
                'page', 
                'rating', 
                'title', 
                'text', 
                'date', 
                'month', 
                'year', 
                'mode', 
                'place_id'
            ])
            writer.writeheader()

            for result in results:
                result['lat'] = result['coords']['lat']
                result['lng'] = result['coords']['lng']

                del result['coords']

                for review in result['reviews']:
                    result_copy = result.copy()

                    del result_copy['reviews']

                    result_copy.update(review['metadata']) 
                    writer.writerow(result_copy)


        self.__cleanup()

        return results

    def __halt_error(self):
        self.__cleanup()
        self.addMessage.emit("worker halted forcefully")
        self.finished.emit([])

    def stop(self):
        self.running = False

    def run(self):
        self.running = True

        self.logger.info("starting run...")

        results = self.__scrape()

        self.running = False

        self.logger.info("finished.")
        self.finished.emit(results)
