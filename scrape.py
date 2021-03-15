import json
from queue import Empty
import time
import multiprocessing as mp
import sys
import os

import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import (NoSuchElementException,
                                        StaleElementReferenceException, TimeoutException)
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.chrome.options import Options
chrome_options = Options()
chrome_options.add_argument("--headless")

# We use selenium to automate some web browser stuff that beautiful soup can't.
# That means we can click the javascript links on the website (ex: click accept terms and agreement stuff)
LINKS_TO_ISSUERS_FILE = "links_to_issuers.json"
LINKS_TO_ISSUERS_DETAILS_FILE = "links_to_issuers_details.json"
DETAILS_JSON_FILE = "details.json"


# TO GET GECKODRIVER.EXE yourself DOWNLOAD THIS THING HERE https://github.com/mozilla/geckodriver/releases and save it on your comp

def accept_terms(driver) -> None:
    accept_button = driver.find_element_by_id('ctl00_mainContentArea_disclaimerContent_yesButton')
    accept_button.click()


# click_next_page selects the next button, returns true if clicked
def click_next_page(driver) -> bool:
    # find 'next' button to get the next list
    try:
        next_button = driver.find_element_by_class_name('next')
    except NoSuchElementException:
        print("no items on this page: ", driver.current_url)
        return False

    # Check if we can go to the next page or not
    attributes = next_button.get_attribute('class').split(" ")
    for attr in attributes:
        if attr == "disabled":
            return False

    # go to next page
    next_button.click()
    return True


def get_details_in_table(driver) -> list:
    details = []
    while True:
        time.sleep(0.5)  # if there is no sleep the table doesn't load in time....very annoying
        for row in driver.find_elements_by_tag_name('tr'):
            row_data = row.find_elements_by_tag_name('td')
            if len(row_data) != 12:
                pass
            else:
                # the cusip is usally a link
                detail = {
                    "CUSIP": 'TODO',
                    "Principle Amount at Issuance ($)": row_data[1].text,
                    "Security Description": row_data[2].text,
                    "Coupon": row_data[3].text,
                    "Maturity Date": row_data[4].text,
                    "Price/Yield": row_data[5].text,
                    "Price": row_data[6].text,
                    "Yield": row_data[7].text,
                    "Fitch": row_data[8].text,
                    "KBRA": row_data[9].text,
                    "Moody's": row_data[10].text,
                    "S&P": row_data[11].text,
                }
                details.append(detail)

        if not click_next_page(driver):
            return details


def get_links_in_table(driver) -> list:
    # get the details, this code pretty much the same as the issuers
    links = []
    while True:
        # find table on page and add links to each issuer
        for cell in driver.find_elements_by_css_selector('td'):
            try:
                link = cell.find_element_by_tag_name('a').get_attribute('href')
                links.append(link)
            except NoSuchElementException:
                pass

        if not click_next_page(driver):
            return links


def scrape_for_links_to_details(driver, links_to_issuers) -> list:
    links_to_details = []
    for index, link in enumerate(links_to_issuers):
        print("getting link to details on link", index, "of", len(links_to_issuers))
        driver.get(link)
        links_to_details.extend(get_links_in_table(driver))
        print("current detail link count", len(links_to_details))
    return links_to_details


# spawned using the multiprocessing library
def scrape_for_details(links_to_details, result_queue, start_index, end_index, process_index):
    d = new_driver()
    try:
        d.get(links_to_details[start_index])
        accept_terms(d)
    except:
        print("couldn't accept terms while scraping for details...")
        pass

    details = []
    index = start_index
    while index < end_index and index < len(links_to_details):
        link = links_to_details[index]
        try:
            d.get(link)
        except TimeoutException:
            print(f"timeout on first link load {link}...trying again")
            d.get(link)

        details.extend(get_details_in_table(d))
        index += 1
        if index % 10 == 0:
            print(f"process {process_index} is {index-start_index}/{end_index-start_index} done. {len(details)} details found")
            sys.stdout.flush()  # need this for subprocess to clear output buffer and to actually see logs

    # save details
    with open(f"{DETAILS_JSON_FILE}_{process_index}", 'w') as details_json_file:
        json.dump(details, details_json_file, indent=4)

    result_queue.put(details)


def new_driver() -> webdriver:
    #driver = webdriver.Chrome(options=chrome_options)

    driver = webdriver.Chrome('/Users/jpate201/Downloads/chromedriver', options=chrome_options) 
    driver.maximize_window()  # maximize so all elements are clickable
    return driver

if __name__ == "__main__":
    driver = new_driver()

    # Get website loaded
    driver.get("https://emma.msrb.org/IssuerHomePage/State?state=IL")

    accept_terms(driver)

    # gets links_to_issuers from file from previous run or scrape the site
    links_to_issuers = []
    try:
        with open(LINKS_TO_ISSUERS_FILE) as json_file:
            links_to_issuers = json.load(json_file)
            if links_to_issuers is None:
                raise FileNotFoundError
    except FileNotFoundError:
        print(f"no {LINKS_TO_ISSUERS_FILE}.....scraping website for new data")
        links_to_issuers = get_links_in_table(driver)

        # Save issuers links
        with open(LINKS_TO_ISSUERS_FILE, 'w') as issuer_link_file:
            json.dump(links_to_issuers, issuer_link_file, indent=4)

    driver.implicitly_wait(0.1)  # Implicit wait is needed for detail page

    # Go to each issuer's securities table
    links_to_details = []
    try:
        with open(LINKS_TO_ISSUERS_DETAILS_FILE) as json_file:
            links_to_details = json.load(json_file)
            if links_to_details is None:
                raise FileNotFoundError
    except FileNotFoundError:
        print(f"no {LINKS_TO_ISSUERS_DETAILS_FILE}...scraping website for new data")
        links_to_details = scrape_for_links_to_details(driver, links_to_issuers)
        # Save links to details
        with open(LINKS_TO_ISSUERS_DETAILS_FILE, 'w') as details_links_file:
            json.dump(links_to_details, details_links_file, indent=4)

    driver.close()

    # Go inside each issue detail and get the data from the final table

    details = []
    try:
        with open(DETAILS_JSON_FILE) as details_json_file:
            details = json.load(details_json_file)
            if details is None or len(details) == 0:
                raise FileNotFoundError
    except (FileNotFoundError, json.decoder.JSONDecodeError):
        print(f"no {DETAILS_JSON_FILE}...scraping website for new data")

        result_queue = mp.Queue()
        process_list = []
        process_count = int(15)
        chunk_size = int(len(links_to_details) / process_count) + len(links_to_details) % process_count

        # create threads to get details
        for i in range(process_count):
            start_index = i * chunk_size
            end_index = start_index + chunk_size
            if start_index < len(links_to_details):
                p = mp.Process(
                    target=scrape_for_details,
                    args=(links_to_details, result_queue, start_index, end_index, i)
                )
                process_list.append(p)

        # start all processes
        for p in process_list:
            p.start()

        # wait for all processes to finish
        for p in process_list:
            p.join()

        print("all details processes done")

    # todo fix the result queue stuff
    # save details
    # with open(DETAILS_JSON_FILE, 'w') as details_json_file:
    #     while result_queue.qsize() != 0:
    #         details.extend(result_queue.get())
    #     json.dump(details, details_json_file, indent=4)

    # combine all detail_process_index files
    for entry in os.listdir('./'):
        if os.path.isfile(entry) and f"{DETAILS_JSON_FILE}_" in entry:
            # this is a details.json_{process_index} file, combine it to giant list and save that as final output
            with open(entry) as json_file:
                details.extend(json.load(json_file))

    with open(DETAILS_JSON_FILE, 'w') as details_json_file:
        json.dump(details, details_json_file, indent=4)
