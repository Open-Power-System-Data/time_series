"""
Open Power System Data

Timeseries Datapackage

terna.py : extract file links for the web page of Terna
"""
__author__ = "Milos Simic"
__date__ = "2018-12-7"


from selenium import webdriver
import time
import datetime
import re
import requests
from selenium.webdriver.common.by import By
from selenium.common.exceptions import StaleElementReferenceException
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def get_driver_for_terna():
    """ 
    Return a Selenium webdriver instance for the Terna page.
    
    Returns
    ----------
    driver : selenium.webdriver.chrome.webdriver.WebDriver
        Webdriver Chrome instance for the Terna page.
        
    """
    driver = webdriver.Chrome(executable_path="./chromedriver/chromedriver")
    driver.maximize_window()
    driver.get("https://www.terna.it/SistemaElettrico/TransparencyReport/Generation/Forecastandactualgeneration.aspx")
    set_maximum_page_length(driver)
    return driver

def select_year(driver, year):
    """ 
    Selects year in the search form.
    
    Parameters
    ----------
    driver : WebDriver
        Webdriver for the Terna web page.
    year: int or str
        Year to select in the search form.
       
    Returns
    ----------
    None
    
    """
    # Convert year to string
    year = str(year)
    
    # Get currently select year
    year_input = driver.find_element_by_id("dnn_ctr5990_TernaViewDocumentView_cbAnno_Input")
    currently_selected_year = year_input.get_attribute("value")
    
    # If currently selected year is the one we want, do nothing. 
    # Else, determine the direction of search through year options.
    if year == currently_selected_year:
        return 
    elif year > currently_selected_year:
        direction = "up"
    else:
        direction = "down"
        
    # Make the options visible by clicking on the dropdown arrow.
    arrow = driver.find_element_by_id("dnn_ctr5990_TernaViewDocumentView_cbAnno_Arrow")
    arrow.click()
    
    # Get the action chain to navigate in said direction.
    navigate = get_navigate_action(driver, direction)
    
    # Go in the determined direction (up or down) until the right year is selected.
    while currently_selected_year != year:
        navigate.perform()
        currently_selected_year = year_input.get_attribute("value")
        #print("I see: ", currently_selected_year, ", I want: ", year)
        
    # Make sure that the year is selected.
    select = get_select_action(driver)
    select.perform()

def select_month(driver, month):
    """ 
    Selects month in the search form.
    
    Parameters
    ----------
    driver : WebDriver
        Webdriver for the Terna web page.
    month: int
        Month to select in the search form.
       
    Returns
    ----------
    None
    
    """
    # Make the month options visible by clicking on the dropdown arrow
    arrow = driver.find_element_by_id("dnn_ctr5990_TernaViewDocumentView_cbMese_Arrow")
    arrow.click()
    # Get the name of the currently selected month and convert it to number
    month_input = driver.find_element_by_id("dnn_ctr5990_TernaViewDocumentView_cbMese_Input")
    currently_selected_month = month_input.get_attribute("value")
    currently_selected_month = to_month_number(currently_selected_month)
    
    # If the desired month is already selected, do nothing.
    # Else, determine the direction to search the month options in.
    if month == currently_selected_month:
        return
    elif month > currently_selected_month:
        direction = "down"
    else:
        direction = "up"
    
    # Get the navigation action chain.
    navigate = get_navigate_action(driver, direction)
    
    # Iterate through the month options in the determined direction
    # until the desired month is reached.
    while currently_selected_month != month:
        navigate.perform()
        currently_selected_month = month_input.get_attribute("value")
        currently_selected_month = to_month_number(currently_selected_month)
        #print("I see: ", currently_selected_month, ", I want: ", month)
    
    # Make sure that the month is selected.
    select = get_select_action(driver)
    select.perform()

def search(driver):
    """ 
    Clicks the search button in the form.
    
    Parameters
    ----------
    driver : WebDriver
        Webdriver for the Terna web page.
       
    Returns
    ----------
    None
    
    """
    # Find the search button
    search_button = driver.find_element_by_class_name("dnnSecondaryAction")
    # Click it and wait for the results
    safe_refresh_click(driver, search_button)


def set_maximum_page_length(driver):
    """ 
    Set the result page length to its maximal value.
    
    Parameters
    ----------
    driver : WebDriver
        Webdriver for the Terna web page.
       
    Returns
    ----------
    None
    
    """
    # Find the div with the page length options.
    paginator_div = driver.find_element_by_id("dnn_ctr5990_TernaViewDocumentView_grdDocument_ctl00_ctl03_ctl01_PageSizeComboBox")
    # Activate the dropdown menu by clicking the arrow.
    arrow = paginator_div.find_element_by_id("dnn_ctr5990_TernaViewDocumentView_grdDocument_ctl00_ctl03_ctl01_PageSizeComboBox_Arrow")
    arrow.click()
    # There are three ordered options, so go to the last one as it is the maximal one.
    lengths = paginator_div.find_elements_by_tag_name("li")
    navigate = get_navigate_action(driver, "down")
    navigate.perform()
    navigate.perform()
    # Select the maximal length, activating the form.
    select = get_select_action(driver)
    select.perform()
    # Wait for the results to refresh
    wait_for_results_to_refresh(driver)

def next_page(driver, current_page_number):
    """ 
    Go to the next page of the results
    
    After clicking on the right page number in the form, wait for the results to refresh. 
    If the results are successfully refreshed, return current_page_number + 1.
    If it is not possible to click on the indicated page number because there is none,
    do nothing and return None.
    
    Parameters
    ----------
    driver : WebDriver
        Webdriver for the Terna web page.
    current_page_number: int
        The 1-based index number of the current page.
       
    Returns
    ----------
    new_page_number: int or None
        The number of the current next page to search.
    
    """
    
    # Determine the next page number.
    next_page_number = current_page_number + 1
    page_index = driver.find_element_by_class_name("rgNumPart")
    # Finds links to the pages.
    links = page_index.find_elements_by_tag_name("a")
    for link in links:
        span = link.find_element_by_tag_name("span")
        if span.text == "...":
            # If the search reached the next chunk of pages, click the "..." link to load them.
            if link.get_attribute("title") == "Next Pages":
                #print("I will click: ", link.text)
                status = safe_refresh_click(driver, link)
                if status == True:
                    return next_page_number
                else:
                    return current_page_number
        else:
            # If the link number is equal to the next page number, click it.
            page_number = int(span.text)
            if page_number == next_page_number:
                #print("I will click: ", link.text)
                status = safe_refresh_click(driver, link)
                if status == True:
                    return next_page_number
                else:
                    return current_page_number
    return None

def go_back(driver):
    """ 
    Reset the page number in the search form to 1
    
    Parameters
    ----------
    driver : WebDriver
        Webdriver for the Terna web page.
       
    Returns
    ----------
    None
    
    """
    
    dots_link = None
    start_link = None
    at_the_beginning = False
    #print("Going back...")
    # Clicks left "..." links or "1" to set the page number to 1.
    while not at_the_beginning:
        page_index = driver.find_element_by_class_name("rgNumPart")
        links = page_index.find_elements_by_tag_name("a")
        for link in links:
            span = link.find_element_by_tag_name("span")
            if span.text == "...":
                if link.get_attribute("title") == "Previous Pages":
                    dots_link = link
            else:
                page_number = int(span.text)
                if page_number == 1:
                    start_link = link
        if start_link is not None:
            #print("I will click 1")
            safe_refresh_click(driver, start_link)
            at_the_beginning = True
        else:
            #print("I will click ...(Previous Pages)")
            safe_refresh_click(driver, dots_link)

def safe_refresh_click(driver, element):
    """ 
    Click on the element and wait for the results to refresh
    
    Parameters
    ----------
    driver : WebDriver
        Webdriver for the Terna web page.
    element: WebDriverElement
        Page element to click
       
    Returns
    ----------
    status: bool
        True if the results refreshed, False otherwise.
    
    """
    # Click using javascript
    do_javascript_click(driver, element)
    # Wait for the results to refresh
    status = wait_for_results_to_refresh(driver)
    
    return status

def wait_for_results_to_refresh(driver):
    """ 
    Wait for the results to refresh
    
    Wait for 90 seconds for the element indicating
    that the results are being refreshed to be gone.
    
    Parameters
    ----------
    driver : WebDriver
        Webdriver for the Terna web page.
       
    Returns
    ----------
    status: bool
        True if the page refreshed in no more than 90 seconds, False otherwise.
    
    """
    try:
        # Locate the element which indicates that the results are being refreshed
        id_of_the_loading_panel = "dnn_ctr5990_TernaViewDocumentView_raLoadingPaneldnn_ctr5990_TernaViewDocumentView_grdDocument"
        start = time.time()
        # Wait 0.5 seconds
        time.sleep(0.5)
        # Wait until it is gone
        status = WebDriverWait(driver, 90).until_not(
            EC.presence_of_element_located((By.ID, id_of_the_loading_panel))
        )
        elapsed = time.time() - start
        #print("Continuing after {}s. Status: {}".format(elapsed, status))
        return True
    except TimeoutException:
        status = None
        elapsed = time.time() - start
        #print("Continuing after {}s. Status: {}".format(elapsed, status))
        return False


def to_month_number(month_name):
    """ 
    Convert English month name to a number from 1 to 12.
    
    Parameters
    ----------
    month_name : str
        Month name in English
       
    Returns
    ----------
    month_number: int
        1-12 for the names from January to December, 0 for other inputs
    
    """
    names = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
    if month_name in names:
        return names.index(month_name) + 1
    else:
        return 0

def filter_links(driver, prefix, start_date, end_date):
    """ 
    Get the links whose titles start with prefix and whose dates are between start_date and end_date
    
    Parameters
    ----------
    driver : WebDriver
        Webdriver for the Terna web page
    prefix: str
        The substring with which the links have to start
    start_date: datetime.date
        The minimal allowed date of the links
    end_date: datetime.date
        The maximal allowed date of the links
       
    Returns
    ----------
    url_dictionary: dict
        Dictionary of the form {(year, month, day) : url} of the links satisfying search conditions.
    
    """
    links = driver.find_elements_by_css_selector("#dnn_ctr5990_TernaViewDocumentView_pnlAccordion a")
    return {date_of_link(link):url_of_link(link) for link in links if link_is_ok(link, prefix, start_date, end_date)}


def link_is_ok(link, prefix, start_date, end_date):
    """ 
    Check if the link satisfies search conditions
    
    Parameters
    ----------
    link : WebElement
        The very link to inspect
    prefix: str
        String with which the title of the link has to start
    start_date: datetime.date
        The minimal allowed date of the links
    end_date: datetime.date
        The maximal allowed date of the links
    
       
    Returns
    ----------
    is_ok: bool
        True if link satisfies search conditions, False otherwise.
    
    """
    # Get the title and remove all the leading and ending whitespaces
    title = link.get_attribute("title").strip()
    if title.startswith(prefix):
        # If the link starts with prefix
        # Get its date
        (year, month, day) = date_of_link(link)
        link_date = datetime.date(year=year, month=month, day=day)
        # And check if it is the interval [start_date, end_date]
        return start_date <= link_date <= end_date
    else:
        # Otherwise, conclude that the link does not satisfy search conditions
        return False


def date_of_link(link):
    """ 
    Extract the date from the title of the link
    
    Parameters
    ----------
    link : WebElement
        The very link to inspect
       
    Returns
    ----------
    date: tuple
        Date tuple of the form (year, month, day)
    
    """
    title = link.get_attribute("title")
    date_part = title.split("Actual generation of intermittent generation ")[1]
    day, month_abbreviation, year = date_part.split("_")
    month_abbreviations = ["gen", "feb", "mar", "apr", "mag", "giu", "lug", "ago", "set", "ott", "nov", "dic"]
    month = month_abbreviations.index(month_abbreviation) + 1
    date = (int(year), int(month), int(day))
    return date

def url_of_link(link):
    """ 
    Extract the url of the link
    
    Parameters
    ----------
    link : WebElement
        The link to inspect
       
    Returns
    ----------
    url: str
        The url of the given link
    
    """
    return link.get_attribute("href")

def get_navigate_action(driver, direction):
    """ 
    Get the action chain to navigate through a list of options
    in the given direction.
    
    Parameters
    ----------
    driver : WebDriver
        The webdriver for the Terna page
    direction: str
        The direction in which to move: "up" or "down"
       
    Returns
    ----------
    action: ActionChains
        The chain of actions by which to navigate through a list of options on a page
    
    """
    action = webdriver.common.action_chains.ActionChains(driver)
    if direction == "up":
        action.send_keys(webdriver.common.keys.Keys.UP)
    elif direction == "down":
        action.send_keys(webdriver.common.keys.Keys.DOWN)
    else:
        # down by default
        print("No direction specified!")
        action.send_keys(webdriver.common.keys.Keys.DOWN)

    action.release()
    return action

def get_select_action(driver):
    """ 
    Get the action chain to select the currently active option in a list.
    
    Parameters
    ----------
    driver : WebDriver
        The webdriver for the Terna page"
       
    Returns
    ----------
    action: ActionChains
        The chain of actions selecting the active option in a list.
    
    """
    action = webdriver.common.action_chains.ActionChains(driver)
    action.send_keys(webdriver.common.keys.Keys.ENTER)
    action.release()
    return action

def do_javascript_click(driver, element, sleeping_time=0):
    """ 
    Click on the web element using javascript
    
    Parameters
    ----------
    driver : WebDriver
        The webdriver for the Terna page
    element: WebElement
        The element to click
    sleeping_time: int
        The amount of time to wait after clicking, 0 by default
    
       
    Returns
    ----------
    None
    
    """
    driver.execute_script("arguments[0].click();", element)
    if sleeping_time > 0:
        time.sleep(sleeping_time)

def get_web_element_attribute_names(web_element):
    """Get all attribute names of a web element"""
    # get element html
    html = web_element.get_attribute("outerHTML")
    # find all with regex
    pattern = """([a-z]+-?[a-z]+_?)='?"?"""
    return re.findall(pattern, html)

def make_editable(driver, element_id):
    """Make the element editable"""
    script_template = 'document.getElementById("{}").removeAttribute("readonly")'
    script = script_template.format(element_id)
    driver.execute_script(script)

def search_subperiod(driver, start_date, end_date, year, month=None):
    """ 
    Search for the links whose dates are between start_date and end_date, 
    are in the given year and month and represent the desired files. 
    If month is None, search only the given year.
    
    Parameters
    ----------
    driver : WebDriver
        The webdriver for the Terna page
    start_date: datetime.date
        The minimal allowed date of the links
    end_date: datetime.date
        The maximal allowed date of the links
    year: int
        Year to search
    month: int
        Month to search, None by default
       
    Returns
    ----------
    url_dictionary: dict
        Dictionary of the form {(year, month, day) : url} of the links satisfying search conditions.
    
    """
    # Select the year in the form
    select_year(driver, year)
    # Select the month if it is specified
    if month is not None:
        select_month(driver, month)
    if month is None:
        month = "1-12"
    # Click the search button
    search(driver)
    # Go through result pages and collect the links
    page_number = 1
    collected = {}
    while page_number is not None:
        filtered = filter_links(driver, "Actual generation of intermittent generation", start_date, end_date)
        #print("{} links on page {} for ({}, {}).".format(len(filtered), page_number, year, month))
        collected.update(filtered)
        page_number = next_page(driver, page_number)
    return collected


def search_period(driver, start_date, end_date, subperiods=None):
    """ 
    Search for the links whose dates are between start_date and end_date, 
    represent the desired files, and belong to given subperiods, if specified. 
    If subperiods is None, divide [start_date, end_date] to subperiods and search them
    one by one.
    
    Parameters
    ----------
    driver : WebDriver
        The webdriver for the Terna page
    start_date: datetime.date
        The minimal allowed date of the links
    end_date: datetime.date
        The maximal allowed date of the links
    subperiods: list
        List of subperiods of the form (year, month) or (year, None)
       
    Returns
    ----------
    url_dictionary: dict
        Dictionary of the form {(year, month, day) : url} of the links satisfying search conditions.
    
    """
    if subperiods is None:
        subperiods = get_subperiods(start_date, end_date)
    all_the_links = {}
    for year, month in subperiods:
        collected = search_subperiod(driver, start_date, end_date, year, month)
        all_the_links.update(collected)
        go_back(driver)
    return all_the_links


def get_subperiods(start_date, end_date):
    """ 
    Divide the interval [start_date, end_date] into several subperiods.
    
    The subperiods are either whole years or months of specific years. 
    The year of the start_date is included completely.
    The years between the years of start_date and end_date are also included completely.
    The period [1.1.(end_date.year), end_date] is included by months of the year end_date.year
    from 1 to end_date.month, inclusive.
    
    Parameters
    ----------
    start_date: datetime.date
        The minimal allowed date of the links
    end_date: datetime.date
        The maximal allowed date of the links
   
    Returns
    ----------
    subperiods: list
        List of subperiods of the form (year, month) or (year, None)
    
    """
    subperiods = []
    
    date = start_date
    while date.year < end_date.year:
        subperiods += [(date.year, None)]
        date = add_years(date, 1)
    for month in range(1, end_date.month + 1):
        subperiods += [(end_date.year, month)]
    return subperiods



def add_years(d, years):
    """
    Add 'year' years to the date 'd'.
    
    Return a date that's `years` years after the date (or datetime)
    object `d`. Return the same calendar date (month and day) in the
    destination year, if it exists, otherwise use the following day
    (thus changing February 29 to March 1).
    source: https://stackoverflow.com/a/15743908/1518684
    
    Parameters:
    ----------
    d: datetime.date
        The date to which 'years' years should be added.
    years: int
        The number of years to add to date.
    
    Returns:
    ----------
    new_d: datetime.date
        Date d incremented by 'years' years.

    """
    try:
        return d.replace(year = d.year + years)
    except ValueError:
        return d + (date(d.year + years, 1, 1) - date(d.year, 1, 1))


def extract_urls(start_date, end_date, save=False):
    """
    Extract all the available urls for dates between start_date and end_date
    
    Parameters:
    ----------
    start_date: datetime.date
        The minimal allowed date of the links
    end_date: datetime.date
        The maximal allowed date of the links
    save: bool
        True if the results should be saved as a csv file, False otherwise
    
    Returns
    ----------
    url_dictionary: dict
        Dictionary of the form {(year, month, day) : url} of the links satisfying search conditions.
        
    """
    subperiods = get_subperiods(start_date, end_date)
    collected, missing_dates, no_duplicates = collect_urls(start_date, end_date, subperiods=subperiods)
    # This commented code is not necessary for now, but should remain here in the case we need it later
    #i = 0
    #while len(missing_dates) > 0 and i < 3:
    #   subperiods = {}
    #   for missing_date in missing_dates:
    #       subperiods[(missing_date.year, missing_date.month)] = True
    #   subperiods = list(subperiods.keys())
    #   collected, missing_dates, no_duplicates = collect_urls(start_date, end_date, collected=collected, subperiods=subperiods)
    #   i = i + 1
    if save:
        filename = "results_{}-{}.csv".format(start_date, end_date)
        save_to_file(collected, filename)
    return collected        

def get_terna_no_data_dates():
    """Return a list of dates for which it is certain that there are no corresponding files on the Terna page"""
    date_tuples = [
        (2015, 10, 9)
    ]
    return date_tuples


def save_to_file(collected, filename):
    """Save the dictionary of collected urls to a csv file"""
    f = open(filename, "w")
    f.write("Year, Month, Day, Url\n")
    ordered_dates = sorted(collected.keys())
    for date_tuple in ordered_dates:
        url = collected[date_tuple]
        year, month, day = date_tuple
        f.write("{},{},{},{}\n".format(year, month, day, url))
    f.close()


def collect_urls(start_date, end_date, collected={}, subperiods=[]):
    """
    Collect the urls between start_date and end_date and add them to collected
    
    If subperiods are provided, search them. If they are not given, create subperiods to cover
    [start_date, end_date] by using get_subperiods. Check if dates are missing and urls are all unique.
    
    Parameters:
    ----------
    start_date: datetime.date
        The minimal allowed date of the links
    end_date: datetime.date
        The maximal allowed date of the links
    collected: dict
        The dictionary of previously collected links, {} by default
    subperiods: list
        The list of subperiods covering the period 1.1.(start_date.year)-end_date, [] by default
    
    Returns
    ----------
    (collected, missing_dates, no_duplicates) : tuple
        A tuple consisting of 1. collected urls appended to those provided with the function call,
        2. a list of dates for which no url was found, 3. indicator if some urls are repeated
    """
    driver = get_driver_for_terna()
    if len(subperiods) == 0:
        subperiods = get_subperiods(start_date, end_date)
    #print(subperiods)
    results = search_period(driver, start_date, end_date, subperiods=subperiods)
    collected.update(results)
    #print("{} links.".format(len(collected)))
    

    # Checking if URLs are collected for all the dates in the period.

    delta = end_date - start_date
    all_the_dates = True
    missing_dates = [] #
    no_data_dates = get_terna_no_data_dates()
    for i in range(0, delta.days + 1):
        date = start_date + datetime.timedelta(days=i)
        date_key = (date.year, date.month, date.day)
        if date_key not in collected and date_key not in no_data_dates:
            print("Missing date: ", date)
            missing_dates += [date]
            all_the_dates = False

    #if all_the_dates:
    #    print("All the dates were processed.")

    # Checking if all the URLs are unique.
    no_duplicates = True

    reversed_collection = {}
    for date_key, url in collected.items():
        reversed_collection.setdefault(url, set()).add(date_key)

    for url in reversed_collection.keys():
        if len(reversed_collection[url]) > 1:
            #print("Repeated url: ", url)
            #print("\tDates: ", reversed_collection[url])
            no_duplicates = False

    #if no_duplicates:
    #    print("All the URLs are unique.")

    driver.quit()

    return (collected, missing_dates, no_duplicates)