# Function 1

import pandas as pd
import requests
import time
from functools import lru_cache
from dotenv import load_dotenv
import os

# Load API key from environment variables
load_dotenv('key_for_TM.env')
api_key = os.getenv('CONSUMER_KEY')

@lru_cache(maxsize=100)  # Simple in-memory caching
def find_attraction_info(identifier, identifier_type, api_key):
    """
    Queries the Ticketmaster API to retrieve information about an attraction, based on a specified identifier.
    This function can search for attractions using either their name or unique ID. It's particularly useful
    in workflows where an attraction's detailed information, including its ID, is needed for subsequent
    queries, such as fetching related event details with the 'get_performer_events_1' function.

    Args:
        identifier (str): The name or unique ID of the attraction. The value should be a string corresponding 
                          to the attraction's name (e.g., 'Taylor Swift') if 'name' is chosen as identifier_type, 
                          or the attraction's unique ID (e.g., 'K8vZ9175Tr0') if 'id' is chosen.
        identifier_type (str): Specifies the type of the identifier. Acceptable values are 'name' or 'id'.
                               'name' will treat the identifier as the attraction's name, while 'id' will treat 
                               it as the attraction's unique ID.
        api_key (str): API key for accessing the Ticketmaster API. This should be a valid API key provided by 
                       Ticketmaster.

    Returns:
        pandas.DataFrame: A DataFrame containing detailed information about attractions that match the given 
                          identifier. The DataFrame includes columns like the attraction's name, its unique ID, 
                          the number of upcoming events listed on Ticketmaster, and the total number of upcoming 
                          events.
        str: An error message if the request fails, or if no attractions are found. This is a string detailing 
             the error. If the function executes successfully, this return value will be None.

    Raises:
        ValueError: If the 'identifier_type' is not one of the expected values ('name' or 'id').

    Example Usage:
        # To find information about an attraction by name:
        >>> attraction_df, error_message = find_attraction_info('Taylor Swift', 'name', api_key)
        >>> print(attraction_df)

        # To find information about an attraction by its unique ID:
        >>> attraction_df, error_message = find_attraction_info('K8vZ9175Tr0', 'id', api_key)
        >>> print(attraction_df)

    Note:
        The function implements a retry mechanism with exponential backoff to handle potential issues like 
        rate limits or network errors. This ensures reliability in fetching data from the Ticketmaster API.
        It is designed to work in conjunction with 'get_performer_events_1', providing the necessary attraction
        ID to fetch detailed event information.
    """
    if identifier_type not in ['name', 'id']:
        raise ValueError("Invalid identifier type. Must be 'name' or 'id'.")

    if not identifier:
        return pd.DataFrame(), "Identifier cannot be empty."
    
    if identifier_type == 'name':
        link = f"https://app.ticketmaster.com/discovery/v2/attractions.json?keyword={identifier}&apikey={api_key}"
    elif identifier_type == 'id':
        link = f"https://app.ticketmaster.com/discovery/v2/attractions/{identifier}.json?apikey={api_key}"

    # Implementing retry mechanism with exponential backoff
    max_retries = 5
    retry_delay = 1  # Starting delay in seconds

    for attempt in range(max_retries):
        try:
            response = requests.get(link)
            if response.status_code == 429:  # Rate limit error code
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential increase in delay
                continue
            response.raise_for_status()  # Raise an error for bad HTTP status
            break  # Break the loop if successful
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                return pd.DataFrame(), f"Failed to fetch data after {max_retries} attempts. Error: {e}"
            time.sleep(retry_delay)
            retry_delay *= 2

    # Handle the case where all retries fail
    if response.status_code != 200:
        return pd.DataFrame(), f"Error fetching data: Status code {response.status_code}"

    # Process the response data
    data = response.json()
    matched_attractions = []
    error_message = None  # Initialize the error message as None

    if identifier_type == 'name':
        if '_embedded' in data and 'attractions' in data['_embedded']:
            identifier_keywords = identifier.lower().split()
            for attraction in data['_embedded']['attractions']:
                attraction_name_keywords = attraction['name'].lower().split()
                if all(keyword in attraction_name_keywords for keyword in identifier_keywords):
                    matched_attractions.append(extract_attraction_info(attraction))
            if not matched_attractions:
                error_message = "No matching attractions found with the name."
        else:
            error_message = "No matching attractions found."

    elif identifier_type == 'id':
        if 'name' in data and 'id' in data:
            matched_attractions.append(extract_attraction_info(data))
        else:
            error_message = "No matching attractions found by ID."

    return pd.DataFrame(matched_attractions), error_message

def extract_attraction_info(attraction): 
    """
    Extracts and formats key information from an attraction entry returned by the Ticketmaster API.
    This helper function is utilized within 'find_attraction_info' to process individual attractions 
    in the API response. It structures the data into a readable and easy-to-use format.

    Args:
        attraction (dict): A dictionary containing the raw data of a single attraction from the API.
                           Expected to include keys like 'name', 'id', and 'upcomingEvents', along with 
                           their associated values as provided by the Ticketmaster API.

    Returns:
        dict: A dictionary containing formatted information about the attraction. Includes the attraction's
              'name', 'ID', 'Ticketmaster Upcoming Events' count, and 'Total Upcoming Events' count. 
              Defaults to 'N/A' for 'name' and 'ID' if they are not present in the input dictionary, 
              and 0 for event counts if this information is not available.

    The function ensures that even if certain details are missing in the API response, the returned
    dictionary still maintains a consistent structure, which is crucial for integrating the output 
    into a DataFrame in the 'find_attraction_info' function.
    """
    return {
        'name': attraction.get('name', 'N/A'),
        'ID': attraction.get('id', 'N/A'),
        'Ticketmaster Upcoming Events': attraction.get('upcomingEvents', {}).get('ticketmaster', 0),
        'Total Upcoming Events': attraction.get('upcomingEvents', {}).get('_total', 0)
    }




# Function 2

def get_performer_events_1(attraction_id, api_key, max_retries=5):
    """
    Retrieves detailed event information for a performer based on their Ticketmaster attraction ID. 
    The function queries the Ticketmaster API and returns a structured DataFrame containing 
    various details about the events associated with the specified attraction ID. 

    It implements a retry mechanism with exponential backoff to handle rate limit errors 
    and other network-related issues, ensuring robustness in API request handling.

    Args:
    attraction_id (str): The unique ID of the performer's attraction as recognized by Ticketmaster.
    api_key (str): API key for accessing the Ticketmaster API.
    max_retries (int): Maximum number of retry attempts for the API request in case of failure.

    Returns:
    tuple: 
        - pandas.DataFrame: A DataFrame containing details of events such as event name, ID, 
          dates, venue, city, country, and pricing information.
        - str or None: An error message if the request fails, or None if it is successful.

    The DataFrame will be empty if no events are found for the performer. The function 
    retries the request up to 'max_retries' times in case of a rate limit error (HTTP 429) 
    or other network issues, with an increasing delay between retries.

    Example:
    >>> performer_events_df, error_message = get_performer_events_1('K8vZ9175Tr0', 'YOUR_API_KEY', max_retries=3)
    >>> print(performer_events_df)

    Note:
    If the request fails after all retries, or if another error occurs, the function 
    returns an empty DataFrame and an error message.
    """    
    events_url = f"https://app.ticketmaster.com/discovery/v2/events.json?apikey={api_key}&attractionId={attraction_id}"
    retry_delay = 1  # Start with a delay of 1 second

    for attempt in range(max_retries):
        try:
            response = requests.get(events_url)
            
            # Check for rate limit before proceeding
            if response.status_code == 429:
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                    continue
                else:
                    return pd.DataFrame(), "Rate limit exceeded, try again later."

            response.raise_for_status()  # Raises an exception for HTTP errors

            events_data = response.json().get('_embedded', {}).get('events', [])
            if not events_data:
                return pd.DataFrame(), "No events found for this performer."

            # Process events data
            events_info = []
            for event in events_data:
                event_info = extract_event_info(event)
                events_info.append(event_info)

            return pd.DataFrame(events_info), None

        except requests.HTTPError as http_err:
            return pd.DataFrame(), f"HTTP error occurred: {http_err}"
        except Exception as err:
            return pd.DataFrame(), f"Other error occurred: {err}"

    return pd.DataFrame(), "Failed to fetch events after retries"

def extract_event_info(event):
    """
    Extracts relevant information from a single event object.

    Args:
    event (dict): A dictionary containing details of a single event.

    Returns:
    dict: A dictionary with extracted event information.
    """
    min_price, max_price = None, None
    if 'priceRanges' in event:
        prices = event['priceRanges'][0]
        min_price = prices.get('min')
        max_price = prices.get('max')

    avg_price = (min_price + max_price) / 2 if min_price and max_price else None

    # Extracting venue ID
    venue_id = event.get('_embedded', {}).get('venues', [{}])[0].get('id', 'N/A')

    return {
        'Event Name': event.get('name'),
        'Event ID': event.get('id'),
        'Start Date': event.get('dates', {}).get('start', {}).get('localDate'),
        'Start Time': event.get('dates', {}).get('start', {}).get('localTime'),
        'Venue': event.get('_embedded', {}).get('venues', [{}])[0].get('name'),
        'Venue ID': venue_id,  # Including Venue ID
        'City': event.get('_embedded', {}).get('venues', [{}])[0].get('city', {}).get('name'),
        'Country': event.get('_embedded', {}).get('venues', [{}])[0].get('country', {}).get('name'),
        'publicsale_start': event.get('sales', {}).get('public', {}).get('startDateTime'),
        'publicsale_end': event.get('sales', {}).get('public', {}).get('endDateTime'),
        'presale_start': event.get('sales', {}).get('presale', [{}])[0].get('startDateTime'),
        'presale_end': event.get('sales', {}).get('presale', [{}])[0].get('endDateTime'),
        'Min Price': min_price,
        'Max Price': max_price,
        'Average Price': avg_price
    }



# Function 3

def fetch_filtered_events(api_key, start_date=None, end_date=None, city=None, state_code=None, country_code=None): 
    """
    Retrieves a list of events from the Ticketmaster API based on specified filtering criteria.

    Args:
    api_key (str): API key for accessing the Ticketmaster API.
    start_date (str, optional): The start date for filtering events in ISO 8601 format (YYYY-MM-DDThh:mm:ssZ).
                                Defaults to None, which means no start date filter is applied.
    end_date (str, optional): The end date for filtering events in ISO 8601 format (YYYY-MM-DDThh:mm:ssZ).
                              Defaults to None, which means no end date filter is applied.
    city (str, optional): City name to filter events. Defaults to None.
    state_code (str, optional): State code to filter events. Defaults to None.
    country_code (str, optional): Country code to filter events. Defaults to None.

    Returns:
    tuple: A tuple where the first element is a pandas DataFrame with the events data. The DataFrame
           includes columns like event IDs, venue IDs, start and end times, and locations.
           The second element is either an error message (str) in case of failure or None if
           the request is successful.

    This function handles rate limits by implementing a retry mechanism with exponential backoff.
    In case of rate limit errors, it retries the request up to a maximum of five times with increasing delays.

    Example:
    >>> events_df, error_message = fetch_filtered_events(api_key, city="New York", start_date="2023-01-01T00:00:00Z", end_date="2023-12-31T00:00:00Z")
    >>> print(events_df)
    
    Note:
    The function raises HTTP errors and other exceptions if the request fails after all retry attempts.
    """
    base_url = "https://app.ticketmaster.com/discovery/v2/events.json"
    params = {
        'apikey': api_key,
        'startDateTime': start_date,
        'endDateTime': end_date,
        'city': city,
        'stateCode': state_code,
        'countryCode': country_code
    }
    
    max_retries = 5
    retry_delay = 1  # Starting delay in seconds
    response = None  # Initialize response

    for attempt in range(max_retries):
        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()  # Raises an exception for HTTP errors
            break  # Exit the loop if the request is successful
        except requests.exceptions.HTTPError as http_err:
            if response and response.status_code == 429:  # Rate limit error code
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential increase in delay
                continue
            return pd.DataFrame(), f"HTTP error occurred: {http_err}"
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                return pd.DataFrame(), f"Failed to fetch data after {max_retries} attempts. Error: {e}"
            time.sleep(retry_delay)
            retry_delay *= 2

    if response is None or response.status_code != 200:
        return pd.DataFrame(), "No successful response received from the API."

    events_data = response.json().get('_embedded', {}).get('events', [])
    if not events_data:
        return pd.DataFrame(), "No events found for the given criteria."

    events_info = []
    for event in events_data:
            event_info = {
                'ID': event.get('id'),
                'Venue ID': event.get('_embedded', {}).get('venues', [{}])[0].get('id'),
                'Start DateTime': event.get('dates', {}).get('start', {}).get('dateTime'),
                'End DateTime': event.get('dates', {}).get('end', {}).get('dateTime'),
                'City': event.get('_embedded', {}).get('venues', [{}])[0].get('city', {}).get('name'),
                'State Code': event.get('_embedded', {}).get('venues', [{}])[0].get('state', {}).get('stateCode'),
                'Country Code': event.get('_embedded', {}).get('venues', [{}])[0].get('country', {}).get('countryCode'),
                'Onsale Start DateTime': event.get('sales', {}).get('public', {}).get('startDateTime'),
                'Onsale End DateTime': event.get('sales', {}).get('public', {}).get('endDateTime'),
                'Local Start DateTime': event.get('dates', {}).get('start', {}).get('localDate'),
                'Local End DateTime': event.get('dates', {}).get('end', {}).get('localDate'),
                'Start End DateTime': event.get('dates', {}).get('timezone')
            }
            events_info.append(event_info)

    return pd.DataFrame(events_info), None
