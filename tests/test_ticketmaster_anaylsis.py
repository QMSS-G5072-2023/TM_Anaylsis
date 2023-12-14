from ticketmaster_anaylsis import *
import pytest
import pandas as pd
import requests
from unittest.mock import patch, Mock

# Test for successful data retrieval by name
@patch('ticketmaster_anaylsis.requests.get')
def test_find_attraction_info_success_name(mock_get):
    mock_response = Mock()
    expected_output = {'_embedded': {'attractions': [{'name': 'Test Attraction', 'id': '123'}]}}
    mock_response.json.return_value = expected_output
    mock_response.status_code = 200
    mock_get.return_value = mock_response

    df, error = find_attraction_info('Test Attraction', 'name', 'dummy_api_key')
    assert error is None
    assert not df.empty
    assert df.iloc[0]['name'] == 'Test Attraction'
    assert df.iloc[0]['ID'] == '123'

# Test for successful data retrieval by ID
@patch('ticketmaster_anaylsis.requests.get')
def test_find_attraction_info_success_id(mock_get):
    mock_response = Mock()
    expected_output = {'name': 'Test Attraction', 'id': '123'}
    mock_response.json.return_value = expected_output
    mock_response.status_code = 200
    mock_get.return_value = mock_response

    df, error = find_attraction_info('123', 'id', 'dummy_api_key')
    assert error is None
    assert not df.empty
    assert df.iloc[0]['name'] == 'Test Attraction'
    assert df.iloc[0]['ID'] == '123'

# Test for handling invalid identifier type
def test_find_attraction_info_invalid_identifier():
    with pytest.raises(ValueError):
        find_attraction_info('Test Attraction', 'invalid_type', 'dummy_api_key')

# Test for handling empty identifier
def test_find_attraction_info_empty_identifier():
    df, error = find_attraction_info('', 'name', 'dummy_api_key')
    assert df.empty
    assert error == "Identifier cannot be empty."

# Test for handling no matching attractions
@patch('ticketmaster_anaylsis.requests.get')
def test_find_attraction_info_no_match(mock_get):
    mock_response = Mock()
    mock_response.json.return_value = {}
    mock_response.status_code = 200
    mock_get.return_value = mock_response

    df, error = find_attraction_info('Nonexistent Attraction', 'name', 'dummy_api_key')
    assert df.empty
    assert error == "No matching attractions found."

# Test for API rate limiting and retry mechanism
@patch('ticketmaster_anaylsis.requests.get')
def test_find_attraction_info_rate_limiting(mock_get):
    mock_response = Mock()
    # First call simulates a rate limit scenario, second call returns successful response
    mock_response.side_effect = [
        Mock(status_code=429),  # Rate limit response
        Mock(status_code=200, json=lambda: {'_embedded': {'attractions': [{'name': 'Test Attraction', 'id': '123'}]}})
    ]
    mock_get.side_effect = mock_response.side_effect

    df, error = find_attraction_info('Test Attraction', 'name', 'dummy_api_key')
    assert error is None
    assert not df.empty
    assert df.iloc[0]['name'] == 'Test Attraction'

    
@patch('ticketmaster_anaylsis.requests.get')
def test_get_performer_events_no_events(mock_get):
    mock_response = Mock()
    mock_response.json.return_value = {}
    mock_response.status_code = 200
    mock_get.return_value = mock_response

    df, error = get_performer_events_1('attraction_id', 'api_key')
    assert df.empty
    assert error == "No events found for this performer."

@patch('ticketmaster_anaylsis.requests.get')
def test_get_performer_events_rate_limiting(mock_get):
    mock_response_rate_limit = Mock(status_code=429)
    mock_response_success = Mock(status_code=200, json=lambda: {'_embedded': {'events': [{}]}})
    mock_get.side_effect = [mock_response_rate_limit, mock_response_success]

    df, error = get_performer_events_1('attraction_id', 'api_key')
    assert not df.empty
    assert error is None

@patch('ticketmaster_anaylsis.requests.get')
def test_get_performer_events_correct_data_parsing(mock_get):
    mock_response = Mock()
    mock_response.json.return_value = {
        '_embedded': {
            'events': [
                {
                    'name': 'Pinkpop 2024 (Sunday)',
                    'id': 'Z698xZbpZ171_uE',
                    'dates': {
                        'start': {
                            'localDate': '2024-06-23',
                            'localTime': '12:00:00',
                        }
                    },
                    'sales': {
                        'public': {
                            'startDateTime': '2023-12-09T09:00:00Z',
                            'endDateTime': '2024-06-23T15:00:00Z'
                        }
                    },
                    '_embedded': {
                        'venues': [
                            {
                                'name': 'Megaland',
                                'city': {'name': 'Landgraaf'},
                                'country': {'name': 'Netherlands'},
                            }
                        ]
                    }
                }
            ]
        }
    }
    mock_response.status_code = 200
    mock_get.return_value = mock_response

    df, error = get_performer_events_1('K8vZ9178pt7', 'dummy_api_key')
    assert not df.empty
    assert error is None
    assert df.iloc[0]['Event Name'] == 'Pinkpop 2024 (Sunday)'
    assert df.iloc[0]['Event ID'] == 'Z698xZbpZ171_uE'
    assert df.iloc[0]['Start Date'] == '2024-06-23'
    assert df.iloc[0]['Start Time'] == '12:00:00'
    assert df.iloc[0]['Venue'] == 'Megaland'
    assert df.iloc[0]['City'] == 'Landgraaf'
    assert df.iloc[0]['Country'] == 'Netherlands'

@patch('ticketmaster_anaylsis.requests.get')
def test_get_performer_events_venue_data_extraction(mock_get):
    mock_response = Mock()
    mock_response.json.return_value = {
        '_embedded': {
            'events': [
                {
                    'name': 'Event with Detailed Venue Data',
                    'id': 'Z698xZbpZ17xxx',
                    'dates': {
                        'start': {
                            'localDate': '2024-06-23',
                            'localTime': '12:00:00',
                            'dateTime': '2024-06-23T10:00:00Z'
                        }
                    },
                    '_embedded': {
                        'venues': [
                            {
                                'name': 'Megaland',
                                'id': 'Z598xZbpZee11',
                                'city': {'name': 'Landgraaf'},
                                'country': {'name': 'Netherlands', 'countryCode': 'NL'},
                                'address': {'line1': 'Hofstraat 13-15'},
                                'location': {'longitude': '6.02', 'latitude': '50.88219'},
                            }
                        ]
                    }
                }
            ]
        }
    }
    mock_response.status_code = 200
    mock_get.return_value = mock_response

    df, error = get_performer_events_1('K8vZ9178pt7', 'dummy_api_key')
    assert not df.empty
    assert error is None
    assert df.iloc[0]['Event Name'] == 'Event with Detailed Venue Data'
    assert df.iloc[0]['Venue'] == 'Megaland'
    assert df.iloc[0]['City'] == 'Landgraaf'
    assert df.iloc[0]['Country'] == 'Netherlands'
    assert df.iloc[0]['Venue ID'] == 'Z598xZbpZee11'  # Assuming 'Venue ID' is a field you want to include

@patch('ticketmaster_anaylsis.requests.get')
def test_get_performer_events_http_error(mock_get):
    mock_response = Mock()
    mock_response.raise_for_status.side_effect = requests.HTTPError("500 Server Error: Internal Server Error for url")
    mock_response.status_code = 500
    mock_get.return_value = mock_response

    df, error = get_performer_events_1('attraction_id', 'api_key')
    assert df.empty
    assert "HTTP error occurred: 500 Server Error: Internal Server Error for url" in error

@patch('ticketmaster_anaylsis.requests.get')
def test_get_performer_events_malformed_response(mock_get):
    mock_response = Mock()
    mock_response.json.return_value = {"malformed_data": "data"}  # An unexpected format
    mock_response.status_code = 200
    mock_get.return_value = mock_response

    df, error = get_performer_events_1('attraction_id', 'api_key')
    assert df.empty
    assert "No events found for this performer." in error

@patch('ticketmaster_anaylsis.requests.get')
def test_fetch_filtered_events_no_events(mock_get):
    mock_response = Mock()
    mock_response.json.return_value = {}  # No events in response
    mock_response.status_code = 200
    mock_get.return_value = mock_response

    df, error = fetch_filtered_events(api_key, city="Nonexistent City")
    assert df.empty
    assert error == "No events found for the given criteria."

@patch('ticketmaster_anaylsis.requests.get')
def test_fetch_filtered_events_http_error(mock_get):
    mock_response = Mock()
    mock_response.raise_for_status.side_effect = requests.HTTPError("500 Server Error: Internal Server Error for url")
    mock_response.status_code = 500
    mock_get.return_value = mock_response

    df, error = fetch_filtered_events(api_key, city="New York")
    assert df.empty
    assert "HTTP error occurred: 500 Server Error: Internal Server Error for url" in error

@patch('ticketmaster_anaylsis.requests.get')
def test_fetch_filtered_events_network_issues(mock_get):
    mock_get.side_effect = requests.exceptions.ConnectionError

    df, error = fetch_filtered_events(api_key, city="New York")
    assert df.empty
    assert "Failed to fetch data after 5 attempts." in error
