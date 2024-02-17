import requests
import urllib
import json
import time
from requests import Request, Response, Session
from abc import ABC, abstractmethod
import os

class API(ABC):
    """Abstract base class for subclassing by API related classes
       Contains the basic definitions that every API class should have"""

    session = Session()

    def call(self, request, retry_count=0) -> Response:
        """Issues the request to the API, and handle retrying the request"""
        try:
            resp = self.session.send(request, timeout=70)
            assert resp.status_code == 200
        except AssertionError:

            if retry_count < 6:
                return self.call(request, retry_count+1)
            else:
                raise APIResponseCodeException(resp.status_code, "Failed to get 2xx response 6 times.")
        except requests.exceptions.ReadTimeout:
            print("Request timed out, retrying")
            if retry_count < 6:
                return self.call(request, retry_count+1)

        self.confirm_response(resp)

        return resp

    @abstractmethod
    def confirm_response(self, resp):
        """Examines the response to confirm the response status is OK"""

    @abstractmethod
    def create_request(self, data) -> Request:
        """Construct a Request containing the data that needs to be sent to the API"""
        pass

    @abstractmethod
    def prepare_request(self, request) -> Request:
        """Add the necessary authorization params just before sending"""
        pass

    @abstractmethod
    def process_response(self, response):
        """Confirm the response is valid, and convert it to a suitable datatype like a dict"""
        pass


class APIResponseCodeException(Exception):
    def __init__(self, error_code, error_msg):
        self.error_code = error_code
        self.error_msg = error_msg


class APIResourceException(Exception):
    def __init__(self, error_msg):
        self.error_msg = error_msg


class APIResponseException(Exception):
    def __init__(self, error_msg):
        self.error_msg = error_msg


class SerpAPI(API):
    """Provides methods to query data from SerpAPI
       Depreciated due to lack of the key pre-snippet datapoint. Replaced by DataForSeo"""

    api_base = "https://serpapi.com/"
    api_key = "..." # Put your own

    def confirm_response(self, resp):
        try:
            assert json.loads(resp.text)["search_metadata"]["status"] == "Success"
        except AssertionError:
            raise APIResponseException("SerpAPI response has unsuccessful status")

    def create_request(self, data, path):
        url = f"{self.api_base + path}?{urllib.parse.urlencode(data)}"
        request = self.prepare_request(Request('GET', url))
        return request

    def prepare_request(self, request):
        request.url+= f"&api_key={self.api_key}"
        request = request.prepare()
        return request

    @staticmethod
    def process_response(resp):
        resp = json.loads(resp.text)
        next_page = False
        if "serpapi_pagination" in resp and "next" in resp["serpapi_pagination"] and resp["serpapi_pagination"]["next"]:
            next_page = resp["serpapi_pagination"]["next"]

        if resp["search_parameters"]["engine"] == "google_maps":
            if not "error" in resp:
                return resp["local_results"], next_page
            else:
                return [], next_page

        if resp["search_parameters"]["engine"] == "google":
            if "organic_results" in resp:
                return resp["organic_results"]
            else:
                return []

    @staticmethod
    def verify_maps_results(results):
        """Checks that each result is located within the UK by checking its lat&long
           Later down the road we would want to expand the results to other countries
           and thus need to re-write this function to be more clever"""

        verified_results = []
        for result in results:
            lat = float(result["gps_coordinates"]["latitude"])
            long = float(result["gps_coordinates"]["longitude"])

            if 49.8774 <= lat <= 60.861838 and -11.712093 <= long <= 2.108298:
                verified_results.append(result)
            else:
                print(f"Non UK result, skipping: {result['title']}")

        return verified_results

    def search_maps(self, query, lat=52.520110, long=1.375725, z=6.7):
        """Grabs all the results from all pages of a google maps search."""
        path = "search"
        request_data = {
            "q": query,
            "engine": "google_maps",
            "google_domain": "google.co.uk",
            "type": "search",
            "hl": "en",
            "ll": f"@{lat},{long},{z}z"
        }

        request = self.create_request(request_data, path)
        response = self.call(request)
        results, next_page = self.process_response(response)
        while next_page:
            request = self.prepare_request(Request('GET', next_page))
            response = self.call(request)
            next_results, next_page = self.process_response(response)
            results.extend(next_results)

        # verified_results = self.verify_maps_results(results)
        return results

    def search_google(self, query, start=0):
        """Searches google, one page at a time. For pagination use the start parameter."""
        path = "search"
        request_data = {
            "q": query,
            "start": start,
            "engine": "google",
            "hl": "en",
        }
        request = self.create_request(request_data, path)
        response = self.call(request)
        return self.process_response(response)


class DataForSEO(API):
    """Uses the serps API from dataforseo.com to fetch search resutls. Replaces SerpAPI"""

    api_base = "https://api.dataforseo.com/v3"
    api_key = "..." # Put your own

    def confirm_response(self, resp):
        try:
            assert json.loads(resp.text)["status_message"] == "Ok."
        except AssertionError:
            raise APIResponseException("DataForSEO response has unsuccessful status")

    def create_request(self, data, path, method='POST'):
        url = self.api_base + path
        request = self.prepare_request(Request(method, url, json=data))
        return request

    def prepare_request(self, request):
        request.headers = {"Authorization": f"Basic {self.api_key}"}
        return request.prepare()

    @staticmethod
    def process_response(resp):
        resp = json.loads(resp.text)
        task = resp["tasks"][0]
        if not task:
            return []

        if task["data"]["function"] == "live":
            return task["result"][0]["items"]
        else:
            return task

    def get_task(self, task_id):
        request = self.prepare_request(Request("GET", f"{self.api_base}/serp/google/organic/task_get/advanced/{task_id}"))
        response = self.call(request)
        resp = json.loads(response.text)

        if resp["tasks"][0]["result"]:
            return resp["tasks"][0]["result"][0]["items"]
        else:
            time.sleep(5)
            return self.get_task(task_id)

    def search_maps(self, query, lat=52.520110, long=1.375725, z=6):
        """Does a live search for maps, returns the results immediately."""
        payload = [{
            "keyword": query,
            "location_coordinate": f"{lat},{long},{z}z",
            "language_code": "en",
            "depth": 700
        }]
        request = self.create_request(payload, "/serp/google/maps/live/advanced")
        resp = self.call(request)
        results = self.process_response(resp)
        if not results:
            return []
        else:
            return results

    def search_google(self, query):
        """Submits the payload, but does not return the results straight away. Instead returns search identifier
           which can later be used to retrieve the results. This is cheaper and faster in bulk than the live method."""

        payload = [{
            "keyword": query,
            "location_code": 2826,
            "language_code": "en",
            "se_domain": "google.co.uk",
            "depth": 100,
            "priority": 2
        }]

        path = "/serp/google/organic/task_post"
        request = self.create_request(payload, path)
        # if os.path.isfile("tasks_list.txt"):
        #     with open("tasks_list.txt", "r") as f:
        #         return [x.strip() for x in f.readlines()]
        # else:
        response = self.call(request)
        return self.process_response(response)

    def search_google_realtime(self, query):
        """Same as above,"""
        payload = [{
            "keyword": query,
            "location_code": 2826,
            "language_code": "en",
            "se_domain": "google.co.uk",
            "depth": 100,
        }]
        path = "/serp/google/organic/live/advanced"
        request = self.create_request(payload, path)
        response = self.call(request)
        return self.process_response(response)


class OpenCageAPI(API):
    """Uses the generous free queries provided by opencageapi.com for translating locations.
       Translate a named location to a latitude and longitude."""

    api_key = "..." # Put your own
    api_base = "https://api.opencagedata.com/geocode/v1/json"

    @staticmethod
    def confirm_response(resp):
        # Check if we can do more queries today
        json_resp = json.loads(resp.text)
        if json_resp["rate"]["remaining"] == 0:
            raise APIResourceException("Out of free queries")
        try:
            assert json_resp["status"]["code"] == 200
        except AssertionError:
            raise APIResponseException(f"Bad response code in OpenCage response - {json_resp['status']['code']} ({json_resp['status']['message']})")

    def prepare_request(self, request) -> Request:
        request.url += f"&key={self.api_key}"
        return request.prepare()

    @staticmethod
    def process_response(response):
        json_resp = json.loads(response.text)
        if len(json_resp["results"]) == 0:
            raise APIResponseException("No location data found for this location name")
        return json_resp["results"][0]

    def create_request(self, data) -> Request:
        url = f"{self.api_base}?{urllib.parse.urlencode(data)}"
        request = Request('GET', url)
        return self.prepare_request(request)

    def translate_forwards(self, location):
        data = {
            "q": location
        }
        request = self.create_request(data)
        response = self.call(request)
        return self.process_response(response)


class ProxyCurlAPI(API):
    """Used for scraping LinkedIn in a reliable fashion
       Extract all company and employee data by just calling the respective endpoint with a LinkedIn page URL"""

    api_base = "https://nubela.co"
    api_key = "No api key yet"

    @staticmethod
    def confirm_response(response):
        """No response processing yet, as the usage of the api has not began"""
        return True

    def prepare_request(self, request) -> Request:
        request.headers["Authorization"] = f"Bearer {self.api_key}"
        return request

    @staticmethod
    def process_response(response):
        return json.loads(response.text)

    def create_request(self, data, path) -> Request:
        url = f"{self.api_base}{path}?{urllib.parse.urlencode(data)}"
        request = Request('GET', url)
        return self.prepare_request(request)

    def list_employees(self, url):
        """Get all employees of a company.
           URL: URL of the Linkedin Company Profile to crawl."""
        req = self.create_request({"url": url}, "/proxycurl/api/linkedin/company/employees/")
        return self.call(req)

    def company_info(self, url):
        """Get information about a company
           URL: URL of the Linkedin Company to crawl.
           DOCS: https://nubela.co/proxycurl/docs#enrichment-api-linkedin-company-profile-endpoint"""
        req = self.create_request({"url": url}, "/proxycurl/api/linkedin/company")
        return self.process_response(self.call(req))


class DebounceAPI(API):

    api_base = "https://api.debounce.io/v1/"
    api_key = "612e2b8c6a1ba"

    def confirm_response(self, resp):
        resp_json = json.loads(resp.text)
        if resp_json["success"] == '1' and resp.status_code == 200:
            return True
        else:
            if resp.status_code == 402:
                raise APIResourceException("Debounce payment required")
            elif resp.status_code == 429:
                raise APIResponseException("The rate limit has been exceeded.")
            else:
                raise APIResponseCodeException(resp.status_code, "Unknown status code exception")

    def create_request(self, data) -> Request:
        url = f"{self.api_base}?{urllib.parse.urlencode(data)}"
        request = Request('GET', url)
        return self.prepare_request(request)

    def prepare_request(self, request):
        request.url += f"&api={self.api_key}"
        return request.prepare()

    def process_response(self, response):
        return json.loads(response.text)

    def validate_email(self, email):
        req = self.create_request({"email": email})
        resp = self.call(req)
        result = self.process_response(resp)
        return result


if __name__ == "__main__":
    # x = DebounceAPI()
    # x.validate_email("art@bridge.media")
    y = DataForSEO()
    print(y.search_google_realtime("test query!"))
