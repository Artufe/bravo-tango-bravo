import datetime
import dataclasses
import json
from dataclasses import dataclass
from fuzzywuzzy import process

from emails import find_email
from api_interfaces import OpenCageAPI, DataForSEO
from bravo import Company, Query, Employee, Address
from functions import assert_maps_result, populate_maps_dataclass, linkedin_result_extract, rank_employee


@dataclass
class MapsData:
    """Holds all data that is relating to the google maps result"""
    search_position: int
    lat: float
    long: float
    rating: float
    reviews: int
    type: str
    thumbnail: str


@dataclass
class Employee:
    """Representation of an employee in a company."""
    full_name: str
    first_name: str
    last_name: str
    position: str
    company: str
    email: str
    rank_score: int

    # Debug info
    search_title: str
    linkedin_url: str

    # For enabling the sort function and make it return highest rank first
    def __lt__(self, other):
        return self.rank_score > other.rank_score


@dataclass
class Query:
    """Represents a query generated from input"""
    sector: str
    location: str
    type: str
    started_at: datetime.datetime
    finished_at: datetime.datetime
    stats: dict

@dataclass
class Address:
    """Address data for a company"""
    address: str
    borough: str
    line1: str
    city: str
    zip: str
    region: str
    country_code: str

@dataclass
class Company:
    """Representation of a single company."""

    name: str
    website: str
    address: Address
    phone: str
    employees: [Employee]
    gmaps_data: MapsData


class Query:
    """ Represents a collection of results that resulted from a query."""
    def __init__(self, query_type, **kwargs):
        self.type = query_type
        self.started_at = datetime.datetime.utcnow()
        if self.type == "standard":
            self.location = kwargs["location"]
            self.sector = kwargs["sector"]

        self.id = self.new_query()

    def new_query(self):
        pass

    def standard_query(self, maps_results, search_results):
        """Tracks the results of a normal query. Assign result counts at each step."""




class FlowManager:

    def __init__(self):

        # Make the query object with starting values

        # Init the API modules
        # self.serp_api = SerpAPI() Depreciated
        self.dfs = DataForSEO()
        self.ocage = OpenCageAPI()

    def standard_query(self, sector, location):
        """This executes the normal order of maps -> google result processing -> emails"""
        query = Query()
        query.standard_query(sector, location)
        maps_results = self.gmaps_step(sector,location)
        companies = self.search_step(maps_results)
        # Get the email for each companies top employee
        for company in companies:
            if not company.employees:
                continue

            company.employees[0].email = find_email(company.employees[0], company.website)

        query.standard_query(maps_results, companies)

        return companies

    def gmaps_step(self, sector, location):
        """Perform a search withi  n google maps, with verification for each result
            returns: list of semi populated Company dataclasses"""
        # Get lat long of query location from OpenCageAPI
        location_data = self.ocage.translate_forwards(location)
        loc_lat = location_data["geometry"]["lat"]
        loc_long = location_data["geometry"]["lng"]

        # Call to Google maps, with lat long and zoom levels to use as the origin of the search
        maps_results = self.dfs.search_maps(f"{sector} in {location}", loc_lat, loc_long, 11)

        refined_results = []
        processed_companies = set()
        for result in maps_results:
            # Skip duplicate companies, as the google maps api
            # sometimes returns multiple results for same company
            if result["title"] in processed_companies or result["url"] in processed_companies:
                continue
            processed_companies.add(result["title"])
            processed_companies.add(result["url"])

            if not assert_maps_result(result, loc_lat, loc_long):
                continue

            # Clean up the company title a little
            if " in " in result["title"]:
                # Strips stuff like "Mortgage medics in Brighton" to "Mortgage medics"
                result["title"].split(" in ")[0]
            # Replace " & " with " and "
            result["title"].replace(" & ", " and ") if " & " in result["title"] else result["title"]

            addr = Address(address=result["address"], borough=result["address_info"]["borough"],
                           line1=result["address_info"]["address"], city=result["address_info"]["city"],
                           zip=result["address_info"]["zip"], region=result["address_info"]["region"],
                           country_code=result["address_info"]["country_code"])
            comp = Company(name=result["title"],
                           website=result["url"],
                           address=addr,
                           phone=result["phone"],
                           gmaps_data=populate_maps_dataclass(result),
                           employees=[])
            refined_results.append(comp)

        return refined_results

    def search_step(self, companies):
        # Submits the search tasks, and adds a task reference to the company object
        for company in companies:
            company.search_task = self.dfs.search_google(f"inurl:uk.linkedin.com/in {company.name}")["id"]
            # with open("tasks_store.txt", "a") as f:
            #     f.write(f"{company.name}|||{company.search_task}\n")

            # with open("tasks_store.txt", "r") as f:
            #     lines = [x.strip() for x in f.readlines()]
            #     for line in lines:
            #         if line.split("|||")[0] == company.name:
            #             company.search_task = line.split("|||")[-1]

        # Retrieves the search task results
        for company in companies:
            try:
                results = self.dfs.get_task(company.search_task)
                del company.search_task
                company.employees = self.process_search_results(results, company)
            except AttributeError:
                pass

        self.query.finished_at = datetime.datetime.utcnow()
        return companies

    def process_search_results(self, results, company):
        """ Loops over a list of google search results, in a attempt to find employees of the given company """
        employees = []

        if not results:
            return employees

        # Track the results in a list, as they will be iterated over more than once
        extracted_results = []
        for result in results:
            if result["type"] != 'organic':
                continue

            # Attempt to get name, positon, company from pre-snippet or title.
            name, position, extracted_company = linkedin_result_extract(result)
            if not extracted_company:
                continue

            extracted_results.append((name, position, extracted_company,
                                      result["title"], result["url"], result["rank_absolute"]))

        best_match = process.extractOne(company.name, [x[2] for x in extracted_results])
        if not best_match:
            return employees

        # If ratio is below 70, the uncertainty is too high
        # And no employees can be found with confidence
        if best_match[1] < 70:
            return employees

        for name, position, extracted_company, title, url, search_position in extracted_results:
            if extracted_company == best_match[0]:
                employee = Employee(
                    first_name=name.split(" ")[0] if name else None,
                    last_name=name.split(" ")[-1] if name else None,
                    full_name=name,
                    position=position, company=extracted_company,
                    email="",
                    rank_score=rank_employee(search_position, position),
                    search_title=title,
                    linkedin_url=url)
                if employee.first_name == employee.last_name:
                    employee.last_name = None
                employees.append(employee)

        employees.sort()
        return employees

class InputManager:

    def ask_input(self, question):
        """Ask the user a simple question, with a confirmation"""
        answer = input(question + "\n")
        confirm = input(f"You typed: '{answer}'\nIs this correct? (y/n)\n")
        if confirm.lower() != 'y':
            answer = self.ask_input(question)
        return answer

    def cli_input_method(self):
        """Manual way to ask the user for the parameters of the search direct from CLI."""

        sector = self.ask_input("Type in the sector of business you want to target (removal company, plumbers, etc)")
        location = self.ask_input("Type in the location you want to target (Brighton, London, etc)")

        print(f"The final query built: '{sector} in {location}'")

        return FlowManager(sector, location, "full").standard_query()


class OutputManager:

    def output_json(self, results, file_name):
        json_array = []
        for x in results:
            json_array.append(dataclasses.asdict(x))

        with open(file_name, "w") as f:
            f.write(json.dumps(json_array))

    def output_csv(self, res, file_name, short_format=False):
        csv_lines = []
        if short_format:
            csv_headers = "Company name~Website~Employee name~Employee position~Employee email~Employee company"
        else:
            csv_headers = "Company name~Website~Address~Phone~Employee name~Employee position~Employee email~Employee company~Employee result title~Employee linkedin page~google_maps_data~all_employees"
        csv_lines.append(csv_headers
                         )
        for c in res:
            if short_format:
                if len(c.employees) >= 1:
                    csv_lines.append(f"{c.name}~{c.website}~{c.employees[0].full_name}~{c.employees[0].email}~{c.employees[0].company}")
                else:
                    csv_lines.append(f"{c.name}~{c.website}~~~")
            else:
                all_employees = []
                for e in c.employees:
                    all_employees.append(dataclasses.asdict(e))

                if len(c.employees) >= 1:
                    csv_lines.append(
                        f"{c.name}~{c.website}~{c.address}~{c.phone}~{c.employees[0].full_name}~{c.employees[0].position}~{c.employees[0].email}~{c.employees[0].company}~{c.employees[0].search_title}~{c.employees[0].linkedin_url}~{json.dumps(dataclasses.asdict(c.gmaps_data))}~{json.dumps(all_employees)}")
                else:
                    csv_lines.append(
                        f"{c.name}~{c.website}~{c.address}~{c.phone}~~~~~~{json.dumps(dataclasses.asdict(c.gmaps_data))}~{json.dumps(all_employees)}")

        with open(file_name, "w") as f:
            f.write("\n".join(csv_lines))


if __name__ == "__main__":
    x = FlowManager("Mortgage broker", "Brighton", 1)
    res = x.standard_query()
    o = OutputManager()
    o.output_csv(res, "query1.csv", True)