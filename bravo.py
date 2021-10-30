import datetime
import json
import csv
import sys
import urllib.parse
from dataclasses import asdict
from fuzzywuzzy import process
import pika
import gspread

from emails import find_email
from api_interfaces import OpenCageAPI, DataForSEO
from functions import assert_maps_result, populate_maps_dataclass, \
    linkedin_result_extract, rank_employee, MapsData, company_from_database, haversine, \
    load_query_from_db
from data_models import *


class Query:
    """Represents a user-made query which ties input to output
       and stats collected along the way"""

    def __init__(self, query_type, **kwargs):
        self.type = query_type
        self.started_at = datetime.datetime.utcnow()
        if self.type == "standard":
            self.query = QueryModel.create(type=self.type, location=kwargs["location"],
                                           sector=kwargs["sector"])
        elif self.type == "from_csv":
            self.query = QueryModel.create(type=self.type)

        # RabbitMQ init
        # Connect with pika to RabbitMQ in localhost
        self.rmq_connection_params = pika.URLParameters('amqp://arthur:FlaskTubCupp@localhost:5672/%2F')
        self.connection = pika.BlockingConnection(self.rmq_connection_params)
        # Initialize a channel
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange='B2B', durable=True)
        self.channel.queue_declare(queue='contacts')
        self.channel.queue_bind(exchange='B2B', queue='contacts')

    def save_results_db(self, companies):
        for company in companies:
            if company.done or not company.website:
                continue

            if company.website and type(company.website) == str:
                company.website = company.website.lower()
                company.website = company.website.replace("https://", "").replace("http://", "")

            comp_instance = CompanyModel.create(name=company.name,
                                                website=company.website,
                                                phone=company.phone,
                                                full_address=company.address.address,
                                                borough=company.address.borough,
                                                line1=company.address.line1,
                                                city=company.address.city,
                                                zip=company.address.zip,
                                                region=company.address.region,
                                                country_code=company.address.country_code,
                                                query=self.query)

            if company.gmaps_data:
                # Save the maps data
                MapsDataModel.create(search_position=company.gmaps_data.search_position,
                                     lat=company.gmaps_data.lat, long=company.gmaps_data.long,
                                     rating=company.gmaps_data.rating,
                                     reviews=company.gmaps_data.reviews,
                                     type=company.gmaps_data.type,
                                     thumbnail=company.gmaps_data.thumbnail,
                                     company=comp_instance)
            # Save all of the employees
            for employee in company.employees:
                EmployeeModel.create(full_name=employee.full_name,
                                     first_name=employee.first_name,
                                     last_name=employee.last_name,
                                     position=employee.position,
                                     extracted_company=employee.company,
                                     company=comp_instance,
                                     email=employee.email,
                                     rank_score=employee.rank_score,
                                     search_title=employee.search_title,
                                     pre_snippet=employee.pre_snippet,
                                     linkedin_url=employee.linkedin_url)

    def push_to_rmq(self, companies):
        for comp in companies:
            self.channel.basic_publish(exchange='B2B', routing_key='contacts', body=comp.website)

    def standard_query(self, maps_results, search_results):
        """Saves the results of a normal query."""
        self.query.finished_at = datetime.datetime.utcnow()

        self.query.maps_results = len(maps_results)
        self.query.search_results = len(search_results)
        self.query.save()

        # Save all of the companies
        self.save_results_db(search_results)

        # Put in queue for scrapy spider to process further
        self.push_to_rmq(search_results)

    def from_csv(self, search_results):
        self.query.finished_at = datetime.datetime.utcnow()

        self.query.search_results = len(search_results)
        self.query.save()

        # Save all of the companies
        self.save_results_db(search_results)

        # Put in queue for scrapy spider to process further
        self.push_to_rmq(search_results)


def process_search_results(results, company):
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

        extracted_results.append((name, position, extracted_company, result))

    best_match = process.extractOne(company.name, [x[2] for x in extracted_results])
    if not best_match:
        return employees

    # If ratio is below 70, the uncertainty is too high
    # And no employees can be found with confidence
    if best_match[1] < 70:
        return employees

    for name, position, extracted_company, result in extracted_results:
        if extracted_company == best_match[0]:
            employee = Employee(
                first_name=name.split(" ")[0] if name else "",
                last_name=name.split(" ")[-1] if name else "",
                full_name=name if name else "",
                position=position, company=extracted_company,
                email="",
                rank_score=rank_employee(result["rank_absolute"], position),
                search_title=result["title"],
                linkedin_url=result["url"],
                pre_snippet=result["pre_snippet"])
            if employee.first_name == employee.last_name:
                employee.last_name = ""
            employees.append(employee)

    employees.sort()
    return employees


class FlowManager:

    def __init__(self):
        # Init the API modules
        self.dfs = DataForSEO()
        self.ocage = OpenCageAPI()
        
        # A list of sites to ignore when trying to find a company website
        # These sites will be indexed in google, as they are company information sites (eg. Pomanda)
        with open("company_index_sites.txt", "r") as f:
            self.company_index_sites = [x.strip() for x in f.readlines()]

        self.searched_domains = [x.website.replace("www.", "").lower() for x in
                                 CompanyModel.select(CompanyModel.website)]

    def standard_query(self, sector, location):
        """This executes the normal order of maps -> google result processing -> emails"""
        query = Query("standard", sector=sector, location=location)

        maps_results = self.gmaps_step(sector, location)
        companies = self.search_step(maps_results)

        # Get the email for each companies top employee
        for company in companies:
            if company.employees:
                company.employees[0].email = find_email(company.employees[0], company.website)

        query.standard_query(maps_results, companies)

        return companies

    def from_csv(self, csv_file_location):
        self.query = Query("from_csv")

        companies = InputManager().csv_import(csv_file_location)
        companies = self.find_website(companies)
        companies = self.search_step(companies)

        # Get the email for each companies top employee
        for company in companies:
            if company.employees and not company.done:
                company.employees[0].email = find_email(company.employees[0], company.website)

        self.query.from_csv(companies)

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
            # Clean up the website
            website = result["url"].replace("https://", "").replace("http://", "")
            if website[-1] == "/":
                website = website[:-1]

            # Clean up the company title a little
            if " in " in result["title"]:
                # Strips stuff like "Mortgage medics in Brighton" to "Mortgage medics"
                result["title"] = result["title"].split(" in ")[0]

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

        for i, company in enumerate(companies):
            if company.website:
                # Check if the company has already been done before
                if CompanyModel.get_or_none(CompanyModel.name == company.name):
                    companies[i] = company_from_database(company.name)
                    print(f"{company.name} has already been done before, data loaded from DB")
                else:
                    # Submit the search task, and adds a task reference to the company object
                    company.search_task = self.dfs.search_google(f"inurl:uk.linkedin.com/in {company.name}")["id"]

        # Retrieves the search task results
        for company in companies:
            if not company.done and company.website:
                if not company.search_task:
                    print(f"No search results for company {company.name}")
                    continue
                try:
                    results = self.dfs.get_task(company.search_task)
                    del company.search_task
                    company.employees = process_search_results(results, company)
                except AttributeError:
                    print("Attribute exception while retrieving search results")


        return companies

    def find_website(self, companies):
        """Two staged search for a company website, for companies that dont have one.
           First step is to launch a maps search, trying to find a direct result (1 result)
           Second step is to search google, ignoring all company index sites."""

        search_tasks = []
        for company in companies:
            # If website already there, or a the company already went through gmaps - skip
            if company.website or company.gmaps_data or company.done:
                continue

            # First step
            maps_results = self.dfs.search_maps(company.name)
            if len(maps_results) == 1 and assert_maps_result(maps_results[0], 54.249532, -4.119393, 543):
                print(f"Direct maps match found for {company.name}")
                # A direct result found, woohoo
                if maps_results[0]["url"]:
                    company.website = urllib.parse.urlsplit(maps_results[0]["url"].replace("www.", "")).netloc
                    company.gmaps_data = populate_maps_dataclass(maps_results[0])
                    continue

            # Second step
            search_tasks.append((self.dfs.search_google(f"{company.name} United Kingdom"), company.name))

        for task, comp_name in search_tasks:
            search_results = self.dfs.get_task(task)
            for result in search_results[:50]:
                if result["type"] != "organic":
                    continue

                result_domain = urllib.parse.urlsplit(result["url"]).netloc
                result_domain = result_domain.replace("www.", "")

                if result_domain in self.company_index_sites:
                    print(f"Skipped index site: {result_domain}")
                    continue
                elif result_domain in self.searched_domains:
                    print(f"Skipped already saved site: {result_domain}")
                    continue
                elif ".gov.uk" in result_domain:
                    print(f"Skipped .gov site: {result_domain}")
                    continue

                for company in companies:
                    if company.name == comp_name:
                        company.website = result_domain
                        print(f"Found website: {result_domain}")
                        self.searched_domains.append(result_domain)
                break

            if not company.website:
                company.website = ""

        return companies


def create_basic_company(name, website):
    comp = Company(name=name,
                   website=website,
                   address=Address(address=None, borough=None, line1=None, city=None, zip=None, region=None,
                                   country_code=None),
                   phone='',
                   gmaps_data=None,
                   employees=[])
    return comp


class InputManager:

    def __init__(self):
        # Instantiate output manager for saving outputs
        self.output = OutputManager()

    def parse_input(self):
        # Parse the CLI arguments passed in and execute the correct flow
        args = sys.argv[1:]

        if args[0] == "standard":
            output_csv = args[1]
            self.standard_query_interactive(output_csv)
        elif args[0] == "csv":
            input_csv = args[1]
            manager = FlowManager()
            result = manager.from_csv(input_csv)
            
            if len(args) == 4 and args[2] == "gsheets":
                share_email = args[3]
                self.output.output_gsheets(manager.query.query.id, share_email)
            elif len(args) == 3:
                self.output.output_csv(result, output_csv, True)
            else:
                print("""No output specified as last argument. 
                Not generating a output file upon completion! 
                (CTRL + C NOW IF YOU DONT WANT THIS)
                Please specify eithe a file name for CSV file output, or Email for Google sheets output.
                Example:
                 python3 bravo.py csv ./input_file ./output.csv
                 python3 bravo.py csv ./input.csv gsheets greg@brdige.media""")

    def csv_import(self, csv_file_location):
        csv_lines = []
        with open(csv_file_location, "r") as f:
            csvreader = csv.reader(f)
            for line in csvreader:
                csv_lines.append(line)

        companies = []
        if len(csv_lines[0]) == 2:
            for name, website in csv_lines:
                companies.append(self.create_company_basic(name, website))
        else:
            name_index, website_index = False, False
            try:
                name_index = csv_lines[0].index("Company Name")
                website_index = csv_lines[0].index("Company Website")
            except ValueError:
                try:
                    name_index = csv_lines[0].index("company_name")
                    website_index = csv_lines[0].index("company_website")
                except ValueError:
                    print("name index and website index not found")

            if name_index and website_index:
                print(f"Company name col: {name_index}\nCompany website col: {website_index}")
                for line in csv_lines[1:]:
                    if line[name_index].strip():
                        companies.append(create_basic_company(line[name_index].strip(), line[website_index].strip()))

        return list(filter(None, companies))

    def ask_input(self, question):
        """Ask the user a simple question, with a confirmation"""
        answer = input(question + "\n")
        confirm = input(f"You typed: '{answer}'\nIs this correct? (y/n)\n")
        if confirm.lower() != 'y':
            answer = self.ask_input(question)
        return answer

    def standard_query_interactive(self, save_file):
        """Manual way to ask the user for the parameters of the search direct from CLI."""

        sector = self.ask_input("Type in the sector of business you want to target (removal company, plumbers, etc)")
        location = self.ask_input("Type in the location you want to target (Brighton, London, etc)")

        print(f"The final query built: '{sector} in {location}'")

        result = FlowManager().standard_query(sector, location)
        self.output.output_csv(result, save_file)


class OutputManager:

    def output_json(self, results, file_name):
        json_array = []
        for x in results:
            json_array.append(asdict(x))

        with open(file_name, "w") as f:
            f.write(json.dumps(json_array))

    def output_csv(self, res, file_name, short_format=False):
        csv_lines = []

        if short_format:
            csv_headers = "Company name~Website~Employee name~Employee position~Employee email~Employee company"
        else:
            csv_headers = "Company name~Website~Address~Phone~Employee name~Employee position~Employee email~Employee company~Employee linkedin page~google_maps_data~all_employees"
        csv_lines.append(csv_headers)

        for c in res:
            if not c.website:
                continue

            if c.gmaps_data:
                gmaps_data = json.dumps(asdict(c.gmaps_data))
            else:
                gmaps_data = ""

            if short_format:
                if len(c.employees) >= 1:
                    csv_lines.append(
                        f"{c.name}~{c.website}~{c.employees[0].full_name}~{c.employees[0].position}~{c.employees[0].email}~{c.employees[0].company}")
                else:
                    csv_lines.append(f"{c.name}~{c.website}~~~~")
            else:
                all_employees = []
                for e in c.employees:
                    all_employees.append(asdict(e))

                if len(c.employees) >= 1:

                    csv_lines.append(
                        f"{c.name}~{c.website}~{c.address.address}~{c.phone}~{c.employees[0].full_name}~{c.employees[0].position}~{c.employees[0].email}~{c.employees[0].company}~{c.employees[0].linkedin_url}~{gmaps_data}~{json.dumps(all_employees)}")
                else:
                    csv_lines.append(
                        f"{c.name}~{c.website}~{c.address}~{c.phone}~~~~~~{gmaps_data}~{json.dumps(all_employees)}")

        with open(file_name, "w") as f:
            f.write("\n".join(csv_lines))

    def output_gsheets(self, query_id, share_email):
        """ Takes a query ID and creates a new Google Sheet with the results"""

        query = QueryModel.get_or_none(QueryModel.id == query_id)
        if not query:
            print("Query not found")
            return
        companies = CompanyModel.select().where(CompanyModel.query == query)
        all_employees = EmployeeModel.select().join(CompanyModel).where(CompanyModel.query == query)

        # Connect to gsheets using a service account connection key file in home dir
        gc = gspread.service_account(filename="/home/arthur/bravo-tango-bravo-305e7e39fd14.json")

        # Open a sheet from a spreadsheet in one go
        if query.type == "standard":
            sh = gc.create(f"[B2B] {query.sector} in {query.location}")
        elif query.type == "from_csv":
            sh = gc.create(f"[B2B] CSV import #{query.id}")
        else:
            sh = gc.create(f"[B2B] Unknown query type (TODO) #{query.id}")

        # Share with myself
        sh.share(share_email, perm_type='user', role='writer')

        # Setup the required worksheets, delete the default sheet,
        sum_sheet = sh.add_worksheet(title="Summary", rows="100", cols="20")
        com_sheet = sh.add_worksheet(title="Companies", rows="100", cols="20")
        emp_sheet = sh.add_worksheet(title="Employees", rows="100", cols="20")
        sh.del_worksheet(sh.sheet1)

        # Populate the Companies sheet with headers and data
        com_rows = [["Company Name", "Website", "Contact Email",
                     "Employees found", "Phone", "Full Address",
                     "Linkedin", "Twitter", "Facebook", "Instagram",
                     "Youtube", "Maps Rating", "Maps Reviews", "Maps Position"]]
        for comp in companies:
            employees = EmployeeModel.select().where(EmployeeModel.company == comp).count()
            maps_data = MapsDataModel.get_or_none(MapsDataModel.company == comp)
            single_row = [comp.name, comp.website, comp.contact_email,
                             employees, comp.phone, comp.full_address,
                             comp.linkedin, comp.twitter, comp.facebook,
                             comp.instagram, comp.youtube]
            if maps_data:
                single_row.extend([maps_data.rating, maps_data.reviews, f"{maps_data.lat},{maps_data.long}"])

            com_rows.append(single_row)

        com_sheet.update(f"A1:N{len(com_rows)}", com_rows)

        # Populate the employees table with headers and all employees of the companies in the query
        emp_rows = [["Company Name", "Full Name", "Position",
                     "Email", "Rank Score", "Linkedin URL"]]

        for emp in all_employees:
            emp_rows.append([emp.company.name, emp.full_name, emp.position, emp.email, emp.rank_score, emp.linkedin_url])

        emp_sheet.update(f"A1:F{len(emp_rows)}", emp_rows)

        # Populate the summary sheet with stats for the query
        # And short format table of all emails found and their most vital info
        emails_found = all_employees.select().where(EmployeeModel.email != '').count()
        if emails_found == 0:
            email_rate = 0
        else:
            email_rate = (companies.count()/emails_found) * 100

        sum_sheet.update("A1:D1", [[f"Results: {companies.count()}",
                                    f"Emails: {emails_found}",
                                    f"Email Rate: {email_rate}",
                                    f"Employees: {all_employees.count()}"]])
        time_taken = query.finished_at - query.started_at
        minutes_taken = time_taken.seconds//60
        sum_sheet.update("I4:K6", [[None,                                                "Query Stats:",                                  None],
                                [f"Launched:",                                            "Finished:",                               "Time Taken:"],
               [f"{query.started_at.strftime('%d/%m/%Y, %H:%M:%S')}", f"{query.finished_at.strftime('%d/%m/%Y, %H:%M:%S')}", f"{minutes_taken} minutes"]])

        # Short format table of results with emails
        sum_rows = [["Email", "First Name", "Last Name", "Position", "Company"]]

        for employee in all_employees.select().where(EmployeeModel.email != ''):
            sum_rows.append([employee.email, employee.first_name,
                             employee.last_name, employee.position,
                             employee.company.name])
        sum_sheet.update(f"A3:E{len(sum_rows)+2}", sum_rows)


class Demo:

    def __init__(self):
        self.worker = FlowManager()

    def find_website_single(self, company_name):
        comp = create_basic_company(company_name, "")
        comp = self.worker.find_website([comp])[0]


if __name__ == "__main__":
    InputManager().parse_input()
    # OutputManager().output_gsheets(32, "greg@bridge.media")