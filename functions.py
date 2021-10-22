from math import radians, cos, sin, asin, sqrt
from data_models import *


def haversine(lat1, lon1, lat2, lon2):
    """The haversine formula implementation for calculating the distance between two points on earth
       a = sin²(Δφ/2) + cos φ1 ⋅ cos φ2 ⋅ sin²(Δλ/2)
       c = 2 ⋅ atan2( √a, √(1−a) )
       d = R ⋅ c
       where	φ is latitude, λ is longitude, R is earth’s radius in km"""
    R = 6372.8

    dLat = radians(lat2 - lat1)
    dLon = radians(lon2 - lon1)
    lat1 = radians(lat1)
    lat2 = radians(lat2)

    a = sin(dLat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dLon / 2) ** 2
    c = 2 * asin(sqrt(a))

    return R * c


def assert_maps_result(result, loc_lat, loc_long, distance=150):
    # The result must contain these attributes for further processing
    if not result.get("url"):
        with open("companies_without_sites.txt", "a") as f:
            f.write(f"{result.get('title')}|{result.get('phone')}|{result.get('category')}|{result.get('address')}\n")

    # Result must have lat long
    if not "latitude" in result or not "longitude" in result:
        return False

    # Filter companies house from results, this comes up occasionally
    if result["url"] == "https://beta.companieshouse.gov.uk/":
        return False

    # The result must be within 150km of the location in query
    distance_from_location = haversine(loc_lat, loc_long,
                                       result["latitude"],
                                       result["longitude"])
    
    if distance_from_location > distance:
        print(f"Result {result['title']} was {distance_from_location}km away from search location origin, skipping")
        return False

    return True


def populate_maps_dataclass(result):
    m = MapsData(
        search_position=result["rank_absolute"],
        lat=result["latitude"],
        long=result["longitude"],
        rating=result["rating"]["value"] if result["rating"] else 0,
        reviews=result["rating"]["votes_count"] if result["rating"] else 0,
        type=result["category"],
        thumbnail=result["main_image"]
    )

    return m


def linkedin_result_extract(result):
    """Extract the 3 main components from a search result of a employee
       Name, position and company"""

    # Clean up the title, removing linkedin from it
    if " | LinkedIn" in result["title"]:
        result["title"] = result["title"].replace(" | LinkedIn", "")
    elif " - LinkedIn" in result["title"]:
        result["title"] = result["title"].replace(" - LinkedIn", "")

    # Split title and pre-snippet into parts
    title = result["title"].split(" - ")
    if result["pre_snippet"]:
        pre_snippet = result["pre_snippet"].split(" · ")
    else:
        pre_snippet = []

    if len(pre_snippet) == 3 and len(title) >= 2:
        # If both are found, chances of getting all components right are high
        name = title[0]
        company = pre_snippet[-1]
        position = pre_snippet[1]
        # print(f"Best case {pre_snippet}, {title}")
        return name, position, company
    elif len(pre_snippet) >= 3:
        # If only snippet found, we can only get the location, position, company
        location, position, company = pre_snippet
        # print(f"Pre-snippet only {pre_snippet}")
        return None, position, company
    elif len(title) == 3:
        # print(f"Title only {title}")
        return title
    else:
        # print("Nothing...")
        return None, None, None


def rank_employee(search_position, position):
    """Scores employee based on search position and keywords within position
       Higher score equals more likely the persons higher ranked within the company
       Returns the employee rank score"""

    first_choice = ["owner", "ceo", "chief", "principal", "founder"]
    second_choice = ["director", "md", "manager", "admin", "exec", "president"]

    rank_score = 1000 - search_position

    if position:

        for choice in first_choice:
            if choice in position.lower():
                rank_score += 50
                break

        for choice in second_choice:
            if choice in position.lower():
                rank_score += 25
                break
    return rank_score


def company_from_database(company_name):
    """Get a company from the database and load it into a dataclass representation"""
    model = CompanyModel.get(CompanyModel.name == company_name)
    address = Address(address=model.full_address, borough=model.borough, line1=model.line1,
                      city=model.city, zip=model.zip, region= model.region,
                      country_code=model.country_code)

    # Populate maps data
    maps_model = MapsDataModel.get_or_none(MapsDataModel.company_id == model.id)
    if maps_model:
        maps_data = MapsData(search_position=maps_model.search_position, lat=maps_model.lat,
                             long=maps_model.long, rating=maps_model.rating,
                             reviews=maps_model.reviews, type=maps_model.type, thumbnail=maps_model.thumbnail)
    else:
        maps_data = None

    # Assign the address, maps data and basic fields to company
    company = Company(name=company_name, website=model.website, phone=model.phone,
                      address=address, employees=[], gmaps_data=maps_data, done=True)

    # Populate employees
    employees = EmployeeModel.select().where(EmployeeModel.company_id==model.id)
    for emp in employees:
        employee = Employee(full_name=emp.full_name, first_name=emp.first_name, last_name=emp.last_name,
                            position=emp.position, company=emp.extracted_company, email=emp.email,
                            rank_score=emp.rank_score, search_title=emp.search_title,
                            linkedin_url=emp.linkedin_url, pre_snippet=emp.pre_snippet)
        company.employees.append(employee)

    return company


def load_query_from_db(query_id):
    query = QueryModel.get(QueryModel.id == query_id)
    companies = CompanyModel.select().where(CompanyModel.query == query)
    comp_dc = []
    for comp in companies:
        comp_dc.append(company_from_database(comp.name))

    return query, comp_dc


if __name__ == "__main__":
    print(load_query_from_db(9))