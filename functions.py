from bravo import MapsData
from math import radians, cos, sin, asin, sqrt


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


def assert_maps_result(result, loc_lat, loc_long):
    # The result must contain these attributes for further processing
    if not result.get("url"):
        with open("companies_without_sites.txt", "a") as f:
            f.write(f"{result.get('title')}|{result.get('phone')}|{result.get('category')}|{result.get('address')}\n")

    # Result must have lat long
    if not "latitude" in result or not "longitude" in result:
        return False

    # The result must be within 150km of the location in query
    distance_from_location = haversine(loc_lat, loc_long,
                                       result["latitude"],
                                       result["longitude"])
    if distance_from_location > 150:
        print(f"Result {result['title']} was {distance_from_location}km away from search location origin, skipping")
        return False

    return True


def populate_maps_dataclass(result):
    m = MapsData(
        search_position=result["rank_absolute"],
        lat=result["latitude"],
        long=result["longitude"],
        rating=result["rating"]["value"] if result["rating"] else None,
        reviews=result["rating"]["votes_count"] if result["rating"] else None,
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
       Returns the company with the employees sorted from highest to lowest score"""

    first_choice = ["owner", "ceo", "chief", "principal", "founder"]
    second_choice = ["director", "md", "manager", "admin", "exec"]

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
