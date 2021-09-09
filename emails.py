import dns.resolver

from urllib.parse import urlparse
from api_interfaces import DebounceAPI

def email_combinations(first_name, last_name):
    """Generate a list of combinations to try"""

    combinations = []
    if not first_name:
        return []

    first_initial = first_name[0]
    if not last_name:
        combinations.extend([first_name, first_initial, first_name[:2], first_name[:3]])
    else:
        last_initial = last_name[0]
        combinations.extend([first_name,last_name,first_initial,last_initial,f"{last_initial}{first_initial}",f"{first_initial}{last_initial}",f"{first_name}{last_name}",f"{first_name}.{last_name}", f"{first_initial}{last_name}", f"{first_initial}.{last_name}", f"{first_name}{last_initial}", f"{first_name}.{last_initial}", f"{first_initial}{last_initial}", f"{first_initial}.{last_initial}", f"{last_name}{first_name}", f"{last_name}.{first_name}", f"{last_name}{first_initial}", f"{last_name}.{first_initial}", f"{last_initial}{first_name}", f"{last_initial}.{first_name}", f"{last_initial}{first_initial}", f"{last_initial}.{first_initial}", f"{first_name}-{last_name}", f"{first_initial}-{last_name}", f"{first_name}-{last_initial}", f"{first_initial}-{last_initial}", f"{last_name}-{first_name}", f"{last_name}-{first_initial}", f"{last_initial}-{first_name}", f"{last_initial}-{first_initial}", f"{first_name}_{last_name}", f"{first_initial}_{last_name}", f"{first_name}_{last_initial}", f"{first_initial}_{last_initial}", f"{last_name}_{first_name}", f"{last_name}_{first_initial}", f"{last_initial}_{first_name}", f"{last_initial}_{first_initial}"])

    return combinations

def find_email(employee, url):
    """Checks the domain to assure there is a mail server
       and then find the email using combinations of first and last name"""

    debounce = DebounceAPI()
    domain = urlparse(url).netloc
    if not url or type(url) != str:
        return False

    domain = domain.replace("www.", "")

    # First confirm that the domain does have a mail server
    try:
        answers = dns.resolver.resolve(domain, 'MX')
    except:
        print(f"No email server found for {domain}")
        return ""

    combinations = email_combinations(employee.first_name, employee.last_name)
    combinations = [f"{x}@{domain}" for x in combinations]

    accept_all_hits = 0
    for email in combinations:
        validation_resp = debounce.validate_email(email)
        print(f"Email {email} is {validation_resp['debounce']['result']}. Reason: {validation_resp['debounce']['reason']}")
        if validation_resp["debounce"]["code"] == "5":
            print("Found email!")
            return email
        elif validation_resp["debounce"]["code"] == "4":
            accept_all_hits += 1
            if accept_all_hits >= 20:
                return combinations[0]

    print(f"No email found for {employee.first_name} {employee.last_name} @ {domain}")
    return ""
