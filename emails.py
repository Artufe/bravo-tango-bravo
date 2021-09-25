import dns.resolver

from urllib.parse import urlparse
from api_interfaces import DebounceAPI


def email_combinations(first, last):
    """Generate a list of combinations to try"""

    combinations = []
    if not first:
        return []

    # First name first letter
    f = first[0]
    if not last:
        combinations.extend([f"{first}",
                            f"{f}"])
    else:
        # Last name first letter
        l = last[0]
        combinations.extend([f"{first}",
                            f"{f}{last}",
                            f"{first}.{last}",
                            f"{first}{last}",
                            f"{last}",
                            f"{first}{l}",
                            f"{f}.{last}",
                            f"{last}{f}",
                            f"{first}_{last}"])

    return combinations


def find_email(employee, url):
    """Checks the domain to assure there is a mail server
       and then find the email using combinations of first and last name"""
    if not url or type(url) != str:
        return ""

    debounce = DebounceAPI()

    # Strip to just domain part of the url, if needed
    if "://" in url:
        domain = urlparse(url).netloc
    else:
        domain = url

    domain = domain.replace("www.", "").lower()

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
        print(
            f"Email {email} is {validation_resp['debounce']['result']}. Reason: {validation_resp['debounce']['reason']}")
        if validation_resp["debounce"]["code"] == "5":
            print(f"Email found - {email}")
            return email
        elif validation_resp["debounce"]["code"] == "4":
            accept_all_hits += 1
            if accept_all_hits >= 5:
                return combinations[0]
        # Uncomment this if you needs results FAST
        # elif validation_resp["debounce"]["code"] == "7":
        #     return ""

    print(f"No email found for {employee.first_name} {employee.last_name} @ {domain}")
    return ""
