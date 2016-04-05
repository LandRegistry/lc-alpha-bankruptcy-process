import requests
import json
import logging
import urllib.parse
import kombu
import traceback
import getpass
import re

CONFIG = {}


def get_username():
    return "{}({})".format(
        getpass.getuser(),
        CONFIG['APPLICATION_NAME']
    )


def get_headers(headers=None):
    if headers is None:
        headers = {}

    headers['X-LC-Username'] = get_username()


class BankruptcyProcessError(Exception):
    def __init__(self, value):
        self.value = value
        super(BankruptcyProcessError, self).__init__(value)

    def __str__(self):
        return repr(self.value)


def save_error(errors, response, route, id):
    error = {
        "uri": route,
        "status_code": response.status_code,
        "message": response.content.decode('utf-8'),
        "registration_no": id
    }
    errors.append(error)


def get_simple_name_matches(debtor_name):
    forenames = " ".join(debtor_name['forenames'])
    surname = debtor_name['surname']
    name = forenames + ' ' + surname

    url = CONFIG['LEGACY_DB_URI'] + '/proprietors?name=' + urllib.parse.quote(name.upper())
    response = requests.get(url, headers=get_headers())
    name_search_result = response.json()

    if response.status_code != 200:
        raise BankruptcyProcessError("Unexpected response {} from {}".format(response.status_code, url))

    logging.info('Retrieved %d name hits', len(name_search_result))
    return name_search_result


def get_complex_name_matches(number):
    url = CONFIG['LEGACY_DB_URI'] + '/proprietors?name=' + str(number) + "&complex=Y"
    response = requests.get(url, headers=get_headers())

    if response.status_code != 200:
        raise BankruptcyProcessError("Unexpected response {} from {}".format(response.status_code, url))

    name_search_result = response.json()
    logging.info('Retrieved %d name hits', len(name_search_result))
    return name_search_result


def convert_registration(registration):
    debtor = get_debtor_party(registration)
    logging.debug(debtor['names'])

    match = re.match("(.*) (?:no)? (\d+ of \d{4})", debtor['case_reference'], re.IGNORECASE)
    if match:
        body = match.group(1)
        reference = match.group(2)
    else:
        match = re.match("(.*) (?:ref|no) (.*)", debtor['case_reference'], re.IGNORECASE)
        if match:
            body = match.group(1)
            reference = match.group(2)
        else:
            reference = debtor['case_reference']
            body = ""

    result = {
        'debtor_names': [],
        'legal_body_ref': reference,  # should be num of year
        'trading_name': "",
        'status': registration['status'],
        "class_of_charge": registration['class_of_charge'],
        'key_number': registration['applicant']['key_number'],
        'legal_body': body,
        'registration': {
            'number': registration['registration']['number'],
            'date': registration['registration']['date']
        },
        'occupation': '',
        'residence': [],
        'application_ref': registration['applicant']['reference']
    }

    if 'occupation' in debtor:
        result['occupation'] = debtor['occupation']

    if debtor['names'][0]['type'] == 'Private Individual':
        for n in debtor['names']:
            if n['type'] == 'Private Individual':
                result['debtor_names'].append({
                    'forenames': n['private']['forenames'],
                    'surname': n['private']['surname']
                })
    elif debtor['names'][0]['type'] == 'Complex Name':
        result['complex'] = {
            'number': debtor['names'][0]['complex']['number'],
            'name': debtor['names'][0]['complex']['name']
        }

    for address in [a for a in debtor['addresses'] if a['type'] == 'Residence']:
        result['residence'].append({
            'address_lines': address['address_lines'],
            'county': address['county'],
            'postcode': address['postcode']
        })

    return result


def post_bankruptcy_search(registration, name_search_result):
    data = {
        'registration': convert_registration(registration),
        'iopn': name_search_result
    }

    uri = CONFIG['LEGACY_DB_URI'] + '/debtors'
    headers = {'Content-Type': 'application/json'}
    logging.info('Posting combined dataset to LegacyDB')
    logging.debug(data)
    return requests.post(uri, data=json.dumps(data), headers=get_headers(headers))


def get_entries_for_process(date):
    logging.info('Get entries for date %s', date)
    url = CONFIG['REGISTER_URI'] + '/registrations/' + date
    response = requests.get(url, headers=get_headers())
    if response.status_code == 200:
        return response.json()
    elif response.status_code != 404:
        raise BankruptcyProcessError("Unexpected response {} from {}".format(response.status_code, url))
    return []


def get_registration(date, number):
    uri = "{}{}/{}".format(CONFIG['REGISTER_URI'] + '/registrations/', date, number)
    response = requests.get(uri, headers=get_headers())
    if response.status_code == 200:
        logging.info("Received response 200 from /registrations")
        return response.json()
    else:
        raise BankruptcyProcessError("Unexpected response {} from {}".format(response.status_code, uri))


def get_debtor_party(registration):
    for party in registration['parties']:
        if party['type'] == 'Debtor':
            return party

    return None


def get_debtor_name_matches(name):
    # Complex Name    Coded Name
    if name['type'] == 'Complex Name':
        return get_complex_name_matches(name['complex']['number'])
    elif name['type'] == 'Private Individual':
        return get_simple_name_matches(name['private'])
    else:
        raise BankruptcyProcessError('Unexpected name type: {}'.format(name['type']))


def lead_name_changed(current, previous):
    curr_debtor = None
    prev_debtor = None

    for party in current['parties']:
        if party['type'] == 'Debtor':
            curr_debtor = party

    for party in previous['parties']:
        if party['type'] == 'Debtor':
            prev_debtor = party

    if curr_debtor is None or prev_debtor is None:
        raise BankruptcyProcessError("On amendment, cannot find both debtors")

    curr_name = ' '.join(curr_debtor['names'][0]['private']['forenames']) + ' ' + curr_debtor['names'][0]['private']['surname']
    prev_name = ' '.join(prev_debtor['names'][0]['private']['forenames']) + ' ' + prev_debtor['names'][0]['private']['surname']

    if curr_name.upper() != prev_name.upper():
        return True
    return False


def process_entry(producer, entry):
    return_ok = True

    for item in entry['data']:
        date = item['date']
        number = item['number']
        coc = item['class_of_charge']

        if coc in ['PAB', 'WOB']:
            logging.info("Processing %s", number)
            reg = get_registration(date, number)

            if 'amends_registration' in reg and reg['amends_registration']['type'] == 'Amendment':
                prev_number = reg['amends_registration']['number']
                prev_date = reg['amends_registration']['date']
                prev_reg = get_registration(prev_date, prev_number)
                if not lead_name_changed(reg, prev_reg):
                    continue

            debtor = get_debtor_party(reg)

            if debtor is None:
                raise BankruptcyProcessError("Registration {} has no debtor".format(number))

            # names = []
            # for name in debtor['names']:
            #     names += get_debtor_name_matches(name)
            names = get_debtor_name_matches(debtor['names'][0])

            post_resp = post_bankruptcy_search(reg, names)
            if post_resp.status_code == 200:
                logging.info("Received response 200 from legacy adapter")
            else:
                error = "Received response {} from legacy db trying to add debtor {}".format(post_resp.status_code, number)
                logging.error(error)
                return_ok = False
                raise_error(producer, {
                    "message": error,
                    "stack": "",
                    "subsystem": CONFIG['APPLICATION_NAME'],
                    "type": "E"
                })
        else:
            logging.info("Skip non-bankruptcy: %s", coc)

    return return_ok


def process(config, date):
    global CONFIG
    CONFIG = config
    hostname = config['AMQP_URI']
    connection = kombu.Connection(hostname=hostname)
    producer = connection.SimpleQueue('errors')

    entries = get_entries_for_process(date)
    logging.info("Processor starts")
    logging.info("%d Entries received", len(entries))

    there_were_errors = False
    for entry in entries:
        logging.info('================================================================')
        logging.info("Process {}".format(entry['application']))

        try:
            ok = True
            if entry['application'] in ['new']:
                ok = process_entry(producer, entry)
            elif entry['application'] in ['Amendment']:
                ok = process_entry(producer, entry)
            else:
                logging.info('Skipping application of type "%s"', entry['application'])

            if not ok:
                there_were_errors = True

        # pylint: disable=broad-except
        except Exception as ex:
            there_were_errors = True
            logging.error('Unhandled error: %s', str(ex))
            s = log_stack()
            raise_error(producer, {
                "message": str(ex),
                "stack": s,
                "subsystem": CONFIG['APPLICATION_NAME'],
                "type": "E"
            })
    logging.info("Synchroniser finishes")
    if there_were_errors:
        logging.error("There were errors")

    return not there_were_errors


def log_stack():
    call_stack = traceback.format_exc()

    lines = call_stack.split("\n")
    for line in lines:
        logging.error(line)
    return call_stack


def raise_error(producer, error):
    producer.put(error)
    logging.warning('Error successfully raised.')
