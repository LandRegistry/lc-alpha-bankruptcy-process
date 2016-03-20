import requests
import json
import logging
import urllib.parse
import kombu
import traceback
import getpass


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

    result = {
        'debtor_names': [],
        # 'debtor_names': [{
        #     'forenames': debtor['names'][0]['private']['forenames'],
        #     'surname': debtor['names'][0]['surname']
        # }],
        'legal_body_ref': debtor['case_reference'],
        'trading_name': "",
        'status': registration['status'],
        "class_of_charge": registration['class_of_charge'],
        'key_number': registration['applicant']['key_number'],
        'legal_body': debtor['legal_body'],
        'registration': {
            'number': registration['registration']['number'],
            'date': registration['registration']['date']
        },
        'occupation': debtor['occupation'],
        'residence': [],
        'application_ref': registration['applicant']['reference']
    }

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
    for item in entry['data']:
        date = item['date']
        number = item['number']
        coc = item['class_of_charge']

        if coc in ['PAB', 'WOB']:
            logging.info("Processing %s", number)
            reg = get_registration(date, number)

            if 'amends_registration' in reg and reg['amends_registration']['tyoe'] == 'Amendment':
                prev_number = reg['amends_registration']['number']
                prev_date = reg['amends_registration']['date']
                prev_reg = get_registration(prev_date, prev_number)
                if not lead_name_changed(reg, prev_reg):
                    continue

            debtor = get_debtor_party(reg)

            if debtor is None:
                raise BankruptcyProcessError("Registration {} has no debtor".format(number))

            names = []
            for name in debtor['names']:
                names += get_debtor_name_matches(name)

            post_resp = post_bankruptcy_search(reg, names)
            if post_resp.status_code == 200:
                logging.info("Received response 200 from legacy adpater")
            else:
                error = "Received response {} from legacy db trying to add debtor {}".format(post_resp.status_code, number)
                logging.error(error)
                raise_error(producer, {
                    "message": error,
                    "stack": "",
                    "subsystem": CONFIG['APPLICATION_NAME'],
                    "type": "E"
                })
        else:
            logging.info("Skip non-bankruptcy: %s", coc)


def process(config, date):
    global CONFIG
    CONFIG = config
    hostname = "amqp://{}:{}@{}:{}".format(CONFIG['MQ_USERNAME'], CONFIG['MQ_PASSWORD'],
                                           CONFIG['MQ_HOSTNAME'], CONFIG['MQ_PORT'])
    connection = kombu.Connection(hostname=hostname)
    producer = connection.SimpleQueue('errors')

    entries = get_entries_for_process(date)
    logging.info("Processor starts")
    logging.info("%d Entries received", len(entries))

    there_were_errors = False
    for entry in entries:
        logging.info("Process {}".format(entry['application']))

        try:
            if entry['application'] in ['new']:
                process_entry(producer, entry)
            if entry['application'] in ['Amendment']:
                process_entry(producer, entry) # TODO: only if its a name change
            else:
                logging.info('Skipping application of type "%s"', entry['application'])

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


def log_stack():
    call_stack = traceback.format_exc()

    lines = call_stack.split("\n")
    for line in lines:
        logging.error(line)
    return call_stack


def raise_error(producer, error):
    producer.put(error)
    logging.warning('Error successfully raised.')


# def message_received(body, message):
#     logging.info("Received new registrations: %s", str(body))
#     errors = []
#
#     # TODO: only execute against relevant registrations/amendments etc.
#
#     request_uri = CONFIG['REGISTER_URI'] + '/registrations/'
#     for application in body['data']:
#         number = application['number']
#         date = application['date']
#
#         try:
#             logging.info("Processing %s", number)
#             uri = "{}{}/{}".format(request_uri, date, number)
#             response = requests.get(uri)
#
#             if response.status_code == 200:
#                 logging.info("Received response 200 from /registrations")
#                 registration_response = response.json()
#
#                 print(registration_response)
#                 if 'complex' in registration_response:
#                     # Complex Name Case...
#                     name_search_result = get_complex_name_matches(registration_response['complex']['number'])
#                 else:
#                     name_search_result = get_simple_name_matches(registration_response['debtor_name'])
#
#                 post_response = post_bankruptcy_search(registration_response, name_search_result)
#                 if post_response.status_code == 200:
#                     logging.info("Received response 200 from legacy db ")
#                 else:
#                     logging.error("Received response %d from legacy db trying to add debtor %s",
#                                   post_response.status_code, number)
#                     save_error(errors, post_response, '/debtor', number)
#             else:
#                 logging.error("Received response %d from bankruptcy-registration for registration %s",
#                               response.status_code, number)
#                 save_error(errors, response, '/registrations', number)
#
#         # pylint: disable=broad-except
#         except Exception as exception:
#             errors.append({
#                 "registration_no": number,
#                 "exception_class": type(exception).__name__,
#                 "error_message": str(exception)
#             })
#
#     message.ack()
#     if len(errors) > 0:
#         raise BankruptcyProcessError(errors)
#
#
# def listen(incoming_connection, error_producer, run_forever=True):
#     logging.info('Listening for new registrations')
#
#     while True:
#         try:
#             incoming_connection.drain_events()
#         except BankruptcyProcessError as exception:
#             for error in exception.value:
#                 error_producer.put(error)
#             logging.info("Error published")
#         except KeyboardInterrupt:
#             logging.info("Interrupted")
#             break
#
#         if not run_forever:
#             break
