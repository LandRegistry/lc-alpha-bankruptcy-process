from application.routes import app
import requests
import json
import logging


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

    url = app.config['NAMES_SEARCH_URI'] + '/search?forename=' + forenames + '&surname=' + surname
    response = requests.get(url)
    name_search_result = response.json()
    logging.info('Retrieved %d name hits', len(name_search_result))
    return name_search_result


def get_complex_name_matches(name):
    uri = app.config['LEGACY_DB_URI'] + '/complex_names/search'
    cn_response = requests.post(uri, data=json.dumps({"name": name}), headers={'Content-Type': 'application/json'})

    names = []
    if cn_response.status_code == 404:
        names.append(name)  # TODO: what happens if BR thinks its complex, but CN doesn't?
    else:
        cnames = cn_response.json()
        for item in cnames:
            names.append(item['name'])

    # Now IOPN search against each name
    name_search_result = []
    for name in names:
        url = app.config['NAMES_SEARCH_URI'] + '/search'
        params = {'name': name, 'type': 'exact'}
        names_response = requests.get(url, params=params)
        name_search_result += names_response.json()
    return name_search_result


def post_bankruptcy_search(registration, name_search_result):
    data = {
        'registration': registration,
        'iopn': name_search_result
    }

    uri = app.config['LEGACY_DB_URI'] + '/debtor'
    headers = {'Content-Type': 'application/json'}
    logging.info('Posting combined dataset to LegacyDB')
    return requests.post(uri, data=json.dumps(data), headers=headers)


def message_received(body, message):
    logging.info("Received new registrations: %s", str(body))
    errors = []

    # TODO: only execute against relevant registrations/amendments etc.

    request_uri = app.config['REGISTER_URI'] + '/registration/'
    for number in body:
        try:
            logging.info("Processing %s", number)
            uri = request_uri + str(number)
            response = requests.get(uri)

            if response.status_code == 200:
                logging.info("Received response 200 from /registration")
                registration_response = response.json()

                print(registration_response)
                if 'complex' in registration_response:
                    # Complex Name Case...
                    name_search_result = get_complex_name_matches(registration_response['complex']['name'])
                else:
                    name_search_result = get_simple_name_matches(registration_response['debtor_name'])

                post_response = post_bankruptcy_search(registration_response, name_search_result)
                if post_response.status_code == 200:
                    logging.info("Received response 200 from legacy db ")
                else:
                    logging.error("Received response %d from legacy db trying to add debtor %s",
                                  post_response.status_code, number)
                    save_error(errors, post_response, '/land_charge', number)
            else:
                logging.error("Received response %d from bankruptcy-registration for registration %s",
                              response.status_code, number)
                save_error(errors, response, '/registration', number)

        # pylint: disable=broad-except
        except Exception as exception:
            errors.append({
                "registration_no": number,
                "exception_class": type(exception).__name__,
                "error_message": str(exception)
            })

    message.ack()
    if len(errors) > 0:
        raise BankruptcyProcessError(errors)


def listen(incoming_connection, error_producer, run_forever=True):
    logging.info('Listening for new registrations')

    while True:
        try:
            incoming_connection.drain_events()
        except BankruptcyProcessError as exception:
            for error in exception.value:
                error_producer.put(error)
            logging.info("Error published")
        except KeyboardInterrupt:
            logging.info("Interrupted")
            break

        if not run_forever:
            break
