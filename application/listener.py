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


def message_received(body, message):
    logging.info("Received new registrations: %s", str(body))
    errors = []

    request_uri = app.config['REGISTER_URI'] + '/registration/'
    for number in body:
        try:
            logging.info("Processing %s", number)
            uri = request_uri + str(number)
            response = requests.get(uri)

            if response.status_code == 200:
                logging.info("Received response 200 from /registration")
                registration_response = response.json()

                forenames = " ".join(registration_response['debtor_name']['forenames'])
                surname = registration_response['debtor_name']['surname']

                url = app.config['NAMES_SEARCH_URI'] + '/search?forename=' + forenames + '&surname=' + surname
                response = requests.get(url)
                name_search_result = response.json()
                logging.info('Retrieved %d name hits', len(name_search_result))

                combined_data = {'registration': registration_response,
                                 'iopn': name_search_result}

                uri = app.config['LEGACY_DB_URI'] + '/debtor'
                headers = {'Content-Type': 'application/json'}
                logging.info('Posting combined dataset to LegacyDB')
                post_response = requests.post(uri, data=json.dumps(combined_data), headers=headers)
                if post_response.status_code == 200:
                    logging.info("Received response 200 from legacy db ")
                else:
                    logging.error("Received response %d from legacy db trying to add debtor %s",
                                  response.status_code, number)
                    error = {
                        "uri": '/land_charge',
                        "status_code": post_response.status_code,
                        "message": post_response.content.decode('utf-8'),
                        "registration_no": number
                    }
                    errors.append(error)
            else:
                logging.error("Received response %d from bankruptcy-registration for registration %s",
                              response.status_code, number)
                error = {
                    "uri": '/registration',
                    "status_code": response.status_code,
                    "message": post_response.content.decode('utf-8'),
                    "registration_no": number
                }
                errors.append(error)
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
