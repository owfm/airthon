import json
import requests
from os import environ


class Base:

    def __init__(self, AIRTABLE_BASE=None, AIRTABLE_API_KEY=None):

        if AIRTABLE_API_KEY:
            self.AIRTABLE_API_KEY = AIRTABLE_API_KEY
        elif environ.get('AIRTABLE_API_KEY'):
            self.AIRTABLE_API_KEY = environ.get('AIRTABLE_API_KEY')
        else:
            raise KeyError(
                'You must provide AIRTABLE_API_KEY as a named arg or env variable.')

        if AIRTABLE_BASE:
            self.AIRTABLE_BASE = AIRTABLE_BASE
        elif environ.get('AIRTABLE_BASE'):
            self.AIRTABLE_BASE = environ.get('AIRTABLE_BASE')
        else:
            raise KeyError(
                'You must provide AIRTABLE_API_KEY as a named arg or env variable.')


        self.BASE_URL="https://api.airtable.com/v0/{}".format(
            self.AIRTABLE_BASE)
        self.headers={"Authorization": "Bearer {}".format(self.AIRTABLE_API_KEY)}

    def _chunker(self, seq, size):
        ''' breaks up larges lists into iterable chunks of size 'size'.
        Used for pagination of payloads '''
        return (seq[pos:pos + size] for pos in range(0, len(seq), size))

    def post(self, resource_type, payload = None):

        URL="{}/{}".format(self.BASE_URL, resource_type)

        if not payload:
            raise Exception("No payload for POST request.")

        if type(payload) is dict:
            payload=[payload]

        for entry in payload:
            if entry.get('fields', -1) == -1:  # check if fields is present, even if empty
                raise Exception(
                    'Invalid payload: must contain a "fields" key.')

        responses={"records": []}

        for chunk in self._chunker(payload, 10):
            data={'records': chunk}
            try:
                response=requests.post(
                    URL, headers = self.headers, json = data)
                response_json=response.json()

                if response_json.get('records'):
                    responses['records'] += response_json['records']
                else:
                    # problem with the post request, return response to user.
                    return response_json

            except Exception as err:
                raise Exception('Something went wrong: ' + str(err))

        return responses

    def get(self, resource_type = None, resource_id = None, filter_formula_string = None, filter_fields = None, view = None):

        if not resource_type:
            raise ValueError(
                'You must supply resource type as first argument to "get".')

        if resource_id:  # retrieve single item
            try:
                URL = "{}/{}/{}".format(self.BASE_URL,
                                        resource_type, resource_id)

                response = requests.get(URL, headers=self.headers)
                response_json = response.json()

                return response_json

            except Exception as err:
                raise Exception('Something went wrong: ' + str(err))

        # list multiple entries
        responses = {'records': []}

        offset = None  # flag

        URL = "{}/{}".format(self.BASE_URL, resource_type)

        while True:
            try:
                params = {"offset": offset} if offset else {}
                params['filterByFormula'] = filter_formula_string if filter_formula_string else None
                params['fields[]'] = filter_fields if filter_fields else None
                params['view'] = view if view else None

                response = requests.get(
                    URL, headers=self.headers, params=params)

                response_json = response.json()

                if response_json.get('records'):
                    responses['records'] += response_json['records']
                else:
                    return response_json

                if response_json.get('offset'):
                    offset = response_json['offset']
                else:
                    offset = None

            except Exception:
                raise

            if not offset:
                break

        return responses

    def delete(self, resource_type=None, resource_id=None):

        if not resource_type:
            raise ValueError('You must supply a resource_type (table name)')

        if not resource_id:
            raise Exception(
                "You must provide either a single ID or a list of IDs to delete.")

        if type(resource_id) is str:
            resource_id = [resource_id]

        URL = "{}/{}".format(self.BASE_URL, resource_type)

        responses = {'records': []}

        for id_batch in self._chunker(resource_id, 10):

            params = {
                'records[]': id_batch
            }

            try:
                response = requests.delete(
                    URL, params=params, headers=self.headers)

                response_json = response.json()

                responses['records'] += response_json['records']

                if len(response_json['records']) != len(id_batch):
                    raise Exception(
                        'Not all records were deleted: Please check AirTable.')
            except Exception:
                raise
        return responses

    def patch(self, resource_type=None, payload=None):

        if not resource_type:
            raise ValueError('You must supply a resource_type (table name)')

        if not payload or type(payload) not in [dict, list]:
            raise Exception(
                "You must provide a payload object or list of objects to patch.")

        if type(payload) is dict:
            payload = [payload]

        for entry in payload:
            for key in entry:
                if key not in ['id', 'fields']:
                    raise Exception(
                        'Invalid PATCH payload: must only contain a "fields" and "id" key. Found "{}"'.format(key))

        URL = "{}/{}".format(self.BASE_URL, resource_type)
        responses = {'records': []}

        for chunk in self._chunker(payload, 10):

            data = {
                'records': chunk
            }

            try:
                response = requests.patch(
                    URL, headers=self.headers, json=data)
                response_json = response.json()
                responses['records'] += response_json['records']

            except Exception:
                raise
        return responses
