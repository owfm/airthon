from airthon import Base
import unittest
from os import environ

TEST_TABLE = 'TestTable'


class TestBase(unittest.TestCase):
    def test_get_single_item(self):
        '''
        check getting single valid item returns the item.
        '''

        base = Base()

        entry = base.get(TEST_TABLE, resource_id='recmpa1Nj0KtKwr9k')
        print(entry)

        self.assertEqual(entry['fields']['Name'], 'Test Item 1')

    def test_get_single_wrong_item(self):
        '''
        check getting single  item returns the item.
        '''

        base = Base()

        entry = base.get(TEST_TABLE, resource_id='zxxxx')

        self.assertEqual(entry['error'], 'NOT_FOUND')

    def test_get_all_items(self):
        '''
        check getting all items
        '''

        base = Base()

        response = base.get(TEST_TABLE)

        self.assertIsInstance(response, dict)
        self.assertTrue(response.get('records'))

        for entry in response.get('records'):
            self.assertTrue(entry.get('fields'))

    def test_post_single(self):

        payload = {
            'fields': {
                'Name': 'TestCase1'
            }
        }

        base = Base()

        entry = base.post(TEST_TABLE, payload=payload)

        self.assertTrue(entry.get('records'))
        self.assertEqual(entry['records'][0]['fields']['Name'], 'TestCase1')

    def test_post_multiple(self):

        payload = [{'fields': {
            'Name': 'TestCase' + str(number)
        }
        } for number in range(50)]

        base = Base()

        entries = base.post(TEST_TABLE, payload=payload)

        self.assertTrue(len(entries.get('records')), 50)

    def test_delete_multiple(self):

        payload = [{'fields': {
            'Name': 'TestCase' + str(number)
        }
        } for number in range(50)]

        base = Base()

        entries = base.post(TEST_TABLE, payload=payload)

        entry_ids = [entry['id'] for entry in entries['records']]
        deleted_entries = base.delete(TEST_TABLE, resource_id=entry_ids)
        self.assertEqual(len(deleted_entries['records']), 50)

    def test_search_by_formula(self):

        base = Base()

        filter_string = '({Name} = "searchbyformulaexample")'
        response = base.get(TEST_TABLE, filter_formula_string=filter_string)
        self.assertEqual(response['records'][0]['fields']
                         ['Name'], 'searchbyformulaexample')

    def test_cleanup(self):

        base = Base()

        filter_string = '(LEFT({Name},8) = "TestCase")'
        response = base.get(TEST_TABLE, filter_formula_string=filter_string)

        ids = [entry['id'] for entry in response['records']]
        base.delete(TEST_TABLE, resource_id=ids)
