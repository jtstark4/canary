import json
import sqlite3
import unittest
from app import app


class SensorRoutesTestCases(unittest.TestCase):

    def setUp(self):
        # Setup the SQLite DB
        conn = sqlite3.connect('test_database.db')
        conn.execute('DROP TABLE IF EXISTS readings')
        conn.execute('CREATE TABLE IF NOT EXISTS readings (device_uuid TEXT, type TEXT, value INTEGER, date_created INTEGER)')

        self.device_uuid = 'test_device'

        # Setup some sensor data
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (self.device_uuid, 'temperature', 22, 1))
        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (self.device_uuid, 'temperature', 50, 5))

        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (self.device_uuid, 'humidity', 44, 7))
        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (self.device_uuid, 'humidity', 24, 10))

        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (self.device_uuid, 'temperature', 100, 20))
        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (self.device_uuid, 'temperature', 29, 20))
        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (self.device_uuid, 'temperature', 4, 25))
        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (self.device_uuid, 'temperature', 28, 30))
        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (self.device_uuid, 'temperature', 100, 40))
        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (self.device_uuid, 'temperature', 52, 50))
        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (self.device_uuid, 'temperature', 4, 50))
        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (self.device_uuid, 'temperature', 100, 50))

        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    ('other_uuid', 'temperature', 22, 60))
        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    ('another_uuid', 'humidity', 27, 60))

        conn.commit()

        app.config['TESTING'] = True

        self.client = app.test_client

    def test_device_readings_get(self):
        # Given a device UUID
        # When we make a request with the given UUID
        request = self.client().get('/devices/{}/readings/'.format(self.device_uuid))

        # Then we should receive a 200
        self.assertEqual(request.status_code, 200)

        # And the response data should have twelve sensor readings
        self.assertTrue(len(json.loads(request.data)) == 12)

    def test_device_readings_post(self):
        # Given a device UUID
        # When we make a request with the given UUID to create a reading
        request = self.client().post(
            '/devices/{}/readings/'.format(self.device_uuid),
            data=json.dumps({
                'type': 'temperature',
                'value': 100
            }))

        # Then we should receive a 201
        self.assertEqual(request.status_code, 201)

        # And when we check for readings in the db
        conn = sqlite3.connect('test_database.db')
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute('select * from readings where device_uuid="{}"'.format(self.device_uuid))
        rows = cur.fetchall()

        # We should have thirteen
        self.assertTrue(len(rows) == 13)

    def test_device_readings_get_temperature(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's temperature data only.
        """
        # Given a device UUID
        # When we make a request with the given UUID
        # And sensor type of "temperature"
        request = self.client().get('/devices/{}/readings/?type=temperature'.format(self.device_uuid))

        # Then we should receive a 200
        self.assertEqual(request.status_code, 200)

        res = json.loads(request.data)

        # And the response data should have ten sensor readings
        self.assertTrue(len(res) == 10)

        # And all readings should have the correct device_uuid
        self.assertTrue(all([x['device_uuid'] == self.device_uuid for x in res]))

        # And type "temperature"
        self.assertTrue(all([x['type'] == 'temperature' for x in res]))

    def test_device_readings_get_humidity(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's humidity data only.
        """
        # Given a device UUID
        # When we make a request with the given UUID
        # And sensor type of "humidity"
        request = self.client().get('/devices/{}/readings/?type=humidity'.format(self.device_uuid))

        # Then we should receive a 200
        self.assertEqual(request.status_code, 200)

        res = json.loads(request.data)

        # And the response data should have three sensor readings
        self.assertTrue(len(res) == 2)

        # And all readings should have the correct device_uuid
        self.assertTrue(all([x['device_uuid'] == self.device_uuid for x in res]))

        # And type "temperature"
        self.assertTrue(all([x['type'] == 'humidity' for x in res]))

    def test_device_readings_get_past_dates(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's sensor data over
        a specific date range. We should only get the readings
        that were created in this time range.
        """
        # Given a device UUID
        # And a start and end date
        # When we make a request with the given UUID and date range
        request = self.client().get(
            '/devices/{}/readings/'.format(self.device_uuid),
            query_string={
                'start': 7,
                'end': 41,
            }
        )

        # Then we should receive a 200
        self.assertEqual(request.status_code, 200)

        res = json.loads(request.data)

        # And the response data should have seven sensor readings
        self.assertTrue(len(res) == 7)

        # And all readings should have a date_created between 7 and 41
        self.assertTrue(all([7 <= x['date_created'] < 41 for x in res]))

    def test_device_readings_min(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's min sensor reading.
        """
        # Given a device UUID and sensor type
        # When we make a request with the given UUID and sensor type
        request = self.client().get('/devices/{}/readings/min/?type=temperature'.format(self.device_uuid))

        # Then we should receive a 200
        self.assertEqual(request.status_code, 200)

        res = json.loads(request.data)

        # And the response data should have a value for the minimum of 4
        self.assertTrue(res['value'] == 4)

    def test_device_readings_max(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's max sensor reading.
        """
        # Given a device UUID and sensor type
        # When we make a request with the given UUID and sensor type
        request = self.client().get('/devices/{}/readings/max/?type=temperature'.format(self.device_uuid))

        # Then we should receive a 200
        self.assertEqual(request.status_code, 200)

        res = json.loads(request.data)

        # And the response data should have a value for the maximum of 4
        self.assertTrue(res['value'] == 100)

    def test_device_readings_median(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's median sensor reading.
        """
        # Given a device UUID and sensor type
        # When we make a request with the given UUID and sensor type
        request = self.client().get('/devices/{}/readings/median/?type=temperature'.format(self.device_uuid))

        # Then we should receive a 200
        self.assertEqual(request.status_code, 200)

        res = json.loads(request.data)

        # And the response data should have a value for the median of 50
        self.assertTrue(res['value'] == 50)

    def test_device_readings_mean(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's mean sensor reading value.
        """
        # Given a device UUID and sensor type
        # When we make a request with the given UUID and sensor type
        request = self.client().get('/devices/{}/readings/mean/?type=temperature'.format(self.device_uuid))

        # Then we should receive a 200
        self.assertEqual(request.status_code, 200)

        res = json.loads(request.data)

        # And the response data should have a value for the mean of 49
        self.assertTrue(res['value'] == 49)

    def test_device_readings_mode(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's mode sensor reading value.
        """
        # Given a device UUID and sensor type
        # When we make a request with the given UUID and sensor type
        request = self.client().get('/devices/{}/readings/mode/?type=temperature'.format(self.device_uuid))

        # Then we should receive a 200
        self.assertEqual(request.status_code, 200)

        res = json.loads(request.data)

        # And the response data should have a value for the mode of 100
        self.assertTrue(res['value'] == 100)

    def test_device_readings_quartiles(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's 1st and 3rd quartile
        sensor reading value.
        """
        # Given a device UUID and sensor type
        # When we make a request with the given UUID and sensor type
        request = self.client().get('/devices/{}/readings/quartiles/?type=temperature&start=1&end=101'.format(self.device_uuid))

        # Then we should receive a 200
        self.assertEqual(request.status_code, 200)

        res = json.loads(request.data)

        # And the response data should have a value for the q1 of 22
        self.assertTrue(res['quartile_1'] == 22)

        # And the response data should have a value for the q3 of 100
        self.assertTrue(res['quartile_3'] == 100)
