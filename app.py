from flask import Flask, request
from flask.json import jsonify
import json
import sqlite3
import statistics
from marshmallow import ValidationError
from schemas import DeviceReadingSchema, DeviceReadingInputSchema, DeviceReadingValueInputSchema, DeviceReadingQuartilesInputSchema

app = Flask(__name__)

# Setup the SQLite DB
conn = sqlite3.connect('database.db')
conn.execute('CREATE TABLE IF NOT EXISTS readings (device_uuid TEXT, type TEXT, value INTEGER, date_created INTEGER)')
conn.close()


@app.route('/devices/<string:device_uuid>/readings/', methods=['POST', 'GET'])
def request_device_readings(device_uuid):
    """
    This endpoint allows clients to POST or GET data specific sensor types.

    POST Parameters:
    * type -> The type of sensor (temperature or humidity)
    * value -> The integer value of the sensor reading
    * date_created -> The epoch date of the sensor reading.
        If none provided, we set to now.

    Optional Query Parameters:
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    * type -> The type of sensor value a client is looking for
    """

    conn = _get_db_connection()
    cur = conn.cursor()

    if request.method == 'POST':
        # Grab the post parameters
        post_data = json.loads(request.data)

        try:
            # Validate the payload
            data = DeviceReadingSchema().load(post_data)
        except ValidationError as err:
            return err.messages, 400

        # Insert data into db
        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (device_uuid, data['type'], data['value'], data['date_created']))

        conn.commit()

        # Return success
        return 'success', 201
    else:
        try:
            # Validate the request args
            data = DeviceReadingInputSchema().load(request.args)
        except ValidationError as err:
            return err.messages, 400

        # Construct the query
        # Ideally we would use an ORM instead of writing inline SQL
        query = 'select * from readings where device_uuid = ?'
        if data['type']:
            query += ' and type = ?'
        if data['start']:
            query += ' and date_created >= ?'
        if data['end']:
            query += ' and date_created < ?'

        # Doing this to avoid SQL injection, which would be possible if
        # doing 'select * from readings where blah={}'.format(param)
        params = tuple(p for p in (device_uuid, data['type'], data['start'], data['end']) if p)

        # Execute the query
        cur.execute(query, params)
        rows = cur.fetchall()

        # Return the JSON
        return jsonify([dict(zip(['device_uuid', 'type', 'value', 'date_created'], row)) for row in rows]), 200


@app.route('/devices/<string:device_uuid>/readings/min/', methods=['GET'])
def request_device_readings_min(device_uuid):
    """
    This endpoint allows clients to GET the min sensor reading for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for

    Optional Query Parameters
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """

    conn = _get_db_connection()
    cur = conn.cursor()

    try:
        # Validate the request args
        data = DeviceReadingValueInputSchema().load(request.args)
    except ValidationError as err:
        return err.messages, 400

    # Construct the query
    query = 'select * from readings where device_uuid = ? and type = ?'
    if data['start']:
        query += ' and date_created >= ?'
    if data['end']:
        query += ' and date_created < ?'
    query += ' order by value, date_created limit 1'

    params = tuple(p for p in (device_uuid, data['type'], data['start'], data['end']) if p)

    # Execute the query
    cur.execute(query, params)
    row = cur.fetchone()

    # Return the JSON
    return jsonify(dict(zip(['device_uuid', 'type', 'value', 'date_created'], row))), 200


@app.route('/devices/<string:device_uuid>/readings/max/', methods=['GET'])
def request_device_readings_max(device_uuid):
    """
    This endpoint allows clients to GET the max sensor reading for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for

    Optional Query Parameters
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """

    conn = _get_db_connection()
    cur = conn.cursor()

    try:
        # Validate the request args
        data = DeviceReadingValueInputSchema().load(request.args)
    except ValidationError as err:
        return err.messages, 400

    # Construct the query
    query = 'select * from readings where device_uuid = ? and type = ?'
    if data['start']:
        query += ' and date_created >= ?'
    if data['end']:
        query += ' and date_created < ?'
    query += ' order by value desc, date_created limit 1'

    params = tuple(p for p in (device_uuid, data['type'], data['start'], data['end']) if p)

    # Execute the query
    cur.execute(query, params)
    row = cur.fetchone()

    # Return the JSON
    return jsonify(dict(zip(['device_uuid', 'type', 'value', 'date_created'], row))), 200


@app.route('/devices/<string:device_uuid>/readings/median/', methods=['GET'])
def request_device_readings_median(device_uuid):
    """
    This endpoint allows clients to GET the median sensor reading for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for

    Optional Query Parameters
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """

    conn = _get_db_connection()
    cur = conn.cursor()

    try:
        # Validate the request args
        data = DeviceReadingValueInputSchema().load(request.args)
    except ValidationError as err:
        return err.messages, 400

    # Construct the query
    # The below query is a little convoluted - depending on the number of
    # records I might decide just to do this in python rathen than in the
    # SQL query
    # I demonstrate how to do it in python earlier in the git commit history
    where_query = 'where device_uuid = ? and type = ?'
    if data['start']:
        where_query += ' and date_created >= ?'
    if data['end']:
        where_query += ' and date_created < ?'

    query = f'select * from readings {where_query}'
    query += f' order by value, date_created limit 1 offset (select count(*) / 2 from readings {where_query})'

    params = 2 * tuple(p for p in (device_uuid, data['type'], data['start'], data['end']) if p)

    # Execute the query
    cur.execute(query, params)
    row = cur.fetchone()

    # Return the JSON
    return jsonify(dict(zip(['device_uuid', 'type', 'value', 'date_created'], row))), 200


@app.route('/devices/<string:device_uuid>/readings/mean/', methods=['GET'])
def request_device_readings_mean(device_uuid):
    """
    This endpoint allows clients to GET the mean sensor readings for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for

    Optional Query Parameters
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """

    conn = _get_db_connection()
    cur = conn.cursor()

    try:
        # Validate the request args
        data = DeviceReadingValueInputSchema().load(request.args)
    except ValidationError as err:
        return err.messages, 400

    # Construct the query
    query = 'select avg(value) as mean_value from readings where device_uuid = ? and type = ?'
    if data['start']:
        query += ' and date_created >= ?'
    if data['end']:
        query += ' and date_created < ?'

    params = tuple(p for p in (device_uuid, data['type'], data['start'], data['end']) if p)

    # Execute the query
    cur.execute(query, params)
    row = cur.fetchone()

    # Return the JSON
    return jsonify(dict(value=round(row['mean_value']))), 200


@app.route('/devices/<string:device_uuid>/readings/mode/', methods=['GET'])
def request_device_readings_mode(device_uuid):
    """
    This endpoint allows clients to GET the mode sensor reading value for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for

    Optional Query Parameters
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """

    conn = _get_db_connection()
    cur = conn.cursor()

    try:
        # Validate the request args
        data = DeviceReadingValueInputSchema().load(request.args)
    except ValidationError as err:
        return err.messages, 400

    # Construct the query
    query = 'select value as mode_value from readings where device_uuid = ? and type = ?'
    if data['start']:
        query += ' and date_created >= ?'
    if data['end']:
        query += ' and date_created < ?'
    # if 2 different values have the same frequency of occurence, take the smaller number
    query += 'group by value order by count(*) desc, value limit 1'

    params = tuple(p for p in (device_uuid, data['type'], data['start'], data['end']) if p)

    # Execute the query
    cur.execute(query, params)
    row = cur.fetchone()

    # Return the JSON
    return jsonify(dict(value=row['mode_value'])), 200


@app.route('/devices/<string:device_uuid>/readings/quartiles/', methods=['GET'])
def request_device_readings_quartiles(device_uuid):
    """
    This endpoint allows clients to GET the 1st and 3rd quartile
    sensor reading value for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """

    conn = _get_db_connection()
    cur = conn.cursor()

    try:
        # Validate the request args
        data = DeviceReadingQuartilesInputSchema().load(request.args)
    except ValidationError as err:
        return err.messages, 400

    # Construct the query
    query = 'select value from readings where device_uuid = ? and type = ? and date_created >= ? and date_created < ?'
    params = (device_uuid, data['type'], data['start'], data['end'])

    # Execute the query
    cur.execute(query, params)
    rows = cur.fetchall()

    # Should also be possible to calculate the first and third quartiles
    # with a complicated SQL query, but this is simpler
    values = sorted([r['value'] for r in rows])
    mid = len(values) // 2

    q1 = round(statistics.median(values[:mid]))
    q3 = round(statistics.median(values[-mid:]))

    # Return the JSON
    return jsonify(dict(quartile_1=q1, quartile_3=q3)), 200


def _get_db_connection():
    # Set the db that we want and open the connection
    if app.config['TESTING']:
        conn = sqlite3.connect('test_database.db')
    else:
        conn = sqlite3.connect('database.db')
    conn.row_factory = sqlite3.Row

    return conn


if __name__ == '__main__':
    app.run()
