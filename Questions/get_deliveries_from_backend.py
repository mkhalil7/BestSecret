"""
Gets delivery data from the Backend reporting API and inserts
into the target Data Warehouse (a PostgreSQL database).
This script runs under Python 3.4 and requires psycopg2
"""

import datetime
import json
import urllib.request
import urllib.parse
import http
import http.cookiejar
import psycopg2  # Connector for PostgreSQL DB

LOGIN_URL = 'https://api.deliveryhero.com/login'
DELIVERIES_URL = 'https://api.deliveryhero.com/reporting/deliveries/'

DB_USER = 'dbadmin'
DB_PASSWORD = 'fasd325q3tg*&'
BACKEND_USER = 'admin'
BACKEND_PASSWORD = 'sdaf@#@#hkjasd2'

EXPECTED_FIELDS = ['fleet', 'timezone', 'delivery_uuid', 'restaurant_name', 'restaurant_city',
                   'driver_username', 'customer_phone_number', 'customer_street',
                   'customer_number', 'customer_zipcode', 'customer_city',
                   'distance_restaurant_to_customer', 'distance_driver_to_restaurant',
                   'total', 'payment_type', 'delivery_fee',
                   'last_delivery_status', 'last_delivery_status_timestamp',
                   'created_at_timestamp', 'pickup_timestamp', 'delivered_timestamp'
                   ]

INSERT_SQL_DWH = """
INSERT INTO tableau.delivery_dwh
('fleet', 'timezone', 'delivery_uuid', 'restaurant_name', 'restaurant_city',
'driver_username', 'customer_phone_number', 'customer_street',
'customer_number', 'customer_zipcode', 'customer_city',
'distance_restaurant_to_customer', 'distance_driver_to_restaurant',
'total', 'payment_type', 'delivery_fee',
'last_delivery_status', 'last_delivery_status_timestamp',
'created_at_timestamp', 'pickup_timestamp', 'delivered_timestamp')
VALUES (
%s, %s, %s, %s, %s,
%s, %s, %s,
%s, %s, %s,
%s, %s,
%s, %s, %s,
%s, %s,
%s, %s, %s
);
"""


class ScriptError(Exception):
    pass


def backend_authenticate():
    """
    Access the login API and gets a valid token to be used in the next requests
    The auth_token and url_opener are made available for the rest of the script
    as global variables.
    """
    
    global url_opener, auth_token

    # Creates a URL opener that can handle cookies
    cookie_jar = http.cookiejar.CookieJar()
    url_opener = urllib.request.build_opener(
                            urllib.request.HTTPCookieProcessor(cookie_jar))

    # Builds a JSON payload to be sent in the POST for authentication
    login_params = {'username': DB_USER,
                    'password': DB_PASSWORD}
    post_data = json.dumps(login_params)
    post_data = post_data.encode('utf-8')

    request = urllib.request.Request(LOGIN_URL)
    request.add_header('content-type', 'application/json')
    request.add_header('accept', 'application/json')

    with url_opener.open(request, post_data) as response:

        # The response is a JSON object that shouldn't be larger than 1 KiB
        auth_json = response.read(1024).decode('utf-8')

        if response.read(1024) != b'':
            raise ScriptError('Dowloaded JSON is larger than 1 MiB')

    # Sample JSON of the authenticate response
    #   {
    #       "token": "a6fbc3a1-3a21-424c-bc0e-6500543e602f"
    #   }
    auth_data = json.loads(auth_json)
    auth_token = auth_data['token']

    return


def download_data(start_date, end_date):

    # Builds an URL like this:
    # https://api.deliveryhero.com/reporting/deliveries/?start=2016-05-01&end=2016-05-07
    url = (DELIVERIES_URL + '?' +
           urllib.parse.urlencode({'start': start_date,
                                   'end': end_date}))

    request = urllib.request.Request(url)
    # Uses the authentication token
    request.add_header('Authorization', auth_token)

    with url_opener.open(request) as response:

        # The response is a JSON object that shouldn't be larger than 100 MiB.
        deliveries_json = response.read(104857600).decode('utf-8')

        if response.read(1024) != b'':
            raise ScriptError('Dowloaded JSON is larger than 100 MiB')

    # The returned JSON is just a list (in JSON it's called "array") of rows.
    # The first row is a list containing the name of the fields
    # Each of the other rows represent a single delivery as a list of field
    #  values (in the order specified in the first row). Example:
    #
    # [
    #     ["fleet", "timezone", "delivery_uuid" ...],
    #     ["berlin", "Europe/Berlin", "a6fbc3a1-3a21-424c-bc0e-6500543e602f", ...],
    #     ["munich", "Europe/Berlin", "59238647-37b9-25cb-ab43-98465eab4652", ...],
    #     ...
    # ]
    rows = json.loads(deliveries_json)

    # Checks if we got the expected fields in the correct order
    if rows[0] != EXPECTED_FIELDS:
        # The JSON does not contain the expected fields
        return []
    else:
        # Returns only the data, excluding the first row of field names
        return rows[1:]


def insert_in_db_dwh(conn, rows):

    try:
        # Get a cursor from the connection. This starts a new transaction.
        cur = conn.cursor()

        for row in rows:
            # For each row, execute the INSERT_SQL_DWH SQL statement, passing
            # the contents of the row as argument
            cur.execute(INSERT_SQL_DWH, row)

        # Commit the transaction to make the insertions permanent.
        conn.commit()
        # Close the cursor
        cur.close()

    except:
        pass


def main():

    # Retrieve data for yesterday and the day before yesterday
    end_date = datetime.date.today() - datetime.timedelta(days=1)
    start_date = end_date - datetime.timedelta(days=1)

    # Connects to the backend
    backend_authenticate()

    # Connects to the data warehouse
    # psycopg2.connect() requires a string with the following format:
    # "host='hostname' dbname='dbname' user='username' password='the_pasword'"
    # It returns an object compliant with the Python DB access API (PEP 249),
    # so we can call the cursor(), commit() and close() methods of this object
    db_conn = psycopg2.connect(("host='dwh.deliveryhero.com' "
                                "dbname='dwh' user='{}' "
                                "password='{}'").
                               format(DB_USER, DB_PASSWORD))

    # Reads data from backend
    rows = download_data(start_date, end_date)
    # Writes data into data warehouse
    insert_in_db_dwh(db_conn, rows)


if __name__ == '__main__':

    main()
