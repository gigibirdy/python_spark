import csv
import requests
import random

account_type = ['saving', 'checking', 'trading', 'forex', 'retirement']
field_names = ['customer_id', 'first_name', 'last_name', 'gender', 'state', 'type_of_account', 'created_at']


# get random customers data from randomuser.me in csv format
def get_csv_file():
    response = requests.get('https://randomuser.me/api/?results=50&nat=us&format=csv')
    decoded_content = response.content.decode('utf-8')
    try:
        with open('test_csv.csv', 'w', newline='') as csvfile:
            cr = csv.reader(decoded_content.splitlines(), delimiter=',')
            next(cr, None)
            writer = csv.writer(csvfile, delimiter=',')
            writer.writerow(field_names)
            for row in cr:
                writer.writerow([row[15], row[2], row[3], row[0], row[7], random.choice(account_type), row[24]])
    except IOError:
        print('IOError')


# get random customers data from randomuser.me in json format
def get_json():
    response = requests.get('https://randomuser.me/api/?results=50&nat=us')
    result = response.json()['results']
    customers = []
    for n in range(len(result)):
        d = {'customer_id': result[n]['login']['uuid'], 'first_name': result[n]['name']['first'],
             'last_name': result[n]['name']['last'], 'gender': result[n]['gender'],
             'state': result[n]['location']['state'], 'type_of_account': random.choice(account_type),
             'created_at': result[n]['registered']['date']}
        customers.append(d)
    try:
        with open('test_json.csv', 'w', newline='') as file:
            writer = csv.DictWriter(file, delimiter=',', fieldnames=field_names)
            writer.writeheader()
            writer.writerows(customers)
    except IOError:
        print('IOError')


get_json()
get_csv_file()