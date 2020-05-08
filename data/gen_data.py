import datetime, json, os, random, time

FIRST_NAMES = ['Monet', 'Julia', 'Angelique', 'Stephane', 'Allan', 'Ulrike', 'Vella', 'Melia',
'Noel', 'Terrence', 'Leigh', 'Rubin', 'Tanja', 'Shirlene', 'Deidre', 'Dorthy', 'Leighann',
'Mamie', 'Gabriella', 'Tanika', 'Kennith', 'Merilyn', 'Tonda', 'Adolfo', 'Von', 'Agnus',
'Kieth', 'Lisette', 'Hui', 'Lilliana',]
CITIES = ['Washington', 'Springfield', 'Franklin', 'Greenville', 'Bristol', 'Fairview', 'Salem',
'Madison', 'Georgetown', 'Arlington', 'Ashland',]
STATES = ['MO','SC','IN','CA','IA','DE','ID','AK','NE','VA','PR','IL','ND','OK','VT','DC','CO','MS',
'CT','ME','MN','NV','HI','MT','PA','SD','WA','NJ','NC','WV','AL','AR','FL','NM','KY','GA','MA',
'KS','VI','MI','UT','AZ','WI','RI','NY','TN','OH','TX','AS','MD','OR','MP','LA','WY','GU','NH']

# while True:
#     first_name, last_name = random.sample(FIRST_NAMES, 2)
#     data = {
#     'tr_time_str': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
#     'first_name': first_name,
#     'last_name': last_name,
#     'city': random.choice(CITIES),
#     'state':random.choice(STATES),
#     'product': random.choice(PRODUCTS),
#     'amount': float(random.randrange(50000, 70000)) / 100,
#     }

for i in range(20):
    name = random.sample(FIRST_NAMES, 1)
    amount = float(random.randrange(50000, 70000)) / 100
    with open('./data.csv','a' ) as f:
        f.write(name +','+ '%s'%amount + '\n')


