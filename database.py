import sqlite3
import csv

conn = sqlite3.connect('electoral.sqlite')
cur = conn.cursor()

country = { 'E' : '1', 'N' : '2', 'S' : '3', 'W' : '4' }

###############################################################################
# Create Region Table
#
region = dict()

with open('data/CONSTITUENCY.csv') as csvfile:
    constituency = csv.reader(csvfile, delimiter=',')
    next(constituency, None)
    for row in constituency:
        if row[0] == '':
            continue
        else:
            r = row[7]
            key = row[9]
            val = int(country[r[0]] + r[1:])
            if key in region.keys():
                continue
            else:
                region[key] = val

cur.execute('''
CREATE TABLE IF NOT EXISTS region (
    id NUMBER PRIMARY KEY, 
    name TEXT
    )''')

for key in region:
    cur.execute("INSERT INTO region VALUES (?,?)", (region[key], key))

conn.commit()

###############################################################################
# Create constituency Table
#
constit = dict()

with open('data/CONSTITUENCY.csv') as csvfile:
    constituency = csv.reader(csvfile, delimiter=',')
    next(constituency, None)
    for row in constituency:
        if row[0] == '':
            continue
        else:
            r = row[2]
            key = row[1].lower()
            val = int(country[r[0]] + r[1:])
            if key in constit.keys():
                continue
            else:
                constit[key] = val


cur.execute('''
CREATE TABLE IF NOT EXISTS constituency (
    id NUMBER PRIMARY KEY, 
    name TEXT,
    type TEXT,
    electorate NUMBER,
    region_id NUMBER,
    FOREIGN KEY(region_id) REFERENCES region(id)
    )''')

with open('data/CONSTITUENCY.csv') as csvfile:
    constituency = csv.reader(csvfile, delimiter=',')
    next(constituency, None)
    for row in constituency:
        if row[0] == '':
            continue
        else:
            tup = (int(constit[row[1].lower()]), row[1], row[3], row[4], region[row[9]])
            cur.execute('''
                INSERT INTO constituency
                    VALUES (?,?,?,?,?)''', tup)
            conn.commit()
   
###############################################################################



###############################################################################
# Create party Table
# 
party = dict()
with open('data/RESULTS.csv') as csvfile:
    results = csv.reader(csvfile, delimiter=',')
    next(results, None)
    id = 0
    for row in results:
        key = row[17]
        if key in party.keys():
            continue
        else:
            party[key] = id
            id += 1


cur.execute('''
CREATE TABLE IF NOT EXISTS party (
    id NUMBER, 
    name text
    )''')

for key in party:
    cur.execute("INSERT INTO party VALUES (?,?)", (party[key], key))

conn.commit()
  
  
    

##############################################################################
# Create results Table
# 
cur.execute('''
CREATE TABLE IF NOT EXISTS results (
    constituency_id NUMBER,
    region_id NUMBER,
    party_id NUMBER,
    votes NUMBER,
    FOREIGN KEY(region_id) REFERENCES region(id)
    FOREIGN KEY(constituency_id) REFERENCES constituency(id)
    FOREIGN KEY(party_id) REFERENCES party(id)
    )''')

with open('data/RESULTS.csv') as csvfile:
    constituency = csv.reader(csvfile, delimiter=',')
    next(constituency, None)
    for row in constituency:
        if row[0] == '':
            continue
        else:
            tup = tuple([str(row[i]) for i in [3,5,7,17]])
            tup = (int(constit[row[3].lower()]), int(region[row[14]]), int(party[row[17]]), row[5])
            cur.execute('''
                INSERT INTO results
                    VALUES (?,?,?,?)''', tup)
            conn.commit()
