import csv
import sqlite3

# open the connection to the database
conn = sqlite3.connect('SkinC/SC_data.db')
cur = conn.cursor()

# drop the data from the table so that if we rerun the file, we don't repeat values
conn.execute('DROP TABLE IF EXISTS deployments')
print("table dropped successfully");
# create table again
conn.execute('CREATE TABLE deployments (ITEM_NAME TEXT, ITEM_BRAND TEXT, ITEM_PRICE REAL, CATEGORY TEXT, ITEM_STATUES TEXT, YEAR INTEGER, MONTH INTEGER)')
print("table created successfully");

conn.execute('DROP TABLE IF EXISTS status')
print("table dropped successfully");
# create the status table again  
conn.execute('CREATE TABLE status (price INTEGER PRIMARY KEY, title TEXT, stars REAL, vote INTEGER)')
print("table created successfully");

# open the file to read it into the database
with open('/home/codio/workspace/SkinC/Skin_care/skin_care_df.csv', newline='') as f:
    reader = csv.reader(f, delimiter=",")
    next(reader) # skip the header line
    for row in reader:
        print(row)

        ITEM_NAME = row[0]
        ITEM_BRAND = row[1]
        ITEM_PRICE = row[2]
        CATEGORY = row[3]
        ITEM_STATUES = row[4]
        YEAR = row[5]
        MONTH = row[6]

        cur.execute('INSERT INTO deployments VALUES ( ?,?,?,?,?,?,?)', (ITEM_NAME, ITEM_BRAND, ITEM_PRICE, CATEGORY, ITEM_STATUES, YEAR ,MONTH))
        conn.commit()
print("data parsed successfully");


# open the file to read it into the database
with open('/home/codio/workspace/SkinC/Skin_care/skin_care.csv', newline='') as f:
    reader = csv.reader(f, delimiter=",")
    next(reader) # skip the header line
    print('start to work on the status table')
    for row in reader:
        print(row)
        # check if the row starts with empty string (avoid reading empty lines after the data)
        if row[0]: 
            try:
              price = int(row[0])
              print('ITEM_PRICE', price)
              # link between two tables
              cur.execute('SELECT * from deployments WHERE ITEM_PRICE=?', (price,))
              temp_row = cur.fetchall() #temp_row is a tuple, and not an array, so need first item from first item

              title = row[1]
              stars = float(row[2])
              vote = int(row[3])

              # print(temperature)
              cur.execute('INSERT INTO status (price, title, stars, vote) VALUES ( ?,?,?,?)', (price, title, stars, vote))
              conn.commit()
            except Exception: # if there are missing values, go to the next row
              pass
        else: # stop reading when reaching empty lines.
            break


print("data parsed successfully");



conn.close()