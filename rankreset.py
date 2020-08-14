import sqlite3

conn = sqlite3.connect('crawl.sqlite')
cur = conn.cursor()

cur.execute('''UPDATE Pages SET newrank=1.0, oldrank=0.0''')
conn.commit()

cur.close()

print("All pages set to a rank of 1.0")
