import sqlite3

conn = sqlite3.connect('crawl.sqlite')
cur = conn.cursor()

cur.execute('SELECT DISTINCT from_id  FROM Links')
#store distinct from ids into a list
from_ids=list()
for row in cur:
    from_ids.append(row[0])
#store to_ids into a list which are in from ids also
to_ids=list()
links=list()
cur.execute('SELECT DISTINCT from_id,to_id FROM Links')
for row in cur:
    fromid=row[0]
    toid=row[1]
    if fromid==toid:continue
    if fromid not in from_ids:continue
    if toid not in from_ids:continue
    links.append(row)
    if toid not in to_ids:to_ids.append(toid)
#store ids,and their previous ranks in a dictionary with is as keys
prev_ranks=dict()
for node in from_ids:
    cur.execute('SELECT newrank FROM Pages WHERE id=?',(node,))
    row=cur.fetchone()
    prev_ranks[node]=row[0]

#ask no. of iterations
sval=input('How many iterations:')
many=1
if(len(sval)>0):many=int(sval)

if len(prev_ranks) < 1 :
    print("Nothing to page rank.  Check data.")
    quit()

for i in range(many):
    #make a dict which contains next ranks after single iteration
    next_ranks=dict()
    total=0.0

    for (node,old_rank) in list(prev_ranks.items()):
        total=total+old_rank
        next_ranks[node]=0.0

    #
    for (node,old_rank) in list(prev_ranks.items()):
        give_ids=list()
        for (from_id,to_id) in links:
            if from_id != node : continue
           #  print '   ',from_id,to_id

            if to_id not in to_ids: continue
            give_ids.append(to_id)
        if len(give_ids)<1:continue
        amount=old_rank/len(give_ids)

        for id in give_ids:
            next_ranks[id] = next_ranks[id] + amount

    newtot =0
    for(node,next_rank) in list(next_ranks.items()):
        newtot=newtot+next_rank
    evap = (total - newtot) / len(next_ranks)

    for node in next_ranks:
        next_ranks[node] = next_ranks[node] + evap

    newtot=0
    for (node, next_rank) in list(next_ranks.items()):
        newtot = newtot + next_rank


    totdiff=0
    for (node,old_rank) in list(prev_ranks.items()):
        new_rank=next_ranks[node]
        diff=abs(old_rank-new_rank)
        totdiff=totdiff+diff

    avediff=totdiff/len(prev_ranks)
    print(i+1,avediff)

    prev_ranks = next_ranks

print(list(next_ranks.items())[:5])
cur.execute('''UPDATE Pages SET oldrank = newrank''')
for(id,new_rank) in list(next_ranks.items()):
    cur.execute('''UPDATE Pages SET newrank =? WHERE id=?''',(new_rank,id))
conn.commit()
cur.close()
