import csv
from collections import namedtuple

with open("mysql-collations.txt") as f:
    data = list(csv.reader(f, delimiter="\t"))

collations = namedtuple("collations", data[0])
m = {}
for d in data[1:]:
    c = collations(*d)
    m[int(c.ID)] = c

lines = ["from collections import namedtuple",
         f'collations = namedtuple("collations", {repr(data[0])})',
         f'collations_mapper = {repr(m)}',
         '''def get_collation_by_id(colid):
    return collations_mapper.get(colid, None)        
         ''']

print("\n".join(lines))
