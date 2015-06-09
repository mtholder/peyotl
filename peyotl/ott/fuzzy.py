#!/usr/bin/env python
# requires SimString swig python bindings from http://chokkan.org/software/simstring/
#   and:
#   pip install python-Levenshtein
from Levenshtein import jaro
from peyotl.ott import OTT
import simstring
from time import time
import sys

def print_query(fuzzy_db, ott, queries):
    for name in queries:
      print(time())
      nl = name.lower()
      m =  db.retrieve(nl)
      if m:
          s = [(jaro(i, nl), i) for i in m]
          s.sort(reverse=True)
          for score_lcname in s:
              score, lcname = score_lcname
              n = ott.lcname_to_name[lcname]
              if not isinstance(n, list):
                  n = [n]
              for name in n:
                  ott_id = ott.name_to_ott_id[name]
                  print(score, name, ott_id, ott.ott_id_to_info[ott_id])
                  
      else:
          sys.stderr.write('No matches for "{}"\n'.format(name))
    print(time())

if __name__ == '__main__':
    ott = OTT()
    db = simstring.reader(sys.argv[1])
    db.measure = simstring.cosine
    db.threshold = 0.65
    print_query(db, ott, sys.argv[2:])
