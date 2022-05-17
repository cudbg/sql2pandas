import time
import pandas as pd
from sql2pandas import *
from sql2pandas.tables import InMemoryTable
from sql2pandas.ops import *
from sql2pandas.compile import *
from sql2pandas.exprutil import *


if __name__ == "__main__":
  q = sql2pandas("""SELECT a FROM data""")
  print(q.code)
  exit()

  q = sql2pandas("""
    SELECT d2.x, sum(data.b*2) as bar, sum(data.b*2)+1 as foo
    FROM data, (SELECT a as x FROM data) as d2
    WHERE a > (x-100)
    group by x, x
  """)

  df = pd.DataFrame(dict(
    a = range(10),
    b = range(10, 20),
    c = range(10),
    d = [1] * 10
  ))
  print(q(dict(data=df)))

