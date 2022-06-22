import time
import pandas as pd
from sql2pandas import *
from sql2pandas.tables import InMemoryTable
from sql2pandas.ops import *
from sql2pandas.compile import *
from sql2pandas.exprutil import *


if __name__ == "__main__":
  df = pd.DataFrame(dict(
    a = range(10),
    b = range(10, 20),
    c = range(10),
    d = [1] * 10
  ))
  db = Database.db()
  db.register_dataframe("data", df)
  q = sql2pandas("SELECT a, sum(b+2) as c FROM data GROUP BY a ORDER BY a")
  q.print_code()
  exit()


  q = sql2pandas("SELECT a, sum(a)+sum(b+2) as c FROM data GROUP BY a ORDER BY  a")
  q.print_code()

  q = sql2pandas("""SELECT a FROM data""")
  q.print_code()

  q = sql2pandas("""SELECT a, sum(a)+sum(b+2) * 2 as c 
      FROM data, (SELECT a as x FROM data) AS d2 
      WHERE (1*data.a) = d2.x GROUP BY a ORDER BY a """)
  q.print_code()


  q = sql2pandas("""SELECT a, sum(b+2) * 2 as c 
  FROM data, (SELECT a as x FROM data where a < 5) AS d2 
  WHERE data.a = d2.x
  GROUP BY a""")
  q.print_code()
  print(q(dict(data=df)))
  exit()

  q = sql2pandas("""
    SELECT d2.x, sum(data.b*2) as bar, sum(data.b*2)+1 as foo
    FROM data, (SELECT a as x FROM data) as d2
    WHERE a > (x-100)
    group by x, x
  """)

  print(q(dict(data=df)))

