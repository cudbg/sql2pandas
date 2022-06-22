# sql2pandas

This is a simple query compiler that parses a reasonable subset of SQL,
transforms the query into a logical plan, type checks and optimizes it,
and uses query compilation to generate Pandas code.


Is this overkill?  Probably!   sql2pandas is retrofitted on top of the [databass](https://github.com/w4111/databass-public) 
course project for Columbia's [Introduction to Databases course](https://w4111.github.io).
You can read the [design doc](./design.md) for a high level overview of how
databass works.


# Install and Use

Install

    pip install sql2pandas


Run the following python code in the prompt or as a script:

```python    
import sql2pandas
import pandas as pd

# load a data frame into the "database"
db = sql2pandas.Database.db()
db.register_dataframe("data", pd.DataFrame(dict(a=range(10), b=range(10))))

# translate the SQL!
q = sql2pandas.sql2pandas("SELECT a, sum(b+2) as c FROM data GROUP BY a ORDER BY a")
print(q.code)
```

You should see the generated Pandas code like:

```python
import pandas as pd
csv = '''YOUR CSV DATA HERE'''
data = pd.read_csv(io.StringIO(csv))

# Start Groupby GROUPBY(data.a:num, |, data.a:num as a, sum(data.b:num + 2.0) as c, data.a:num as _ordby_0)
df = (data.assign(_gexpr=data.iloc[:,0], _agg_tmp=(data.iloc[:,1]) + (2.0))
  .groupby(["_gexpr"])
  .agg(a=('a', 'first'), _agg_arg=('_agg_tmp', 'sum'), _ordby_0=('a', 'first')))
df = df = df.assign(c=df.iloc[:,1]).drop(["_agg_arg"], axis=1)
# End Groupby

df_1 = (df.sort_values(["_ordby_0"], ascending=[1])
  .drop(["_ordby_0"], axis=1))
df_1 = df_1.assign(a=df_1.iloc[:,0],c=df_1.iloc[:,1])[['a', 'c']]
print(df_1)
```

