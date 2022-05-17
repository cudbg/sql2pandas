# Optimizer design



## Attribute disambiguation

Databass uses a row-based execution model, where each tuple is a Python list.
Before we can execute the plan, we need to translate each operator's attribute reference
into the appropriate index into the operator's input tuple.  

For instance, in the following plan, we want to replace the filter's `a` reference into the index 0.

      Filter(a > 100)
            |
      Scan(a, b, c)

To do so, the optimizer needs to

1. figure out each operator's output schema
2. for each operator, figure out the index its input schema for each attribute reference

## Join optimization

The Join optimizer design more closely follows how
[PostgreSQL performs join optimization](https://github.com/postgres/postgres/tree/master/src/backend/optimizer).
It uses the Selinger dynamic programming algorithm.
Starting with a single relation, it picks the best 2 relation join plan, then 3, etc.

We use a JoinInfo data structure (analagous to Postgres' RelOptInfo) to store the best
plans for a given set of relations.  We preferentially expand based on
non-crossproduct joins, and fall back to cross-product if that is the only option.
For instance, if we have the following query:

        SELECT ..
        FROM A, B, C, D
        WHERE A.a = B.a and A.a = C.a and C.a = D.a

THen we would construct the following JoinInfo objects:

        {A} {B} {C} {D}
        then
        {AB} {AC} {CD} 
        then
        {ABC} {ACD}
        then
        {ABCD}

For each JoinInfo object, we try all ways to break it into a binary join
between subsets of the relations, and we only consider subsets that have already
been constructed.  For instance, `{ABCD}` will only consider `{ABC}, {D}` and
`{ACD}, {B}`.
      

## Statistics collection

As you can see in [stats.py](../stats.py), we collect very simple statistics for estimation.  In addition,
we basically read the entire database in order to compute the statistics so it's expensive.  This is fine
for a toy database, but a more sophisticated implementation would:

* collect more sophisticated statistics, such as the cardinality, percentage of nulls, the most frequent values, 1 and n-D histograms, etc
* manage statistics as tables in the database
* estimate statistics via samples

