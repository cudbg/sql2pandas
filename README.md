# sql2pandas

This is a simple query compiler that parses a reasonable subset of SQL,
transforms the query into a logical plan, type checks and optimizes it,
and uses query compilation to generate Pandas code.


Is this overkill?  Probably!   sql2pandas is retrofitted on top of the [databass](https://github.com/w4111/databass-public) 
course project for Columbia's [Introduction to Databases course](https://w4111.github.io).
You can read the [design doc](./design) for a high level overview of how
databass works.

