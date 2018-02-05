# Notes

The basic structure of this implementation is:
  1. run the `from` queries, to get the required tables
  2. filter the tables according to the `where` queries
  3. `select` the right columns from the filtered tables and output

The core construct is the `Table` class. When a CSV is first loaded, it is initialized into a `Table`. However, `Tables` or not really tied one-to-one for each CSV file. It's easier to think about the `Table` as a `TableView`; it could contain columns from many tables. The class itself keeps track of which columns belong to which table name for the duration of the query. This makes it really easy to run `where` filters on joined tables, because the fact that it is a joined table is abstracted away. Even things like `as` become simple to handle, as you'll see in the code.

`ColumnConfig` is another important component of the app, and is the main reason why a Table can contain columns from many different tables. The `ColumnConfig` contains everything needed to determine which columns are from which table. Because the data-structure is immutable, we can use `_replace()` functions to easily handle `as` queries, as mentioned above.

At the moment, because this was a 3 hour exercise, we basically join all tables into one mega table before running any `where` or `select` queries. What this means is that if you `select` a table with `Table(N)` rows, and another table with `Table(M)` rows, you end up with a table that is `Table(M * N)` rows. If you `select` a third table with `Q` rows, you end up with a table that is `Table(M * N * Q)` rows. This shortcut makes the current implementation unbelievably straight-forward, but obviously very inefficient.

Steps to make this implementation more efficient:
  - `Where` queries can be classified as `ColumnToLiteralWhere` queries or `ColumnToColumnWhere`. `ColumnToLiteralWhere` only require 1 table without any joins. So the first thing I would do is to run the `ColumnToLiteralWhere` queries on the original tables, before merging them. That should reduce the problem space significantly, to `Table(N' * M')`.
  - Now when you have `N' & M'`, you can either do the work in two passes, where the first pass is merging the two tables `Table(N' * M')`, and the second pass is running the filter, or you can do both in one pass. If there are no `limit` queries, than doing it in one pass will reduce work by 2x in the best case (1 loop instead of 2). If there are `limit` clauses, you can significantly reduce the amount of work you have to do, because you simply don't need to visit any more rows after you have the number you need.
  - Which leads me to the next and interesting implementation of a SQL engine - lazy evaluation. Essentially, you wouldn't want to do any work, till you absolutely need to. So in Python, I'd take all the applicable `ColumnToLiteralWhere` functions, and bind them to `Table(N)`, which would return a lazily-evaluating generator that can yield rows. This generator can be used by a `TableToTable` class to handle `ColumnToColumnWhere` queries, which itself is a generator, that can be used by the `select` query engine. So as the client requests a new row to display, the various connected generators yield a new row that matches all the required queries. If I had the time, this would be a fun implementation to write.
  - At some point, I'd move the querying/filtering logic into their own classes, that essentially can take as input one or more tables, a series of where queries, and optional indexes, and then return a generator that yields matching rows as required.

**This implementation has been cleaned with `yapf`**

I believe consistently formatted code is important when working in a team, and therefore use a formatter for my own code often. This helps to make the code better as well, because if the formatter produces a gnarly line, it's an indicator that the line should be re-written to be made simpler/easier-to-read.
