# An unofficial guide to raw SQL with PostgreSQL and Rails

> **Warning**
> This document is still very WIP

- [An unofficial guide to raw SQL with PostgreSQL and Rails](#an-unofficial-guide-to-raw-sql-with-postgresql-and-rails)
  - [This document assumes PostgreSQL](#this-document-assumes-postgresql)
  - [What decisions do you need to make?](#what-decisions-do-you-need-to-make)
  - [Essential background knowledge](#essential-background-knowledge)
    - [Essential background: Layers of APIs which can run SQL](#essential-background-layers-of-apis-which-can-run-sql)
    - [Essential background: Postgres wire protocol](#essential-background-postgres-wire-protocol)
  - [Step: Verify you actually need need to use raw SQL](#step-verify-you-actually-need-need-to-use-raw-sql)
    - [When should you use raw SQL?](#when-should-you-use-raw-sql)
    - [What do I give up by choosing raw SQL?](#what-do-i-give-up-by-choosing-raw-sql)
  - [Step: Write a good raw SQL query](#step-write-a-good-raw-sql-query)
  - [Step: Interpolate data values safely](#step-interpolate-data-values-safely)
  - [Step: Store the raw SQL query](#step-store-the-raw-sql-query)
    - [Option 1: Store as string in code](#option-1-store-as-string-in-code)
      - [1. ActiveRecord::Base.connection.execute --> PG::Result](#1-activerecordbaseconnectionexecute----pgresult)
      - [2. ActiveRecord::Base.connection.select_all --> ActiveRecord::Result](#2-activerecordbaseconnectionselect_all----activerecordresult)
      - [3. MyModel.find_by_sql --> Array\<MyModel\>](#3-mymodelfind_by_sql----arraymymodel)
      - [4. ActiveRecord::Base.connection.exec_query --> ActiveRecord::Result](#4-activerecordbaseconnectionexec_query----activerecordresult)
      - [5. PG::Connection#exec --> PG::Result (hint: don't use this)](#5-pgconnectionexec----pgresult-hint-dont-use-this)
      - [6. PG::Connection#exec_params --> PG::Result](#6-pgconnectionexec_params----pgresult)
    - [Option 2: Save as database view](#option-2-save-as-database-view)
      - [Consider a materialized view](#consider-a-materialized-view)
  - [Appendices](#appendices)
    - [Appendix: Do I need to call PG::Result#clear?](#appendix-do-i-need-to-call-pgresultclear)
    - [Appendix: What about manually freeing MySQL result memory?](#appendix-what-about-manually-freeing-mysql-result-memory)
    - [Appendix: %q and %Q are handy when building large SQL strings](#appendix-q-and-q-are-handy-when-building-large-sql-strings)
    - [Appendix: `?` confusing in ActiveRecord (hint: it is NOT bound parameters )](#appendix--confusing-in-activerecord-hint-it-is-not-bound-parameters-)
    - [Appendix: Debugging tips](#appendix-debugging-tips)
    - [Appendix: Suggested development workflow for writing raw SQL in Rails](#appendix-suggested-development-workflow-for-writing-raw-sql-in-rails)
    - [Appendix: Debugging with Wireshark](#appendix-debugging-with-wireshark)
    - [Appendix: Avoiding excessive memory usage with a CURSOR](#appendix-avoiding-excessive-memory-usage-with-a-cursor)
      - [Background: PostgreSQL SQL-level Cursors](#background-postgresql-sql-level-cursors)
      - [Implementing a cursor in raw SQL](#implementing-a-cursor-in-raw-sql)
    - [Appendix: Rails 'binds' parameter](#appendix-rails-binds-parameter)
      - [Types](#types)
      - [Examples in usage](#examples-in-usage)
    - [Appendix: Sanitizing SQL strings in Rails](#appendix-sanitizing-sql-strings-in-rails)
    - [Appendix: Using PG::Result efficiently](#appendix-using-pgresult-efficiently)
    - [Appendix: How much memory copying happens when converting PG::Result into ActiveRecord::Result?](#appendix-how-much-memory-copying-happens-when-converting-pgresult-into-activerecordresult)
    - [Appendix: Do I need to use a prepared statement?](#appendix-do-i-need-to-use-a-prepared-statement)
    - [Appendix: Bulk transfer data to/from the DB with SQL COPY](#appendix-bulk-transfer-data-tofrom-the-db-with-sql-copy)
    - [Appendix: Rails and EXPLAIN](#appendix-rails-and-explain)
    - [Appendix: Using EXPLAIN well](#appendix-using-explain-well)

## This document assumes PostgreSQL

This document assumes PostgreSQL because that is what I know. Most of this advice will also apply to MySQL but will differ in the lower level details. MySQL is a feature rich database so it should be straightforward to find the MySQL equivalent of any Postgres specific functions mentioned below.

## What decisions do you need to make?

Broadly speaking, these are the sequence of decisions and actions you will need. The rest of this document explains these in more detail.

```mermaid
graph TD
    A[My Problem]
    B[Can do easily in ActiveRecord?]
    I[Where should query live?]
    C[Use ActiveRecord]
    D[Use Raw SQL]
    E[Write the SQL]
    M[Need CURSOR for mem management?]
    P[How to run query?]

    A --> B
    B -->|Yes| C
    B -->|No| D
    D --> E
    E --> I
    I --> P
    P --> M
```

1. Verify that you cannot achieve the outcomes you need with just normal ActiveRecord queries and you do actually need to write raw SQL.
1. Write the SQL query you want to use.
1. Choose how to embed data values into your SQL (if required).
2. Decide where to store the query, one of:
   1. A PORO _query object_ e.g. `app/queries/quarterly_report_query.rb` or `app/services/quarterly_report_query.rb`
   2. A _database view_ (optionally a _materialised database view_)
3. If the query will live in a query object, choose how much memory copying you are ok with. Options are:
    * I **need** absolutely minimal memory copying so need lower level APIs
        * Decide whether you need to use a CURSOR to manage memory usage of the results
    * Some data copying is ok
    * I don't care about memory usage because my dataset is small enough

## Essential background knowledge

Before we can discuss these decisions in detail, there is some background knowledge we need.

### Essential background: Layers of APIs which can run SQL

We need to be aware of all the APIs that let us interact with the database, especially the lower level layers which, while they are too low level for day to day use, can be very useful for debugging.

To help understand how the various APIs relate to each other, we will think of them in layers:

* _High-level_ API Layers
    1. Standard everyday `ActiveRecord` methods that we use all the time.
* _Mid-level_ API Layers
    1. ActiveRecord methods which let you pass raw SQL
    1.  `pg` gem methods
* _Low-level_ API layers
    1. [libpq](https://www.postgresql.org/docs/current/libpq.html)
        * ships with Postgres itself
        * The `pg` gem is a Ruby wrapper for libpq
    1. [Postgres wire protocol](https://www.postgresql.org/docs/current/protocol.html)
        * Defines the format of the messages exchanged between your app and the database
        * It's never practical to code this low for real work but inspecting it with [Wireshark](https://www.wireshark.org/) can be useful for debugging what ActiveRecord is doing on your behalf (more on this later).

This guide is concerned with the "mid-level" API layer i.e.

1. ActiveRecord methods which let you run raw SQL queries
1.  `pg` gem methods which let you run raw SQL queries

We need to understand the Postgres wire protocol layer to make sense of the APIs so we will discuss that next.

### Essential background: Postgres wire protocol

Your Rails app can use one of 3 "sub-protocols" to exchange data with the PostgreSQL server. These sub-protocols are [well documented in the PostgreSQL manual](https://www.postgresql.org/docs/current/protocol-overview.html).

The sub-protocols are:

1. Extended query sub-protocol
    * Query processing split into multiple steps. You app sends a **separate message** to the database for each step
        1. Parsing (textual-query --> [Parser] --> prepared-statement)
        2. Binding parameter values (prepared-statement --> [Binder] --> portal)
        3. Execution (portal --> [Executor] --> results)
    * This protocol is more complex than the _Simple query_ sub-protocol (see below) but also more flexible, more secure, and has better performance for repeated queries.
    * **It completely eliminates SQLi if you bind all your data values!** The database receives the SQL string and the data values as **separate messages** so it cannot get confused and treat a data value as part of the SQL string.
1. Simple query sub-protocol
    * Your app sends a single string (which already has all required data values interpolated into it). The string is parsed and immediately executed by the server.
    * This is the older sub-protocol. It is simpler but less flexible and less secure than the _Extended query sub-protocol_
1. `COPY` operations sub-protocol
    * This is a special sub-protocol for copying large volumes of data to/from the database e.g. during import or export.

ActiveRecord will try to use the _Extended query_ sub-protocol when it can and otherwise fall back to the _Simple query sub-protocol_.

You can tell which sub-protocol is used by looking at the log output of the SQL query in your Rails log. Queries that use the _Extended query_ sub-protocol have the data binding markers visible in them e.g. `$1`, `$2` etc.

Remember that using the _Extended query sub-protocol_ is more secure if, and only if, you pass your data values as bound parameters.

One of the criteria we will use to evaluate an API is whether it allows us to pass bound parameters as a separate step, thereby eliminating SQLi.

## Step: Verify you actually need need to use raw SQL

### When should you use raw SQL?

We want the following outcomes when we interact with a database:

1. We want the queries to be **fast enough**.
2. We don't want to allocate **too much** memory to perform the query and get the results into a usable form.
3. We want to be safe from SQL injection.

Most of the time, we can all these outcomes from normal `ActiveRecord` methods. This document is about what to do when that doesn't work.

You will notice that the goals above have some squishy language about _fast enough_ and _not too much_ memory because the exact meaning depends on your application. It's too slow if **your team** thinks it is too slow.

### What do I give up by choosing raw SQL?

All architectural decisions have trade-offs. Here are some of the costs of using raw SQL in Rails:

1. The relationships between your tables, which normally lives solely in the Rails relationships between your models, is now duplicated in the raw SQL queries. If you need to change the relationships in future, you will need to change both places.
1. Sometimes ActiveRecord will be faster than raw SQL. ActiveRecord (or any ORM) implements some kinds of caching which can speed up SQL queries which you application uses a lot.
1. Future maintainers have to know enough SQL to be able to maintain what you wrote. Who will maintain this if you aren't around?

## Step: Write a good raw SQL query

A full discussion of writing good SQL is outside of scope of this doc. The following tips may be useful:

* Think in terms of a pipeline of tables, starting with tables on disk flowing into temporary tables in memory and finally an in-memory table that gets sent to your app
* Write SQL for clarity first. Split large queries into smaller subqueries with `WITH` whenever possible.
* Use the [GitLab SQL style guide](https://about.gitlab.com/handbook/business-technology/data-team/platform/sql-style-guide/) directly or as a starting point for your in-house style  guide.
* Be familiar with some particularly useful PostgreSQL functions:
	* [COALESCE()](https://www.postgresql.org/docs/14/functions-conditional.html#FUNCTIONS-COALESCE-NVL-IFNULL) (to set fallback values)
	* [CASE](https://www.postgresql.org/docs/14/functions-conditional.html#FUNCTIONS-CASE) statements (to do if-else logic)
	* [TO_*() functions](https://www.postgresql.org/docs/14/functions-formatting.html) to cast between types


## Step: Interpolate data values safely

    TODO
    this depends on how you choose to run the query
    if you make a db view, then you can use normal AR to interpolate data values in
    options
        string interpolation
            wrap query in PREPARE..EXECUTE
            sanitize values before interpolate
                use sanitize_sql_array method to operate on string directly
        use extended query sub-protocol
                use binds param with rails QueryAttribute objects
                use PG::Connection#exec_query

    ===========================
    Appendix: Avoiding SQL injection

    If possible, use methods which use the extended query sub-protocol to completely avoid SQLi. If that isn't feasible then you can wrap your raw SQL in a prepared statement.

    Pros/cons of PREPARE...EXECUTE

    * ?? How does it compare to sanitizing your SQL string? It seems safer because it eliminates the possibility of the sanitization failing
    * ++ It gives you good protection against SQLi
    * -- You have to clean up the prepared statement afterwards
    * -- You have to use raw SQL to achieve this
    * -- It's more fiddly than using the extended query sub-protocol

## Step: Store the raw SQL query

```mermaid
graph TD
    I{Where should my SQL live?}
    I --> J[Store as string in my code]
    I --> K["Store as a DB View"]
    K --> L["Use materialized view?"]
```

You have some choices about how to have the database invoke your SQL:

1. Store as a string constant in your code and send as regular SQL query
2. Save as a database view, optionally as a materialized view


### Option 1: Store as string in code

There are 6 common ways of sending a SQL string to a PostgreSQL database in Rails:

| Method name                              | Return type          | Allows bound params? | Allow prepared stmts? | Recommended?       | Mem efficient?      |
| ---------------------------------------- | -------------------- | -------------------- | --------------------- | ------------------ | ------------------- |
| MyModel.find_by_sql                      | Array\<MyModel\>     | :white_check_mark:   | :white_check_mark:    | :white_check_mark: | :warning: :warning: |
| ActiveRecord::Base.connection.select_all | ActiveRecord::Result | :white_check_mark:   | :white_check_mark:    | :white_check_mark: | :warning:           |
| ActiveRecord::Base.connection.exec_query | ActiveRecord::Result | :white_check_mark:   | :white_check_mark:    | :white_check_mark: | :warning:           |
| ActiveRecord::Base.connection.execute    | PG::Result           | :x:                  | :x:                   | :x:                | :white_check_mark:  |
| PG::Connection#exec_params               | PG::Result           | :white_check_mark:   | :x:                   | :white_check_mark: | :white_check_mark:  |
| PG::Connection#exec                      | PG::Result           | :x:                  | :x:                   | :x:                | :white_check_mark:  |

#### 1. ActiveRecord::Base.connection.execute --> PG::Result

    Signature = ActiveRecord::Base.connection.execute(sql, name = nil) -> PG::Result
    Return type = PG::Result
    Errors: Raises PG::Error (or a child of PG::Error e.g. PG::UndefinedTable)
    Sub-protocol = Always simple
    Built-in SQLi protection = none
    Memory usage:
        Allocates AR models = no
        ?? num copies of data made
        ?? for every 1k of result from the DB how much mem allocated?
    Gotchas =
        You probably have to call #clear to clean up memory ???
    Notes
        The `name` param is not sent to the DB but it is used as a prefix for the query line in the log - you should include it
    Avoid SQLi by
        Use PREPARE...EXECUTE in your SQL (remembering that you will need to `DEALLOCATE stmtname;` after you get your results)
        OR
        escape your values before you interpolate them (riskier)

* calls `connection.async_exec(sql)` under the hood (wrapped in some logging and sql transformation)
* returns `PG::Result` - this is basically just a slightly lower level version of `exec_query`
    * Rails docs recommend `exec_query` unless you need the `PG::Result` specifically because  you have call clear manually
    * -- main down side of this method is that you cannot pass it bound params (even though the stuff it calls within AR supports them)
* Always uses _Simple Query sub-protocol_ at Postgres wire protocol level
* To avoid SQLi, you must either
    1. use `PREPARE...EXECUTE` in your SQL string to get separation
    1. OR just rely on escaping.
* Example: `ActiveRecord::Base.connection.execute(sql, name = nil)`
* Executes an SQL statement, returning a PG::Result object on success or raising a PG::Error exception otherwise. Note: the PG::Result object is manually memory managed; if you don't need it specifically, you may want consider the exec_query wrapper
* example:
    ```ruby
    sql = "SELECT ..."
    pg_result = ActiveRecord::Base.connection.execute(sql)
    results = pg_result.values
    pg_result.clear

    results
    ```
* You can do a SQL level `PREPARE...EXECUTE` to get more safety with this method:
    ```ruby
    def do_safe_exec(sql_query, params)
        sql_query = "#{sql_query};" unless sql_query.ends_with?(";")

        stmt_name = "a#{Time.now.to_i}"
        unrolled_params = params.map { |param| "'#{param}'" }.join(",")

        prepared_sql = <<~EO_SQL
            PREPARE #{stmt_name} AS #{sql_query}
            EXECUTE #{stmt_name}(#{unrolled_params});
        EO_SQL

        pg_result = ActiveRecord::Base.connection.execute(prepared_sql)

        # clear the statement from the DB. TODO: is this required?
        ActiveRecord::Base.connection.execute("DEALLOCATE #{stmt_name};")

        results = pg_result.to_a

        # Clear the memory associated with the PG::Result
        pg_result.clear

        results
    end
    ```
* A caveat to using execute is depending on your database connector, the result returned by this method might require manual garbage collection. Consider using exec_query instead.
    * TODO: docs mention this but I can't find examples of doing the cleanup

Conclusion

#### 2. ActiveRecord::Base.connection.select_all --> ActiveRecord::Result

* Documented in Rails guide
* Works a bit like `MyModel.find_by_sql` but does not instantiate ActiveRecord objects - it returns arrays of hashes wrapped in an `ActiveRecord::Result`
* returns: `ActiveRecord::Result`
* Example: `ActiveRecord::Base.connection.select_all`
* `ActiveRecord::Base.connection.select_all(arel, name = nil, binds = [], preparable: nil)`
* Note: lives on the connection adapter, not the model but can get at the instance of the connection adapter class via **any** model
        * e.g. `ActiveRecord::Base.connection == User.connection == Page.connection`
* Returns an instance of `ActiveRecord::Result` which can be converted to an array of hashes
* Just runs a SQL query and gives back data - it does not create ActiveRecord objects from that data. This makes it very useful for building reports if we don't need to do "Active Model" things with the returned data.
* Does Parse/Bind/Describe/Execute/Sync at the Postgres protocol level but only safer if you use `binds`.
* You can send bound parameters but it has the same ergonomic problems as `ActiveRecord::Base.connection.exec_query`

Conclusion

* -- bound params handling is a faff, relies on private-ish API
* ?? What does ActiveRecord::Result have that PG::Result doesn't? Is it worth the memory copying
* ++ it supports prepared statements but doesn't force you to use them

#### 3. MyModel.find_by_sql --> Array\<MyModel\>

    Signature: MyModel.find_by_sql(sql, binds = [], preparable: nil, &block) -> Array\<MyModel\>

* Calls `connection.select_all` under the hood
* Documented in Rails guide
* Example: `SomeModel.find_by_sql`
* returns an actual `Array` of `SomeModel` objects (not an `ActiveRecord::Relation`)
* examples:
    ```ruby
    Page.find_by_sql("select * from pages where id = 3") # => Array<Page>

    # If the `sql` param is an array, then AR will interpret it as a SQL string
    # template with values to interpolate in. Note that the ? and :foo are
    # replaced by AR not Postgres.  All the `?` and `:foo` values are
    # interpolated into the string **before** it is sent to Postgres. The
    # examples below send exactly the same SQL to Postgres!
    Page.find_by_sql("select * from pages where id = 3") # => Array<Page>
    Page.find_by_sql(["select * from pages where id = ?", 3]) # note we have to wrap the SQL in an array - bit goofy
    Page.find_by_sql(["select * from pages where id = :some_id", {some_id: 3}])
    ```
* Caveat: Not really suitable for doing INSERT/UPDATE/DELETE
* Caveat: Wrapping all args in an array when you want to interpolate data is goofy
* Does Parse/Bind/Describe/Execute/Sync at the Postgres protocol level but doesn't actually BIND any params (data values are interpolated into the SQL)
* You can send bound parameters but it has the same ergonomic problems as `ActiveRecord::Base.connection.exec_query`

Conclusion:

* -- bound params handling is a faff, relies on undocumented Rails stuff
* -- can only be used for SELECT queries
* ++/-- Creates actual AR model instances for data returned. This
    * could be really handy or annoying if you are trying to minimize memory usage

#### 4. ActiveRecord::Base.connection.exec_query --> ActiveRecord::Result

* Returns instance of `ActiveRecord::Result` which can be converted to an array of hashes
    * ActiveRecords::Result object which has handy methods like .columns and .rows to access headers and values.
* Example: `ActiveRecord::Base.connection.exec_query(sql, name = "SQL", binds = [], prepare: false)`
* Technically this is private API
* Executes sql statement in the context of this connection using `binds` as the bind substitutes. `name` is logged along with the executed sql statement.
* Uses Postgres Parse/Bind/Describe/Execute/Sync protocol messages properly but only if you supply data in `binds`
* Caveat: creating the bind parameters is really fiddly and uses a bunch of undocumented AR objects - it all _feels_ like private API
* example:
    ```ruby
    # works
    full_sql = "select * from pages where template = 'topics'"
    ActiveRecord::Base.connection.exec_query(full_sql)
    # => ActiveRecord::Result

    # works
    sql = "select * from pages where template = $1"
    binds = [
        ActiveRecord::Relation::QueryAttribute.new(
        nil,                          # name
        "August 2019",                # value
        ActiveRecord::Type::Text.new  # type
        )
    ]
    ActiveRecord::Base.connection.exec_query(sql, 'blah', binds)
    # => ActiveRecord::Result
    ```
Conclusion:

* ++ ActiveRecord::Result has some handy methods
* ++ it calls #clear for you on the pg_result
* ++ having a prefix for the logs is handy
* -- An unknown amount of mem copying happens to get the `PG::Result` into the `ActiveRecord::Result`
* -- Using `binds` not well documented, so hopefully devs remember to quote their data before embedding it in the SQL
* -- bound params handling is a faff, relies on undocumented Rails stuff


#### 5. PG::Connection#exec --> PG::Result (hint: don't use this)

> **warning** I'm documenting this so you know to avoid it. Use #exec_params instead.

To use API from this layer you need to get access to the `PG::Connection` from ActiveRecord via `ActiveRecord::Base.connection.raw_connection`
* sends raw SQL, gets back arrays of hashes
* fields in the hashes have types set by the relevant PG Type-map
* seems to be a pretty close wrapper to the libpq API
* -- Always uses simple protocol
* -- You must do sanitization yourself to avoid SQLi

Conclusion:

* -- I can't think of a reason to use this instead of exec_params
* -- Don't use this method, exec_params is a safer version of this

#### 6. PG::Connection#exec_params --> PG::Result

https://deveiate.org/code/pg/PG/Connection.html#method-i-exec_params

> If the types are not specified, they will be inferred by PostgreSQL. Instead
> of specifying type oids, it's recommended to simply add explicit casts in the
> query to ensure that the right type is used.

```ruby
# TODO: this code untested

conn = ActiveRecord::Base.connection.raw_connection

# SQL with explicit type casts
# we use explicit type casts because it's less fiddly than trying to cast the params before sending them to the DB
# just lookup the types of the columns that the values are searching with \dt in psql and use those as the cast
sql = "SELECT * FROM users where id = $1::int AND email = $2::character varying"

# params should all be strings according to the docs
params = [
    12.to_s,
    "foo-1@example.com"
]
pg_result = conn.exec_params(sql, params)
p pg_result.to_a
pg_result.clear

# OR

conn.exec_params(sql, params) do |pg_result|
p pg_result.to_a
    # pg_result.clear automatically called at end of block
end
```

* Does Parse/Bind/Describe/Execute/Sync at the Postgres protocol level
* ++ You can pass bound parameters
* ++ Passing bound parameters is fairly straight forward
* ++ it doesn't create a prepared statement that you later have to clean up
* ++ there are less layers of AR sugar to get through which might copy results and increase memory usage
* ++ only allows one SQL statement per execution (this could also be annoying but mostly it's better for SQLi safety
* Seems to work well for executing raw SQL in the safest manner possible

Q: Should we still quote/sanitize values before using this method?


### Option 2: Save as database view

    in some ways a view just moves your problem
    you can choose whether you need raw SQL to access the view or standard AR

Recommendation: Use https://github.com/scenic-views/scenic to manage your views

Reasons to use a view

It lets you write raw SQL but then use normal activerecord methods to query it
    ++ maintainable for other devs
    ++ you can ignore the whole seciton about "how should I send my raw SQL"
++ view data is never out of date (compared to materialized view)
++ works great if query is fast enough for your need

#### Consider a materialized view


Recommendation: Use https://github.com/scenic-views/scenic to manage your views

* ++ queries just as fast as any other table
* -- you need to be able to tolerate somewhat stale data
    * in a report export I guess you could refresh the view as first step but not sure how slow that would be?

How to setup triggers in rails to update it when any of the tables involved are updated?
* -- a cron job of some kind needs to run to update it

Examples: spreadsheet import or export

TODO: is SQL COPY useful for this? I have never tried

    Q: can views take params from code? Presume not? but you can do SQL queries on the view instead

## Appendices

### Appendix: Do I need to call PG::Result#clear?

> Explicit calling clear can lead to better memory performance, but is not
> generally necessary.
>
> https://deveiate.org/code/pg/PG/Result.html

### Appendix: What about manually freeing MySQL result memory?

The equivalent of PG::Result#clear in MySQL is called `free`. See https://github.com/noahgibbs/mysql_bloat_test/blob/master/mb_test.rb

### Appendix: %q and %Q are handy when building large SQL strings

`%q` or `%Q` are handy to simplify quoting in long SQL strings which contain single and double quotes.

```
%q = create a string using "single quote rules" but you can choose your own delimiter
%Q = create a string using "double quote rules" but you choose your own delimiter i.e. use %Q if you need variable interpolation
```

Examples:

```ruby
[22] pry(main)> x = 34
34

[23] pry(main)> %q[this is literally #{x}!]
"this is literally \#{x}!"

[24] pry(main)> %Q[this is literally #{x}!]
"this is literally 34!"
```

### Appendix: `?` confusing in ActiveRecord (hint: it is NOT bound parameters )

ActiveRecord can interpolate values into a SQL string for you via:

1. Placeholder maker `?` which causes AR to match up its usage to the position of an arg within an array
1. Named placeholder e.g. `:foo` which is used as the key in a hash to get the value to replace

The `?` syntax **looks like MySQL bound parameters** but using it does not mean that the query will use bound parameters when sent to the DB!

It does mean that you are telling ActiveRecord to do escaping for you on the value which is probably safer than doing it yourself or (worst case) just interpolating it into the string yourself.

### Appendix: Debugging tips

Options

* Rails log file
    * usually this is all you need
    * The rails log will show you whether bound parameters where used or not
        ```ruby
        # This ruby ...
        User.where(created_at: (1.year.ago)..(1.second.ago))
        # ... generates the following log line. Notice the $1 and $2 and the nested
        # arrays of bound params - these are only present if bound parameters are
        # used
        #
        # User Load (1.1ms)  SELECT "users".* FROM "users" WHERE "users"."created_at" BETWEEN $1 AND $2  [["created_at", "2021-07-12 22:18:14.991108"], ["created_at", "2022-07-12 22:18:13.991346"]]
        ```
    * ++ in development mode it's pretty good record of what queries are sent to DB
    * -- doesn't tell you whether Rails is using the simple or extended wire protocol for a particular method.
* Use Wireshark to spy on the traffic to/from the DB
    * ++ the wire protocol is the ground truth
    * ++ easy enough to understand the conversation between Rails and DB - messages are pretty sensible looking to human eyes
    * -- fiddly setup especially if you don't use Wireshark on the regular

### Appendix: Suggested development workflow for writing raw SQL in Rails

I use the following workflow to create raw SQL queries in a Rails app:

1. I create a script which defines the SQL I need and runs it, then run that script in the Rails environment with [rails runner](https://guides.rubyonrails.org/command_line.html#bin-rails-runner). I may start the script in an interactive SQL environment such as [pgAdmin]() or [psql]() depending on how complex it is.
2. Once the basic SQL statement is working, I can move it into a query object or a Scenic view as required.

```ruby
# ./my_sql.rb
# Usage:
#     $ bundle exec rails runner ./my_sql.rb

sql = ~<<EO_SQL
  SELECT * FROM ...
EO_SQL

# capture some timestamps for very basic profiling
start_ts = Time.zone.now

pg_result = ActiveRecord::Base.connection.execute(sql)

# do stuff with pg_result here, maybe print it, maybe insert a pry statement to work with it interactively

end_ts = Time.zone.now

# you don't **need** to do this in this script but it's a good habit to get
# into.
pg_result.clear

puts "Finished. Took #{end_ts - start_ts} secs"
```

Then in terminal:

```bash
# terminal #1 ###########################
# load the Rails app and run ./my_sql.rb. This behaves a bit like a
# non-interactive version of `rails console`
$ bundle exec rails runner ./my_sql.rb


# terminal #2 ###########################
# Follow the output of the log file to see what SQL commands Rails is actually
# running.
$ tail -f log/development.log
```

### Appendix: Debugging with Wireshark

Wireshark will capture network packets on a given interface and present them in a UI for you to inspect the traffic.
You can use it to see exactly what your app is sending to the database.

First we need to run our Rails server but disable SSL (SSL would make it much more of a faff to see the contents of the network packets).

```bash
# Tell rails server to not use SSL to local postgres so traffic can be
# inspected with Wireshark
$ PGSSLMODE=disable bundle exec rails s
```

Assuming your PostgreSQL database is running on `localhost:5432`, you can set up
Wireshark to capture this by capturing on the loopback interface (`lo0`) with
the filter `port 5432` to capture just Postgres traffic on that interface.

If you read the [Postgres protocol docs](https://www.postgresql.org/docs/current/protocol.html) you will be equipped to understand the various messages exchanged between database client (the Rails app) and database server.

Explaining how to use [Wireshark](https://www.wireshark.org/) is beyond the scope of this guide but there is pretty good documentation on https://www.wireshark.org/.

### Appendix: Avoiding excessive memory usage with a CURSOR

Using a CURSOR to get the results of a query in pages lets us control Ruby memory usage at the cost of making more requests to the DB.

Aside: CURSOR is not suitable for rails pagination because it requires holding state open on the DB which isn't something you want between requests. A CURSOR is a way to "give me all the results for this query now but give them to me in batches"

You might not need a SQL cursor. The [ActiveRecord batches API](https://api.rubyonrails.org/v7.0.3/classes/ActiveRecord/Batches.html) is a better choice if it'll do the job.

The limitations of the Batches API are:

* You cannot specify the order, it will be ordered by the primary key (usually id)
* The primary key must be numeric (uuid primary keys will not work with Batches API)
* The query is rerun for each chunk (1000 rows), starting at the next id sequence. This _can_ be a problem if your query is slow. With a SQL CURSOR the query is run only once.

Options for using a cursor

1. Use https://github.com/afair/postgresql_cursor
   * :warning: This doesn't look very maintained but it might work.
2. Implement a SQL CURSOR yourself with raw SQL

We are going to discuss the second option here.

> **Info**
> Despite the similar sounding name, the [rails_cursor_pagination](https://github.com/xing/rails_cursor_pagination) gem does not use SQL cursors so isn't applicable here.

#### Background: PostgreSQL SQL-level Cursors

* DECLARE
    * https://www.postgresql.org/docs/current/sql-declare.html
    * `WITH HOLD` allows the cursor to be accessed by later transactions in the same session (after the creating transaction has ended)
* OPEN
    * Part of SQL standard but Postgres doesn't implement it - cursors are considered OPEN when they are DECLAREd
* FETCH
    * https://www.postgresql.org/docs/current/sql-fetch.html
    * fetch results from the cursor
* MOVE
    * https://www.postgresql.org/docs/current/sql-move.html
    * move cursor within the result set without returning records
* CLOSE
    * https://www.postgresql.org/docs/current/sql-close.html
    * close the cursor and free all resources associated with it (open cursors copy their records to a tmp table in memory)

Use the `pg_cursors` system view to see all open cursors

```sql
my_db=# select * from pg_cursors;
 name | statement | is_holdable | is_binary | is_scrollable | creation_time
------+-----------+-------------+-----------+---------------+---------------
(0 rows)
```

#### Implementing a cursor in raw SQL

```ruby
# code/explore_cursor_sql_level.rb
require "pg"
require "securerandom"
# require "memory_profiler"


##
# Attempt 1:
# * I started using exec() instead of exec_params() when there were no params to bind. Is that bad practice?
# Outcomes
# * For each set of tuples we get from the DB we will allocate C mem and then Ruby mem
# * But the tuple set size can be controlled by the batch_size param so we can control memory usage in exchange for having to make more requests to the DB (we are sacrificing speed for memory usage control)
#
# Q: should i use exex_params here or one of th eother APIs. justify my choice
# 	if i used AR exec_query this would work for MySQL too maybe?
#
def fetch_w_cursor_attempt_1(conn, query_sql, binds: [], batch_size: 100)
	cursor_name = "eoin_cursor_#{SecureRandom.hex(5)}"

	setup_cursor_tuple_fraction_sql = "SET cursor_tuple_fraction TO 1.0"
	res_1 = conn.exec(setup_cursor_tuple_fraction_sql)
	res_1.clear

	# ON HOLD is what allows us to acccess this named cursor from future queries
	declare_cursor_sql = "DECLARE #{cursor_name} CURSOR WITH HOLD FOR #{query_sql}"
	res_2 = conn.exec(declare_cursor_sql)
	res_2.clear

	loop do
		fetch_sql = "FETCH #{batch_size} FROM #{cursor_name}";

		# allocates C memory for the returned tuples
		res = conn.exec_params(fetch_sql, binds)

		# copies the C memory tuples into Ruby heap (allocates a lot)
		ruby_mem_vals = res.values
		res.clear # clear the C mem now that we don't need it anymore

		puts "fetch_w_cursor: Got #{ruby_mem_vals.length} results"

		# yield the full batch of Array<Array> of values which are now in the Ruby heap.
		# We want to minimize memory allocations so we don't yield individual records which would require creating more new objects
		yield ruby_mem_vals

		# TODO: is this a good test to know when to finish? Is there a better test?
		if ruby_mem_vals.length < batch_size # we got a partial set of results so assume we are done
			break
		end
	end
ensure
	# No matter what happens in the method above we want to attempt to clean up the cursor
	# TODO: what happens if hte cursor doesn't exist?
	close_cursor_sql = "CLOSE #{cursor_name}"
	res_3 = conn.exec(close_cursor_sql)
	res_3.clear
end

def main
	db_name = ENV.fetch("DB_NAME")
	table_name = ENV.fetch("TABLE_NAME")

	conn = PG.connect(dbname: db_name)

	query_sql = "select * from #{table_name}"

	fetch_w_cursor_attempt_1(conn, query_sql, batch_size: 743) do |rows_batch|
		puts "main block: got #{rows_batch.length} results"
		# rows_batch.each do |row|
		#   operate on individual rows - remember that `row` will be a full copy of the row in the array so you will be allocating!
		# end
	end
end

main
```

### Appendix: Rails 'binds' parameter

* Appears in the following method calls:
    ```
    SomeModel.find_by_sql(sql, binds = [], preparable: nil, &block)
    ActiveRecord::Base.connection.exec_query(sql, name = "SQL", binds = [], prepare: false)
    ```
* Is poorly documented, is probably somewhat private API
* Alternatively you could add explicit type casts to your SQL and pass all params in strings

```ruby
# can be array of ActiveRecord::Relation::QueryAttribute instances
binds = [
    ActiveRecord::Relation::QueryAttribute.new(
    nil,                          # name, unsure where this is used
    "August 2019",                # value before type cast
    ActiveRecord::Type::Text.new  # type
    )
]
# WARNING: Some examples online show it being an array of 2-tuple arrays but
#   this always fails for me with Postgres
# binds = [
#     [nil, "30d90923-c51b-4f15-8043-ff1d7eca960b"]
# ]
```

#### Types

* The type determines a set of transformations applied to the raw value

```ruby
ActiveRecord::Type.constants.map { |c| "ActiveRecord::Type::#{c}" }
=> ["ActiveRecord::Type::Date",
 "ActiveRecord::Type::HashLookupTypeMap",
 "ActiveRecord::Type::DateTime",
 "ActiveRecord::Type::Binary",
 "ActiveRecord::Type::Value",
 "ActiveRecord::Type::Integer",
 "ActiveRecord::Type::String",
 "ActiveRecord::Type::Text",
 "ActiveRecord::Type::Json",
 "ActiveRecord::Type::Time",
 "ActiveRecord::Type::AdapterSpecificRegistry",
 "ActiveRecord::Type::DecimalWithoutScale",
 "ActiveRecord::Type::Decimal",
 "ActiveRecord::Type::Internal",
 "ActiveRecord::Type::BigInteger",
 "ActiveRecord::Type::Serialized",
 "ActiveRecord::Type::UnsignedInteger",
 "ActiveRecord::Type::Float",
 "ActiveRecord::Type::Registration",
 "ActiveRecord::Type::Boolean",
 "ActiveRecord::Type::DecorationRegistration",
 "ActiveRecord::Type::TypeMap",
 "ActiveRecord::Type::ImmutableString"]
```

#### Examples in usage

```ruby
[44] pry(main)> User.find_by_sql("select * from users where id = $1", [[nil, "11"]]) # broken
# TypeError: can't cast Array
# from /Users/eoinkelly/.rbenv/versions/3.0.3/lib/ruby/gems/3.0.0/gems/activerecord-7.0.2.2/lib/active_record/connection_adapters/abstract/quoting.rb:43:in `type_cast

# this works but throws an error while logging
User.find_by_sql("select * from users where id = $1", [11])
# Could not log "sql.active_record" event. NoMethodError: undefined method `name' for "1":String

# this works but throws an error while logging
User.find_by_sql("select * from users where id = $1", ["11"])
# Could not log "sql.active_record" event. NoMethodError: undefined method `name' for 1:Integer

# This works but you should really set a name for better logging
[42] pry(main)> User.find_by_sql("select * from users where id = $1", [ActiveRecord::Relation::QueryAttribute.new(nil, "11", ActiveRecord::Type::String.new)])
#   User Load (1.3ms)  select * from users where id = $1  [[nil, "11"]]

# This works and is good choice
[43] pry(main)> User.find_by_sql("select * from users where id = $1", [ActiveRecord::Relation::QueryAttribute.new("eoinx", "11", ActiveRecord::Type::String.new)])
#   User Load (1.2ms)  select * from users where id = $1  [["eoinx", "11"]]
```

    Q: How to know which type to use?
    Q: Can dodge choosign a type by just passing everything as string/text and using type casts in your sql?
    Q: do you need to escape values if you are using them in binds?

Conclusions

* It **seems** like you could just use the string type for everything (but I haven't tested on stuff like JSON)

### Appendix: Sanitizing SQL strings in Rails

If you can't used the Postgres extended query sub-protocol for some reason you will need to properly escape your data values before interpolating them into the SQL string.

There are different helpers for sanitizing SQL depending on which clause of the
SQL query you are inserting your string.  The helpers are documented in

* https://api.rubyonrails.org/classes/ActiveRecord/Sanitization/ClassMethods.html
* https://github.com/rails/rails/blob/main/activerecord/lib/active_record/sanitization.rb

There are also a number of aliased method names. TL;DR I recommend `sanitize_sql_array` because:

1. It is part of Rails itself, not the `pg` gem so should also work with MySQL too.
2. It is the most "honest" method name, other `sanitize_*` methods just call it anyway and I find their argument syntax more confusing

```ruby
# sanitize_sql(condition) # alias for:
sanitize_sql_for_conditions(condition)
    # calls sanitize_sql_array(condition) to do the actual work (if you don't give it an array it just returns what you gave it)

sanitize_sql_array(ary)
    # first element in ary is the SQL statement
    # all other elements are values to be sanitized and interpolated into the SQL statement
    # calls connection.quote_string() to quote the values

ActiveRecord::Base.connection.quote("August '2019")
connection.quote
    # implementation differs depending on which DB adapther you are using
    # inspects the type of the arg and calls the appropriate quoting method e.g. if type is string then it calls quote_string and wraps it in single quotes
connection.quote_string
    # In PG adapter it calls raw_connection.escape(str)
connection.quote_column_name
    # just calls #to_s
```

Examples (from the docs):

```ruby
sanitize_sql_array(["name=? and group_id=?", "foo'bar", 4])
# => "name='foo''bar' and group_id=4"

sanitize_sql_array(["name=:name and group_id=:group_id", name: "foo'bar", group_id: 4])
# => "name='foo''bar' and group_id=4"

sanitize_sql_array(["name='%s' and group_id='%s'", "foo'bar", 4])
# => "name='foo''bar' and group_id='4'"
```

### Appendix: Using PG::Result efficiently

https://deveiate.org/code/pg/PG/Result.html

* PG::Result stores the result tuples it gets from the DB as a single blob of "C" memory i.e. it's not on the Ruby heap.
* When you read a tuple in Ruby, the data is **copied** from the blob of C memory, **converted to a Ruby object** (which inflates its size somewhat) and **saved** to the Ruby heap.
* In a test in [./code/explore_pg_result.rb](./code/explore_pg_result.rb), 17.65 MB of tuples from the DB became 27.25 MB Ruby objects for a total memory allocation of 44.9 MB

Tips

*  Don't fetch data from the DB you won't use because everything we fetch from the DB gets stored **twice**!
*  Don't access data within the PG::Result that you don't need - this will avoid copying it into Ruby memory unnecessarily.
*  Clear the PG::Result as soon as you have got the values you need! This won't prevent allocations but will free up memory on your system.

```ruby
# code/explore_pg_result.rb
require "active_record"
require "pg"
require "memory_profiler"

db_name = ENV.fetch("DB_NAME")
table_name = ENV.fetch("TABLE_NAME")

conn = PG.connect(dbname: db_name)

# res.inspect # => "#<PG::Result:0x0000000102a67040 status=PGRES_TUPLES_OK ntuples=10 nfields=15 cmd_tuples=10>"
# res.clear
# res.inspect # => "#<PG::Result:0x0000000102a67040 cleared>"

# Testing PG::Result#values
# #########################

# Test 1:
########
# Allocates 17.65 MB total
# report = MemoryProfiler.report do
# 	res  = conn.exec(%Q(select * from #{table_name)) # allocates 17.65 MB
# end
# report.pretty_print(scale_bytes: true)

# Test 2:
########
# Allocates 44.9 MB total
# report = MemoryProfiler.report do
# 	res  = conn.exec(%Q(select * from #{table_name})) # allocates 17.65 MB
# 	xx = res.values # allocates 27.25 MB
# end
# report.pretty_print(scale_bytes: true)

# Test 3:
########
# Allocates 70.15 MB total
report = MemoryProfiler.report do
	res  = conn.exec(%Q(select * from #{table_name})) # allocates 17.65 MB
	ar_res = ActiveRecord::Result.new(res.fields, res.values) # allocates 27.25 MB (for the 2nd arg)
	example_ary = ar_res.to_a # allocates  25.01 MB (because it builds a hash from the res.values we passed in
end
report.pretty_print(scale_bytes: true)

# Conclusion: PG::Result#values allocates significant memory, up to 1.5x the memory used by PG::Result to hold the tuples

# Q: is there any way to get the data out of a PG::Result wihtout paying that cost?
# A: I don't think there is. You just have to be aware.```

### Appendix: How much memory copying happens when converting PG::Result into ActiveRecord::Result?

* It gets initialized from a PG::Result
* It doesn't seem to do much memory copying during init
* If you call `#rows` then you are getting the return value of PG::Result#values which doesn't allocate extra memory
* But any method which calls the private #hash_rows will create a Hash from the contents of PG::Result#values
* public methods which call `#hash_rows`:
    ```
    #each
    #to_a
    #to_ary
    #[](idx)
    #last(n)
    ```

Creating `ActiveRecord::Result` from a `PG::Result` doesn't allocate significantly more memory **but** `ActiveRecord::Result` makes it easy to create yet another copy of your data if you are not careful.

```ruby
# worked example for ActiveRecord::Result
require "active_record"
require "pg"
require "memory_profiler"

db_name = ENV.fetch("DB_NAME")
table_name = ENV.fetch("TABLE_NAME")
conn = PG.connect(dbname: db_name)

# Test 3:
########
# Allocates 70.15 MB total
report = MemoryProfiler.report do
	res  = conn.exec(%Q(select * from #{table_name})) # allocates 17.65 MB
	ar_res = ActiveRecord::Result.new(res.fields, res.values) # allocates 27.25 MB (for the 2nd arg)
	example_ary = ar_res.to_a # allocates  25.01 MB (because it builds a hash from the res.values we passed in
end
report.pretty_print(scale_bytes: true)
```

### Appendix: Do I need to use a prepared statement?

Probably not.

A prepared statement lets you send a query to the DB once and then re-run that saved (named) query multiple times with different bound parameters.
This means the DB doesn't have to re-parse the query each time. Most of the time this is not an optimization you need to care about.

prepared statements can be done at the SQL layer or at the protocol layer

> If successfully created, a named prepared-statement object lasts till the end of the current session, unless explicitly destroyed. An unnamed prepared statement lasts only until the next Parse statement specifying the unnamed statement as destination is issued. (Note that a simple Query message also destroys the unnamed statement.) Named prepared statements must be explicitly closed before they can be redefined by another Parse message, but this is not required for the unnamed statement. Named prepared statements can also be created and accessed at the SQL command level, using PREPARE and EXECUTE.

    conn.prepare and then conn.exec_prepared

https://deveiate.org/code/pg/PG/Connection.html#method-i-prepare
https://deveiate.org/code/pg/PG/Connection.html#method-i-exec_prepared

### Appendix: Bulk transfer data to/from the DB with SQL COPY

    TODO: explain what is going on in this example

```ruby
# code/export_csv_example/survey_tag_exporter.rb
class SurveyTagExporter
  SQL_QUERY_TEMPLATE_PATH = Rails.root.join("./query.sql.erb")
  SQL_QUERY_TEMPLATE = ERB.new(File.read(SQL_QUERY_TEMPLATE_PATH))

  def initialize(survey_group)
    @survey_group = survey_group
    @name = survey_group.name.parameterize
  end

  def export
    pg_conn = ActiveRecord::Base.connection.raw_connection # :: PG::Connection
    query = SQL_QUERY_TEMPLATE.result_with_hash(survey_group_id: @survey_group.id)
    csv_output = ""

    log "Start export CSV"

    # PG::Connection#copy_data can take an optional `decoder` argument.  If
    # `decoder` is not set or is `nil`, the data is returned as a binary string
    # with encoding ASCII::8_BIT, even though the underlying data is UTF-8. We
    # supply an instance of `PG::TextDecode::String` which is a very simple
    # decoder which will just change the encoding of the output to whatever
    # `PG::Connection.internal_encoding` is. This is the behaviour we want because
    # `PG::Connection.internal_encoding` is set to `Encoding::UTF_8` by default.
    #
    pg_result = pg_conn.copy_data(query, PG::TextDecoder::String.new) do
      while row = pg_conn.get_copy_data
        csv_output.concat(row)
      end
    end

    log "End export CSV"

    csv_output
  ensure
    # Clear the memory associated with the PG::Result
    log "Cleaning up PG::Result memory"
    pg_result.clear if pg_result && pg_result.respond_to?(:clear)
  end

  def filename
    "#{Time.zone.today.iso8601}-#{@name}-tags.csv"
  end

  private

  def log(msg)
    Rails.logger.info("SurveyTagExporter: #{msg}")
  end
end
```

```sql
# code/export_csv_example/query.sql.erb
COPY ( WITH decorated_survey_tags AS (
    -- Create a "decorated" version of the "survey_tags" table which has some
    -- extra columns which are useful for our exported report.
    --
    -- A surveyTag is associated with a survey either directly (when
    -- taggable_type='survey, taggable_id=123) or indirectly (when
    -- taggable_type='SurveyAnswer, taggable_id=123 then the SurveyAnswer id=123
    -- is associated to the survey). This table resolves those paths into the
    -- `real_survey_id` column.
    --
    --
    --     |      Column           |              Type              |
    --     |-----------------------+--------------------------------+
    --     | id                    | bigint                         |
    --     | tag_id                | bigint                         |
    --     | start_char            | integer                        |
    --     | end_char              | integer                        |
    --     | created_at            | timestamp(6) without time zone |
    --     | updated_at            | timestamp(6) without time zone |
    --     | text                  | character varying              |
    --     | tagger_id             | bigint                         |
    --     | taggable_type         | character varying              |
    --     | taggable_id           | bigint                         |
    --     | confidence_level      | double precision               |
    --     | auto_tag_job_id       | bigint                         |
    --     | survey_question_token | bigint                         | (extra)
    --     | real_survey_id        | bigint                         | (extra)
    --
    SELECT
      survey_tags.*,
      survey_questions.token AS survey_question_token,
      CASE WHEN survey_tags.taggable_type = 'survey' THEN
        survey_tags.taggable_id
      WHEN survey_tags.taggable_type = 'SurveyAnswer' THEN
        survey_answers.survey_id
      END AS real_survey_id
    FROM
      survey_tags
    LEFT OUTER JOIN survey_answers ON survey_tags.taggable_type = 'SurveyAnswer'
    AND survey_tags.taggable_id = survey_answers.id
  LEFT OUTER JOIN survey_questions ON survey_answers.survey_question_id = survey_questions.id),
decorated_surveys AS (
  -- Create a "decorated" version of the "surveys" table which has some
  -- extra columns which are useful for our exported report.
  --
  --     |           Column           |              Type              |
  --     +----------------------------+--------------------------------+
  --     | id                         | bigint                         |
  --     | survey_group_id            | bigint                         |
  --     | created_at                 | timestamp(6) without time zone |
  --     | updated_at                 | timestamp(6) without time zone |
  --     | text                       | text                           |
  --     | description                | text                           |
  --     | state                      | character varying              |
  --     | submitted_at               | timestamp without time zone    |
  --     | channel                    | character varying              |
  --     | source                     | character varying              |
  --     | name                       | character varying              |
  --     | email_address              | character varying              |
  --     | address                    | character varying              |
  --     | phone_number               | character varying              |
  --     | query_type                 | character varying              |
  --     | anonymise                  | character varying              |
  --     | submitter_type             | character varying              |
  --     | exemplar                   | boolean                        |
  --     | file_hash                  | character varying              |
  --     | survey_id                  | bigint                         |
  --     | resolved_filename          | character varying              | (extra)
  --
  SELECT
    surveys.*,
    -- coalesce() is a PostgreSQL function which returns the first arg which is
    -- not null. This allows us to set `resolved_filename` to the
    -- Survey#original_file if it exists and otherwise fallback to
    -- survey#file
    coalesce(asb_survey_file.filename, asb_sub_file.filename) AS resolved_filename
  FROM
    surveys
    -- join surveys --> active_storage_attachements --> active_storage_blobs,
    -- filtering to get only attachments attached via `survey#file`
    LEFT OUTER JOIN active_storage_attachments AS asa_sub_file ON asa_sub_file.record_type = 'survey'
      AND asa_sub_file.name = 'file'
      AND asa_sub_file.record_id = surveys.id
    LEFT OUTER JOIN active_storage_blobs AS asb_sub_file ON asa_sub_file.blob_id = asb_sub_file.id
  -- join surveys -- surveys --> active_storage_attachements --> -- active_storage_blobs
  -- filtering to get only attachments attached to -- `Survey#original_file`
    LEFT OUTER JOIN surveys ON surveys.survey_id = surveys.id
    LEFT OUTER JOIN active_storage_attachments AS asa_survey_file ON asa_survey_file.record_type = 'Survey'
      AND asa_survey_file.name = 'original_file'
      AND asa_survey_file.record_id = surveys.id
    LEFT OUTER JOIN active_storage_blobs AS asb_survey_file ON asa_survey_file.blob_id = asb_survey_file.id)
-- This is our "main" query which uses the temporary tables defined above in the
-- `WITH ...` clause. The goal of this query is to return results which can be
-- **directly** added to the CSV without any further processing.
SELECT
  decorated_survey_tags.real_survey_id AS survey_id,
  decorated_survey_tags.id AS tag_id,
  coalesce(decorated_surveys.resolved_filename, '') AS survey_filename,
  coalesce(decorated_survey_tags.survey_question_token, '') AS survey_question_token,
  coalesce(tags.name, '') AS tag_name,
  coalesce(tags.full_number, '') AS tag_number,
  coalesce(decorated_survey_tags.text, '') AS "quote",
  decorated_survey_tags.start_char AS start_char,
  decorated_survey_tags.end_char AS end_char,
  CASE WHEN users.email IS NOT NULL THEN
    users.email
  WHEN decorated_survey_tags.auto_tag_job_id IS NOT NULL THEN
    format('ML-%s', decorated_survey_tags.auto_tag_job_id)
  ELSE
    ''
  END AS tagger,
  to_char(decorated_survey_tags.created_at, 'YYYY-MM-DD" "HH24:MI:SS "UTC"') AS tagtime
FROM
  decorated_survey_tags
  LEFT OUTER JOIN decorated_surveys ON decorated_survey_tags.real_survey_id = decorated_surveys.id
  LEFT OUTER JOIN tags ON decorated_survey_tags.tag_id = tags.id
  LEFT OUTER JOIN users ON decorated_survey_tags.tagger_id = users.id
WHERE
  decorated_surveys.survey_group_id = <%= survey_group_id %>
  AND decorated_surveys.state != 'archived'
ORDER BY
  decorated_survey_tags.id)
  TO stdout WITH (
    format csv,
    header TRUE,
    ENCODING 'UTF8',
    force_quote *);

```

### Appendix: Rails and EXPLAIN

> **Warning**
> You may see mention of `config.active_record.auto_explain_threshold_in_seconds` online but this feature was removed in Rails 4. [See the commit](https://github.com/rails/rails/commit/d3688e02ca52c0b72d3092e8498da51e06b7fc58)

You can run explain on queries generated by ActiveRecord.
See the [section in the ActiveRecord guide](https://guides.rubyonrails.org/active_record_querying.html#running-explain) for details.

> **Warning**
> Eager loading may trigger more than one query under the hood, and some queries may need the results of previous ones. Because of that, ActiveRecord `.explain` **actually executes the query**, and then asks for the query plans

```ruby
User.where(:id => 1).joins(:posts).explain # WARNING: this will actually run your query
```

### Appendix: Using EXPLAIN well

* `EXPLAIN` is not part of the SQL standard so the output is completely different between database engines.

* `EXPLAIN` without `ANALYZE` will not actually run your query. It will generate the plan that the database would use to run the query and return that plan to you as plain text so you can see the choices the DB will make.
* `EXPLAIN ANALYZE ...` will generate the plan and then **run the query**. Instead of returning the results it will return a plan similar to `EXPLAIN` output but with more information about the actual query run
* If your query would change the DB you should always run EXPLAIN ANALYZE within a transaction that you immediately roll back e.g
    ```sql
    BEGIN;
    EXPLAIN ANALYZE ...;
    ROLLBACK;
    ```

Options you should use


VERBOSE
BUFFERS

FORMAT TEXT (the default)
FORMAT JSON
FORMAT YAML
FORMAT XML

```sql
EXPLAIN (ANALYZE ON, BUFFERS ON) ...;
```

Interpreting the output

* It isn't intuitive. Practice is required.
* Build up your knowledge by running EXPLAIN on simple queries first
* https://www.pgmustard.com/ can help
