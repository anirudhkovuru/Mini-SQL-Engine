# Mini SQL Engine
A mini​ sql engine which runs a subset of SQL queries using ​a command line interface.

### Running a query
> python3 query.py "select * from file1;"

## Data set
1. csv files for tables. 
    1. If a file is : File1.csv , the table name is File1
2. All the elements in the files are INTEGERS
3. A file named: metadata.txt (note the extension) is which will have the 
following structure for each table: \
<begin_table> \
<table_name> \
attribute1 \
.... \
attributeN \
<end_table>

## Types of Queries
1. Select all records : \
***select * from table_name;***

2. Aggregate functions: Simple aggregate functions on a single column.
Sum, average, max and min: \
***select max(col1) from table1;***

3. Project Columns(could be any number of columns) from one or more tables : \
 ***select col1, col2 from table_name;***
 
4. Select/project with distinct from one table: \
***select distinct col1, col2 from table_name;***

5. Select with where from one or more tables : \
***select col1,col2 from table1,table2 where col1 = 10 AND col2 = 20;***
    1. In the where queries, there can be a maximum of one AND/OR operator 
    with no NOT operators
    2. Relational operators: "< , >, <=, >=, ="
    
6. Projection of one or more(including all the columns) from two tables with one join
condition : 
    1. ***select * from table1, table2 where table1.col1=table2.col2;***
    2. ***select col1, col2 from table1,table2 where table1.col1=table2.col2;***
    
## Output
The output will be in the following form: \
<Table1.column1,Table1.column2....TableN.columnM> \
Row1 \
Row2 \
....... \
RowN