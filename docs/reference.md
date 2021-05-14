# DataFrame API Reference

**Table of contents**
<!--- TOC -->

* [Create columns](#create-columns)
    * [Unnamed column with values](#unnamed-column-with-values)
    * [Named column with values](#named-column-with-values)
    * [Named column without values](#named-column-without-values)
* [Create `DataFrame`](#create-dataframe)
    * [from values](#from-values)
    * [from columns](#from-columns)
    * [from map](#from-map)
    * [from objects](#from-objects)
* [Read `DataFrame`](#read-dataframe)
    * [from CSV](#read-csv)
    * [from JSON](#read-json)
* [`DataFrame` info](#dataframe-info)
    * [schema](#schema)
    * [describe](#describe)
* [Access data](#access-data)
    * [by column](#by-column)
    * [by row](#by-row)
    * [as Iterable](#as-iterable)
* [Compute statistics](#compute-statistics)
    * [count](#count)    
    * [min/max](#min--max)
    * [minBy/maxBy](#minBy--maxBy)
    * [sum](#sum)
* [Modify cells](#modify-cells)
    * [update](#update)
    * [fillNulls](#fillNulls)
    * [nullToZero](#nullToZero)    
* [Modify rows](#modify-rows)
    * [filter](#filter)
    * [sortBy](#sortBy)
    * [distinct](#distinct)
    * [mergeRows](#mergeRows)
    * [explode](#explode)
    * [append](#append)
    * [groupBy](#groupBy)
    * [shuffled](#shuffled)
    * [take/takeLast](#take--takelast)
    * [drop/dropLast](#drop--droplast)
* [Modify columns](#modify-columns)
    * [select](#select)
    * [add](#add)
    * [remove](#remove)
    * [cast](#cast)
    * [split](#split)
    * [merge](#merge)
    * [rename](#rename)
    * [replace](#replace)
    * [move](#move)    
    * [group](#group)
    * [ungroup](#ungroup)
    * [flatten](#flatten)
    * [gather](#gather)
    * [spread](#spread)
* [Merge `DataFrame`s](#merge-dataframes)
    * [add](#add-columns)
    * [union](#union)
    * [join](#join)
* [Column operations](#column-operations)
    * [distinct](#distinct)
    * [guessType](#guessType)
    * [rename](#rename-1)
    * [digitize](#digitize)
    * [Arithmetic operations](#arithmetic-operations)
    * [Column Statistics](#column-statistics)
* [Working with `GroupedDataFrame`](#working-with-groupeddataframe)
    * [aggregate](#aggregate)
    * [spread](#spread-inside-aggregate)
    * [countBy](#countBy)
* [Export `DataFrame`](#export-dataframe)
    * [writeCSV](#writecsv)
    * [writeClass](#writeclass)
    * [toMap](#toMap)
* [`DataFrame` Presentation](#dataframe-presentation)
    * [format](#format)
* [Column kinds](#column-kinds)
* [Column selectors](#column-selectors)
* [Row expressions](#row-expressions)
* [Row properties](#row-properties)


<!--- END -->

## Create columns

### Unnamed column with values
```kotlin
val col = columnOf("Alice", "Bob")
val col = listOf("Alice", "Bob").toColumn()
```
### Named column with values
```kotlin
val name by columnOf("Alice", "Bob")
val col = listOf("Alice", "Bob").toColumn("name")
```
To rename column use function `rename` or infix function `named`:
```kotlin
val unnamedCol = columnOf("Alice", "Bob")
val col = unnamedCol.rename("name")
val col = columnOf("Alice", "Bob") named "name"
```
### Named column without values
```kotlin
val name by column<String>()
val col = column<String>("name")
```
Named column without values is called `ColumnReference` and can be used in `DataFrame` operations for typed access to columns:
```kotlin
df.filter { it[name].startsWith("A") }
df.sortBy { col }
```
`ColumnReference` can be converted to `DataColumn` by adding values:
```kotlin
val col = name.withValues("Alice", "Bob")
```
or for `Iterable` of values:
```kotlin
val values = listOf("Alice", "Bob")
val col = name.withValues(values)
val col = values.toColumn(name)
```
## Create `DataFrame`

Several ways to convert data into `DataFrame`
### from values
```kotlin
val df = dataFrameOf("name", "age")(
   "Alice", 15,
   "Bob", 20
)
```

### from columns
`DataFrame` can be created from one or several [columns](#create-columns)

```kotlin
val name by columnOf("Alice", "Bob")
val age by columnOf(15, 20)

val df1 = dataFrameOf(name, age)
val df2 = listOf(name, age).toDataFrame()
val df3 = name + age
```
### from map
`Map<String, Iterable<Any?>>` can be converted to `DataFrame`:
```kotlin
val data = mapOf("name" to listOf("Alice", "Bob"), "age" to listOf(15, 20))
val df = data.toDataFrame()
```
### from objects

DataFrame can be created from a list of any objects.
Assume we have a list of `Person` objects:
```kotlin
data class Person(val name: String, val age: Int)
val persons = listOf(Person("Alice", 15), Person("Bob", 20))
```
This list can be converted to `DataFrame` with columns for every public property of `Person` class:
```kotlin
persons.toDataFrame()
```

name | age
---|---
Alice | 15
Bob | 20

You can also specify custom expressions for every column:
```kotlin
val df = persons.toDataFrame {
   "name" { name }
   "year of birth" { 2021 - age }
}
```

name | year of birth
---|---
Alice | 2006
Bob | 2001

## Read `DataFrame`

DataFrame supports CSV and JSON input formats.
Use `read` method to guess input format based on file extension and content
```kotlin
DataFrame.read("input.csv")
```
Input string can be a file path or URL.

### Read CSV
```kotlin
DataFrame.readCSV("input.csv")
```
### Read JSON
```kotlin
DataFrame.readJSON("https://covid.ourworldindata.org/data/owid-covid-data.json")
```

## Access Data
### by column
```kotlin
df["name"][0]
```
### by row
```kotlin
df[0]["name"]
```
### as iterable
`DataFrame` can be interpreted as an `Iterable` of `DataRow`. Although `DataFrame` doesn't implement `Iterable` interface, it defines most extension functions available for `Iterable`
```kotlin
df.forEach { println(it) }
df.take(5)
df.drop(2)
df.chunked(10)
```
For compatibility with stdlib, `DataFrame` can be converted to `Iterable`
```kotlin
df.asIterable()
```
or to `Sequence`
```kotlin
df.asSequence()
```
# `DataFrame` info
## describe
Generates descriptive statistics
# Compute statistics
## nrow
Returns number of rows in `DataFrame`
```kotlin
df.nrow()
```
## ncol
Returns number of columns in `DataFrame`
```kotlin
df.ncol()
```
## sum
Computes sum of expressions evaluated for every `DataRow` of `DataFrame`
```kotlin
df.sum { rowExpression }
```
See [Row Expressions](#row-expressions) for details
# Modify cells
Note that all update operations return a new instance of `DataFrame`
## update
Changes values in some cells
```
df.update { columns }
   [.where { filter } | .at(rowIndices) | .at(rowRange) ] // filter cells to be updated 
    .with { valueExpression } | .withNull() | .notNull { valueExpression }

filter = DataRow.(OldValue) -> Boolean
valueExpression = DataRow.(OldValue) -> NewValue
```
Examples
```kotlin
df.update { price }.with { it * 2 }
df.update { age }.where { name == "Alice" }.with { 20 }
df.update { column }.at(4,6,10).with { "value" } 
df.update { column }.at(5..15).withNull() 
df.update { price }.with { (it + (prev?.price ?: it) + (next?.price ?: it)) / 3 } // moving average
df.update { cases }.with { it.toDouble() / population * 100 }
```
## fillNulls
Replaces `null` values with expression. Equivalent to
```
update { columns }.where { it == null }
```
Example
```kotlin
df.fillNulls { intCols() }.with { -1 } 
```
## nullToZero
Replace `null` values with `0`. Works for `Int`, `Double`, `Long` and `BigDecimal` columns.
```kotlin
df.nullToZero { columns }
```
## Modify rows
`DataFrame` is immutable, so all modification operations return a new instance
### filter
Filter rows by row predicate

String API:
```kotlin
df.filter { "age"<Int>() > 10 && "name"<String>().startsWith("A") }
```
Column accessors API:
```kotlin
val age by column<Int>()
val name by column<String>()
df.filter { age() > 10 && name().startsWith("A") }
```
Extension properties API:
```kotlin
df.filter { age > 10 && name.startsWith("A") }
```
More examples:
```kotlin
df.filter { index % 2 == 0} // keep even rows
df.filter { age != prev?.age }
```
### sortBy
Sorts `DataFrame` by one or several columns. 
Several sort columns can be combined by `and` operator. 
By default, columns are sorted in ascending order with null values going first. 
To change column sort order to descending use `.desc` modifier.
To get `null` values in the end of the order use `.nullsLast` modifier

String API:
```kotlin
df.sortBy("age", "name")
```
Column accessors API:
```kotlin
val age by column<Int>()
val name by column<String>()
df.sortBy { age and name.desc }
df.sortBy { name.nullsLast and age.desc }
```
Extension properties API:
```kotlin
df.sortBy { age }
df.sortBy { age and name.desc }
df.sortBy { name.nullsLast and age.desc }
```
To apply descending order to all columns use `sortByDesc` function
```kotlin
df.sortByDesc { name and age}
```
To sort by a continuous range of columns use `cols` function
```kotlin
df.sortBy { cols(0..2) }
```
### distinct
Removes duplicate rows
```kotlin
df.distinct()
```
### mergeRows
Merges values in selected columns into lists grouped by other columns

Input:

|name   | city    | age |
|-------|---------|-----|
| Alice | London  | 15  |
| Bob   | Milan   | 20  |
| Alice | Moscow  | 23  |
| Alice | London  | 30  |
| Bob   | Milan   | 11  |

```kotlin
df.mergeRows { age }
```
Output:

|name   | city   | age 
|-------|--------|-----
| Alice | London | [15, 30]
| Bob   | Milan  | [20, 11]
| Alice | Moscow | [23]
### explode
Splits list-like values and spreads them vertically. Reverse to [mergeRows](#mergerows)
The following types of values will be splitted:
* List 
* DataFrame
* String (splits by ',')
Scalar values will not be transformed. Empty lists will result in `null`
Row values in other columns will be duplicated

Input:

| A | B 
|---|---
| 1 | [1, 2, 3]
| 2 | null 
| 3 | [4, 5] 
| 4 | [] 
```kotlin
df.explode { B }
```
Output:

| A | B 
|---|---
| 1 | 1 
| 1 | 2 
| 1 | 3 
| 2 | null 
| 3 | 4 
| 3 | 5 
| 4 | null 

Note: exploded `FrameColumn` turns into `MapColumn`

When several columns are exploded, lists in different columns are aligned:  

Input:

| A | B | C
|---|---|---
| 1 | [1, 2] | [1, 2]
| 2 | [] | [3, 4]
| 3 | [3, 4, 5] | [5, 6] 
```kotlin
df.explode { B and C }
```
Output:

| A | B | C
|---|---|---
| 1 | 1 | 1
| 1 | 2 | 2
| 2 | null | 3 
| 2 | null | 4
| 3 | 3 | 5
| 3 | 4 | 6
| 3 | 5 | null

### append
Adds one or several rows to `DataFrame`
```kotlin
df.append (
    "Mike", 15,
    "John", 17, 
    "Bill", 30)
```
### groupBy
Groups rows in `DataFrame` by one or several key columns. Returns `GroupedDataFrame` that supports various aggregation operations
```kotlin
df.groupBy { name and city }.aggregate {
    nrow() into "num"
    count { age > 18 } into "number of adults"
    median { age } into "median age"
}
df.groupBy { name }.aggregate {
    maxBy { age }.city into "city of oldest person"
    countBy { city } into { "from $it" }
}
```
See [Working with `GroupedDataFrame`](#working-with-groupeddataframe) for details
For `String` or `ColumnAccessor` API, `aggregate` can be omitted:
```kotlin
val name by column<String>()
val city by column<String>()
val alive by column<Boolean>()

df.groupBy(name, city) {
    nrow() into "total"
    count { alive() } into "alive"
}
```

### shuffled
Reorders rows randomly
```kotlin
df.shuffled()
```
### take / takeLast
Returns `DataFrame` containing first/last `n` rows
```kotlin
df.take(10) // first 10 rows
df.takeLast(20) // last 20 rows
```
### drop / dropLast
Returns `DataFrame` containing all rows except first/last `n` rows
```kotlin
df.drop(10)
df.dropLast(20)
```
## Modify columns
Note that `DataFrame` object is immutable, so all modification operations return a new instance of `DataFrame`
### add
Adds new column to `DataFrame`
```kotlin
add(columnName) { rowExpression }
```
See [row expressions](#row-expressions)
```kotlin
df.add("year of birth") { 2021 - age }
df.add("diff") { temperature - (prev?.temperature ?: 0) }
```
Add several columns:
```kotlin
df.add {
   "is adult" { age > 18 }
   "name length" { name.length } 
}
```
or with `+` operator
```kotlin
df + {
   "is adult" { age > 18 }
   "name length" { name.length } 
}
```
### remove
Removes columns from `DataFrame`
```kotlin
df.remove { columns }
df - { columns }
```
See [Column Selectors](#column-selectors) for column selection syntax
### cast
Changes the type of columns. Supports automatic type conversions between value types `Int`, `String`, `Double`, `Long`, `Short`, `Float`,`BigDecimal`
```kotlin
df.cast { age }.to<Double>()
```
Helper functions for value types are also available
```kotlin
df.cast { age }.toFloat()
df.cast { all() }.toStr()
``` 
### split
Splits cell value into several values and spreads them horizontally or vertically.

   Default split behavior:
* for `String` values: split by `,` and trim (leading and trailing whitespace removed)
* for `List` values: split into list elements

#### Split horizontally
Reverse operation to [merge](#merge)
```
df.split { columns }
    [.by(delimeters) | .by { splitter }] // how to split cell value
    [.inward()] // nest resulting columns into original column
    .into(columnNames) [ { columnNamesGenerator } ]

splitter = (T) -> List<Any>
columnNamesGenerator = DataColumn.(columnIndex: Int) -> String
```
`columnNamesGenerator` is used to generate names for additional columns when the list of explicitly specified `columnNames` was not long enough.  
`columnIndex` in `columnNamesGenerator` starts with `1` for the first additional column name.  
Default `columnNamesGenerator` generates column names `splitted1`, `splitted2`...

Examples:
```kotlin
// artifactId -> groupId, artifactId, version 
df.split { artifactId }.by(":").into("groupId", "artifactId", "version")

// info: List<String> -> info.age, info.weight
df.split { info }.inward().into("age", "weight")

// address -> address1, address2, address3, address4 ...
df.split { address }.into { "address$it" } // splits by ','

// text -> firstWord, secondWord, extraWord1, extraWord2 ...
df.split { text }.by(" ").into("firstWord", "secondWord") { "extraWord$it" }
```

Split `Int` column into digits using `splitter`:
```kotlin
fun digits(num: Int) = sequence {
        var k = num
        if(k == 0) yield(0)
        while(k > 0) {
            yield(k % 10)
            k /= 10
        }
    }.toList()

// number -> number.digit1, number.digit2, number.digit3...
df.split { number }.by { digits(it) }.inward().into { "digit$it" }
```
#### Split vertically
Reverse operation to [mergeRows](#mergeRows). See [explode](#explode) for details
```
df.split { columns }.intoRows()
df.split { columns }.by(delimeters).intoRows()
df.split { columns }.by { splitter }.intoRows()

splitter = (T) -> List<Any>
```
When neither `delimeters` or `splitter` are specified, `split().intoRows()` is equivalent of [explode](#explode)
### merge
Merges several columns into a single column. Reverse operation to [split](#split)
```
df.merge { columns }.into(columnPath)
df.merge { columns }.by(delimeters, options).into(columnPath)
df.merge { columns }.by { merger }.into(columnPath)

merger = List<T> -> Any
```
When no `delimeter` or `merger` are defined, values will be merged into the `List`
```kotlin
df.merge { firstName and lastName }.by(" ").into("fullName")

df.merge { cols { it.name.startsWith("value") } }.into("values")

df.merge { protocol and host and port and path }.by { it[0] + "://" + it[1] + ":" + it[2] + "/" + it[3] }.into("address")
```
### rename
Renames one or several columns without changing its location in `DataFrame`
```
df.rename { columns }.into(name)
df.rename { columns }.into { nameExpression }

nameExpression = (DataColumn) -> String
```
### replace
Replaces one or several columns with new columns
```
df.replace { columns }.with(newColumns)
df.replace { columns }.with { columnExpression }

columnExpression = DataFrame.(DataColumn) -> DataColumn
```
Examples
```kotlin
df.replace { col1 and col2 }.with(newCol1, newCol2)
df.replace { stringCols() }.with { it.lower() }
df.replace { oldColumn }.with(newColumn.rename("newName"))
df.replace { age }.with { 2021 - age named "year" } // another syntax for renaming columns within replace expression
```
### move
Moves one or several columns within `DataFrame`.
```kotlin
df.move { column }.into(columnPath)
df.move { columns }.into { columnPathExpression }
df.move { columns }.under(parentPath)
df.move { columns }.under { parentPathExpression }
df.move { columns }.toTop { columnNameExpression }
df.move { columns }.to(position)
df.move { columns }.toLeft()
df.move { columns }.toRight()
df.move { columns }.after { column }
```
See [Column Selectors](#column-selectors) for column selection syntax.

Columns in `DataFrame` can be ordered hierarchically and form a tree structure. Therefore column can be addressed by `ColumnPath` that represents a list of column names.

`move` operation allows to change hierarchical order of columns in `DataFrame` by providing a new `ColumnPath` for every column

```kotlin
// name, age, weight -> age, name, weight
df.move { age }.toLeft()

// name, age, weight -> name, weight, age
df.move { weight }.to(1)

// name -> info.name
df.move { name }.into("info", "name")

// name -> info.name
df.move { name }.into { info + "name" } // 'info' column should already exist

// firstName -> fullName.firstName
// lastName -> fullName.lastName
df.move { firstName and lastName }.under("fullName")

// firstName -> fullName.first
// lastName -> fullName.last
df.move { firstName and lastName }.into { path("fullName", it.name.dropLast(4)) }

// a:b:c -> a.b.c
df.move { all() }.into { it.name.split(":") }

// totalCases -> total.cases
// totalRecovered -> total.recovered
df.move { cols { it.name.startsWith("total") } }.into { path("total", it.name.substring(5).decapitalize()) }

// some.path.data1 -> new.column.path.data1
// another.path.data2 -> new.column.path.data2
df.move { colsDfs { it.parent.name == "path" } }.under { new.column.path } // new.column.path should aready exists

// info.default.data -> default
// some.field.data -> field
df.move { colsDfs { it.name == "data" } }.toTop { it.parent.name }

// a.b -> b.a
// a.b.c -> a.b.c
df.move { colsDfs { it.path.length == 2 } }.into { it.path.reverse() }
```
### group
Group columns into column groups. It is a special case of [move](#move) operation
```
df.group { columns }.into(groupName)
df.group { columns }.into { groupNameExpression }

groupNameExpression = DataColumn.(DataColumn) -> String
```
Examples
```kotlin
df.group { firstName and lastName }.into("name")
df.group { nameContains(":") }.into { name.substringBefore(":") }
```
### ungroup
Replaces `MapColumn` with its nested columns. Reverse operation to [group](#group)
```kotlin
// fullName.firstName -> firstName
// fullName.lastName -> lastName
df.ungroup { fullName }
``` 
### flatten
Removes all column grouping under selected columns. Potential column name clashes are resolved by adding minimal required prefix from ancestor column names  
```
df.flatten()
df.flatten { rootColumns }
```
Example
```kotlin
// a.b.c.d -> "d"
// a.f -> "f"
// a.c.d.e -> "d.e"
// a.b.e -> "b.e"
df.flatten { a }
```
### gather
Converts several columns into two `key-value` columns, where `key` is a name of original column and `value` is column data
This is reverse to [spread](#spread)
```
df.gather { columns }.into(keyColumnName)

df.gather { columns }.where { valueFilter }.map { valueTransform }.mapNames { keyTransform }.into(keyColumnName, valueColumnName)
```
**Input**

| city | Feb, 18 | Feb, 19 | Feb, 20 | Feb, 21 
|--------|---------|---------|--------|---
| London | 3 | 5 | 4 | null
| Milan  | 7 | null | 3 | 5
```kotlin
df.gather { cols(1..4) }.where { it != null}.mapNames { it.substring(5) }.into("day", "temperature")
```
**Output**

| city | day | temperature 
|--------|---------|---------
| London | 18 | 3
| London | 19 | 5
| London | 20 | 4
| Milan | 18 | 7
| Milan | 20 | 3
| Milan | 21 | 5

When `valueColumnName` is not defined, only 'key' column is added. In this case `valueFilter` will default to `{ it }` for `Boolean` columns and `{ it != null }` for other columns

**Input**

name | London | Paris | Milan
-----|--------|-------|-------
Alice| true | false | true
Bob | false | true | true
```kotlin
df.gather { cols(1..4) }.into("visited")
```
**Output**

name | visited
-----|--------
Alice | London
Alice | Milan
Bob | Paris
Bob | Milan

### spread
Converts two key-value columns into several columns using values in `key` column as new column names and values in `value` column as new column values.
This is reverse to [gather](#gather) 
```
df.spread { keyColumn }.by { valueColumn }.into { keyTransform }
```
**Input**

| city | day | temperature 
|--------|---------|---------
| London | 18 | 3
| London | 19 | 5
| London | 20 | 4
| Milan | 18 | 7
| Milan | 20 | 3
| Milan | 21 | 5

```kotlin
df.spread { day }.by { temperature }.into { " Feb, $it" }
```
**Output**

| city | Feb, 18 | Feb, 19 | Feb, 20 | Feb, 21 
|--------|---------|---------|--------|---
| London | 3 | 5 | 4 | null
| Milan  | 7 | null | 3 | 5
# Merge dataframes
## Add columns
Adds columns from another dataframe. New columns must have the same length as original columns
```
df.add(otherDf)
df.add(otherDf.columns())
df + otherDf.columns()
```
## union
Adds rows from another dataframe. Columns from both dataframes are unioned, values in missing columns are replaced with `null`
```
df.union(otherDf)
df + otherDf
```
**Input**

name | age
---|---
Alice | 15
Bob | 20

name | weight
---|---
Mark |60
Bob |70

```kotlin
df1 + df2
```
**Output**

name|age|weight
---|---|---
Alice | 15 | null
Bob | 20 | null
Mark | null | 60
Bob | null |70

## join
SQL-like joins. Matches rows from two dataframes by key columns and creates cross-product of other columns
```
df.innerJoin(otherDf) { columnMatches }
df.leftJoin(otherDf) { columnMatches }
df.rightJoin(otherDf) { columnMatches }
df.outerJoin(otherDf) { columnMatches }
df.filterJoin(otherDf) { columnMatches }
df.excludeJoin(otherDf) { columnMatches }

df.join(otherDf) { columnMatches } // same as innerJoin
```
To match columns with different names use `match` operation and `right` property to reference second `DataFrame`:
```kotlin
val df1 = dataFrameOf("name", "origin")("Alice", "London", "Bob", "Milan")
val df2 = dataFrameOf("city", "country")("London", "UK", "Milan", "Italy") 
                    
df1.join(df2) { origin.match(right.city) }
df1.join(df2) { origin match right.city } // infix form
```
To match columns with equal names just use column from the first `DataFrame`
```kotlin
df1.join(df2) { city }
df1.join(df2) { firstName and lastName }
```
If `columnMatches` is ommited, all columns with matching names from both dataframes will be used

```
df1
```

name | age
---|---
Alice | 15
Bob | 20

```
df2
```

name | weight
---|---
Mark |60
Bob |70

```kotlin
df1.join(df2)
```

name|age|weight
---|---|---
Bob | 20 | 70

```kotlin
df1.leftJoin(df2)
```

name|age|weight
---|---|---
Alice | 15 | null
Bob | 20 | 70

```kotlin
df1.rightJoin(df2)
```

name|age|weight
---|---|---
Bob | 20 | 70
Mark | null | 60

```kotlin
df1.outerJoin(df2)
```

name|age|weight
---|---|---
Alice | 15 | null
Bob | 20 | 70
Mark | null | 60

```kotlin
df1.filterJoin(df2)
```

name|age
---|---
Bob | 20 

```kotlin
df1.excludeJoin(df2)
```

name|age
---|---
Alice | 15 

## Column operations
Operations for a single `DataColumn` object
### rename
Returns `DataColumn` with a new name
```kotlin
column.rename("newName")
```
### distinct
Return `DataColumn` with unique values
```
column.distinct()
``` 
### guessType
Changes type of `DataColumn` based on its actual values
```kotlin
val mixed by columnOf("Alice", 3, true)
mixed.type() // Any
```
```kotlin
val filtered = mixed.filter { it is String}
filtered.type() // Any
```
```kotlin
val guessed = filtered.guessType()
guessed.type() // String
```
### digitize
Return `DataColumn` with indices of the bins to which each value in original `DataColumn` belongs
```
column.digitize(bins, right = false)
column.digitize(bin0, bin1, bin2)
column.digitize(bin0, bin1, bin2, right = true)
```
`right` is an optional parameter indicating whether to include right edge of the bin into interval instead of left edge. By default, `right = false`

For value `x` an index `i` is returned when
`bins[i-1] <= x < bins[i]` if `right = false`
`bins[i-1] < x <= bins[i]` if `right = true`
If `x` is below all bins, 0 is returned. If `x` is above all bins, `bins.length` is returned
### Arithmetic operations
For `Int`, `Double`, `Long` and `BigDecimal` columns `+`, `-`, `*`, `/` operations with scalar values are supported
### Column statistics

Statistics for `Number` columns:
```kotlin
column.sum()
column.mean()
column.median()
```

Statistics for `Comparable` columns:
```kotlin
column.max()
column.maxBy { expression }
column.min()
column.maxBy { expression }
``` 
### Working with `GroupedDataFrame`
`GroupedDataFrame` is any `DataFrame` with one selected [`FrameColumn`](#framecolumn) that is interpreted as data groups
So any `DataFrame` with `FrameColumn` can be converted to `GroupedDataFrame`:
```kotlin
val files by column("input1.csv", "input2.csv", "input3.csv") // create column of file names 
val data by files.map { DataFrame.read(it) } // create FrameColumn of dataframes
val df = DataFrame.of(files, data) // create DataFrame with two columns 'files' and 'data'
val groupedDf = df.asGrouped { data } // interpret 'data' column as groups of GroupedDataFrame
```

[Union](#union) operation all groups of `GroupedDataFrame` into single `DataFrame`. All other columns of `GroupedDataFrame` are ignored.
```kotlin
groupedDf.union()
```
`union` operation at `FrameColumn` will produce the same result:
```kotlin
groupedDf[data].union()
```
### aggregate
`GroupedDataFrame` can be aggregated into `DataFrame` with one or several [statistics](#compute-statistics) computed for every data group.
```
groupedDf.aggregate { 
    stat1 into "column1"
    stat2 into "column2"
    ...
}
```
Every data group is passed to the body of `aggregate` function as a receiver of type `DataFrame`
```kotlin
groupedDf.aggregate {
    nrow() into "total"
    count { age > 18 } into "adults"
    median { age } into "median age"
    min { age } into "min age"
    maxBy { age }.name into "oldest"
}
```
If only one simple statistics is used, `aggregate` can be omitted:
```kotlin
groupedDf.max { age } // max age for every group into column "age"
groupedDf.mean { weight } // mean weight in every group into column "weight"
groupedDf.count() // number of rows in every group into column "n"
```
`aggregate` can also be applied to any `DataFrame` with [FrameColumn](#framecolumn)
```
df.aggregate { groups }.with {
    stat1 into "column1"
    stat2 into "column2"
    ...
}
```
### `spread` inside `aggregate`
[spread](#spread) operation can also be used within [aggregate](#aggregate) with a slightly different syntax

**Input**

name|city|date
---|---|---
Alice|London|2020-10-01
Bob|Paris|2020-10-02
Alice|Paris|2020-10-03
Alice|London|2020-10-04
Bob|Milan|2020-10-05

```kotlin
df.groupBy { name }.aggregate {
    spread { city }.with { max { date } } into { "$it last visit" }
}
```
or
```kotlin
df.groupBy { name }.spread { city }.with { max { date } }.into { "$it visits" }
```
**Output**

name|London last visit|Paris last visit|Milan last visit
---|---|---|---
Alice|2020-10-04|2020-10-03|null
Bob|null|2020-10-02|2020-10-05
### countBy
[Spreads](#spread) column values into new columns and computes number of rows
Equivalent of `spread { column }.with { nrow () }`

**Input**

name|city|date
---|---|---
Alice|London|2020-10-01
Bob|Paris|2020-10-02
Alice|Paris|2020-10-03
Alice|London|2020-10-04
Bob|Milan|2020-10-05

```kotlin
df.groupBy { name }.aggregate {
    countBy { city } into { it }
}
```
or
```kotlin
df.groupBy { name }.countBy { city }
```

**Output**

name|London|Paris|Milan
---|---|---|---
Alice|2|1|0
Bob|0|1|1
## `DataFrame` Presentation
### format
Applies conditional formatting to cells. Returns `FormattedFrame` that can be rendered in Jupyter notebook
```
df.format { columns }
    [.where { rowFilter } ]
    .with { formatter }

formatter = FormattingDSL.(Value) -> CellFormat
CellFormat = bold | italic | underline | background(color) | textColor(color) | format1 and format2 | linearBg(value, v1 to color1, v2 to color2)
color = rgb(r,g,b) | linear(value, v1 to color1, v2 to color2) | white | gray | red | green | ...
```
If `columns` are not specified, formatting is applied to all numeric columns without `null` values
```kotlin
df.format().with { if(it >= 0) background(green) else background(red) }
```
Several formatters can be chained:
```kotlin
df.format { all() }.where { index % 0 == 2 }.with { background(lightGray) }
  .format { age }.where { it > 20 }.with { bold and underline }
  .format { score }.with { linearBg(it, 0 to red, 100 to green) }
```

## Export `DataFrame`
### writeCSV
Exports `DataFrame` to `CSV` file
```kotlin
df.writeCSV("output.csv")
```
### writeClass
Exports `DataFrame` to `List` of auto-generated data classes. Only for `Jupyter` environment.
```kotlin
val list = df.writeClass("Person")
```
### toMap
Converts `DataFrame` to `Map<String, List<Any?>>`
```kotlin
df.toMap()
```
## Column kinds
There are three kinds of `DataColumn`:
* `MapColumn`: every element is `DataRow`
* `FrameColumn`: every element is `DataFrame`
* `ValueColumn`: all other types of elements

### ValueColumn
`ValueColumn` stores one dimensional array of elements

### MapColumn
Every element of `MapColumn` is `DataRow`, so `MapColumn` can be interpreted as a group of columns. 
Most `DataFrame` operations, such as [select](#select), [filter](#filter), indexing etc. are also available for `MapColumn`.

### FrameColumn
Every element of `FrameColumn` is `DataFrame`, so `FrameColumn` can be interpreted as groups of rows. 
Any `DataFrame` with `FrameColumn` can be [converted](#working-with-groupeddataframe) to `GroupedDataFrame`:
```kotlin
df.asGrouped { groups }
```
`DataFrame`s stored in `FrameColumn` can be [unioned](#union) into single `DataFrame`:
```kotlin
val df = frameColumn.union()
```
## Column Selectors
`DataFrame` provides a column selection DSL for selecting arbitrary set of columns.
Column selectors are used in many operations:
```
df.select { columns }
df.remove { columns }
df.update { columns }.with { expression }
df.gather { columns }.into(keyName, valueName)
df.move { columns }.under(groupName)
```
### Select single column
```
columnName // column by extension property
it.columnName // column by extension property
column // column by accessor
it[column] // column by accessor
it["columnName"] // column by name
"columnName"<Type>() // typed column by name
col(index) // column by index
column.rename("newName") // column with a new name
```
### Select several columns
```
columnSet1 and columnSet2 // union of column sets
cols(index1, ..., indexN) // columns by indices
cols(index1..index2) // columns by range of indices
cols { condition } // columns by condition
colsOf<Type>() // columns of specific type
colsOf<Type> { condition } // columns of specfic type that match condition
dfs { condition } // traverse column tree and yield top-level columns that match condition
dfsOf<Type>() // traverse column tree and yield columns of specific type
dfsOf<Type> { condition } // traverse column tree and yield columns of specific type that match condition
all() // all columns
```
### Special column selectors
```
// Select columns of specific type, with optional predicate
stringCols { condition }
intCols { condition }
booleanCols { condition }
doubleCols { condition }

// Select columns by column name condition
nameContains(text)
startsWith(prefix)
endsWith(suffix)
```
### Modify resulting column set
```
columnSet.drop(n) // remove first 'n' columns from column set
columnSet.take(n) // take first 'n' columns of column sest
columnSet.filter { condition } // filter columns set by condition
columnSet.except { otherColumnSet }
columnSet.except ( otherColumnSet )
```
Column selectors can be used to select subcolumns of a `MapColumn`
```kotlin
val firstName by column("Alice", "Bob")
val middleName by column("Jr", null)
val lastName by column("Merton", "Marley")
val age by column(15, 20)

val fullName by column(firstName, middleName, lastName) // create column group of three columns
val df = fullName + age

df.select { fullName.cols { !it.hasNulls } } // firstName, lastName
df.select { fullName.cols(0, 2) } // firstName, lastName
df.select { fullName.cols(0..1) } // firstName, middleName
df.select { fullName[firstName] }
df.select { fullName.cols(middleName, lastName) }
df.select { fullName.cols().drop(1) }
```
## Row expressions
Row expression provide a value for every row of `DataFrame` and is used in [add](#add), [filter](#filter), [forEach](#forEach), [update](#update) and other opertaions

Row expression syntax is ```DataRow.(DataRow) -> T``` so row values can be accessed with or without ```it``` keyword
```kotlin
df.filter { it.name.startsWith("A") }
df.filter { name.length == 5 }
```
Within row expression you can access [row-properties](#row-properties)
```kotlin
df.add("diff") { value - prev?.value }
df.filter { index % 5 == 0 }
```
## Row properties
`DataRow` object provides the following properties:
* `index` - sequential row number in `DataFrame`, starts from 0
* `prev` - previous row (`null` for the first row)
* `next` - next row (`null` for the last row)

If some of these properties clash with generated extension properties, they still can be accessed as functions `index()`, `prev()`, `next()`
## Column properties
