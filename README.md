# Kotlin DataFrame: data manipulation library
[![Kotlin](https://img.shields.io/badge/kotlin-1.4.20-blue.svg?logo=kotlin)](http://kotlinlang.org)
[![Maven Central](https://img.shields.io/maven-central/v/org.jetbrains.kotlinx/dataframe?color=blue&label=Maven%20Central)](https://search.maven.org/artifact/org.jetbrains.kotlinx/dataframe)
[![GitHub License](https://img.shields.io/badge/license-Apache%20License%202.0-blue.svg?style=flat)](http://www.apache.org/licenses/LICENSE-2.0)

Kotlin DataFrame is a library for in-memory data manipulation
* Simple, readable and powerful DSL for data wrangling
* Supports hierarchical data layouts
* Reads CSV and JSON
* Tracks column nullability
* Provides statically typed API via [code generation](docs/schemas.md)
* Integrates with [Kotlin kernel for Jupyter](https://github.com/Kotlin/kotlin-jupyter)

Inspired by [krangl](https://github.com/holgerbrandl/krangl) and [pandas](https://pandas.pydata.org/)

See [API reference](docs/reference.md) for a list of  supported operations 

## Setup

### Gradle
```groovy
repositories {
    mavenCentral()
}
dependencies {
    implementation 'org.jetbrains.kotlin:dataframe:0.7.3'
}
```
### Jupyter Notebook

Install [Kotlin kernel](https://github.com/Kotlin/kotlin-jupyter) for [Jupyter](https://jupyter.org/)

Import latest stable `dataframe` version into notebook: 
```
%use dataframe
```
or specific versoin:
```
%use dataframe(<version>)
```
## Key entities
* `DataColumn` is a named list of values
* `DataFrame` consists of `DataColumns` with unique names and equal size
* `DataRow` is a single row of `DataFrame` and provides a single value for every `DataColumn`
* `DataFrame` can be optionally typed by `DataSchema` to provide typed data access via extension properties  
* [`DataSchema`](docs/schemas.md) is an interface that describes a single row of `DataFrame`

## Data access

DataFrame comes with three levels of API for data access

### Strings

String column names are the easiest way to access data in DataFrame: 
```kotlin
val df = DataFrame.read("titanic.csv")
df.filter { it["survived"] as Boolean }.groupBy("city").max("age")
```
or using `invoke` operator:
```kotlin
df.filter { "survived"<Boolean>() }.groupBy("city").max("age")
```
### Column Accessors
For frequently accessed columns type casting can be reduced by `ColumnAccessors`:
```kotlin
val survived by column<Boolean>() // accessor for Boolean column with name 'survived'
val home by column<String>()
val age by column<Int?>()
```
Now columns can be accessed in a type-safe way:
```kotlin
df.filter { it[survived] && it[home].endsWith("NY") && it[age] in 10..20 }
```
or just using `invoke` operator at column accessors:
```kotlin
df.filter { survived() && home().endsWith("NY") && age() in 10..20 }
```
### Extension properties
When DataFrame is used within Jupyter Notebooks with [Kotlin Kernel](https://github.com/Kotlin/kotlin-jupyter) there is even more type safe way to access data. 
After every REPL line execution all new global variables of type `DataFrame` are analyzed and replaced with typed `DataFrame` wrapper with auto-generated extension properties 
for data access:
```kotlin
val df = DataFrame.read("titanic.csv")
```
Now data can be accessed by `.` member accessor
```kotlin
df.filter { it.survived && it.home.endsWith("NY") && it.age in 10..20 }
```
`it` can be omitted:    
```kotlin
df.filter { survived && home.endsWith("NY") && age in 10..20 }
```
Extension properties are generated for `DataSchema` that is extracted from `DataFrame` instance after REPL line execution.
After that `DataFrame` variable is typed with its own `DataSchema`, so only valid extension properties corresponding 
to actual columns in `DataFrame` will be allowed by compiler and suggested by completion.

[Learn more about data schemas and code generation](docs/schemas.md)
### Nullability
DataFrame distinguishes between nullable and non-nullable columns in compliance with [Kotlin null safety](https://kotlinlang.org/docs/null-safety.html).
If `DataColumn` actually contains `null` values, it's `type` is marked nullable

Example in Jupyter: 
```kotlin
val df = dataFrameOf("name", "age")(
    "Alice", 20,
    "Bob", null)
```
Typed wrapper for `df`  is generated: `age` is nullable
```kotlin
df.filter { age > 10 } // compilation error
```
```kotlin
df.filter { age != null && age!! > 10 } // ok
df.filter { age?.let { it > 10 } ?: false } // ok
```
```kotlin
val cleaned = df.filter { age != null }
```
Typed wrapper for `cleaned` is generated: `age` is not nullable
```kotlin
cleaned.filter { age > 10 } // ok
```
