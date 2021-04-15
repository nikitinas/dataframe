# Data schemas and code generation

In Jupyter environment `DataFrame` provides typed data access by automatic inference of `DataSchema` of new `DataFrame` instances and generation of schema-specific extension properties

## Overview
After execution of cell
```kotlin
val df = dataFrameOf("name", "age")(
            "Alice", 15,
            "Bob", null)
``` 
the following actions take place:
1. Columns in `df` are analyzed to extract data schema
2. Empty interface with `DataSchema` annotation is generated:
```kotlin
@DataSchema
interface DataFrameType
```
3. Extension properties for this `DataSchema` are generated:
```kotlin
val DataFrameBase<DataFrameType>.name: DataColumn<String> get() = this["name"] as DataColumn<String>
val DataRowBase<DataFrameType>.name: String get() = this["name"] as String
val DataFrameBase<DataFrameType>.age: DataColumn<Int?> get() = this["age"] as DataColumn<Int?>
val DataRowBase<DataFrameType>.age: Int? get() = this["age"] as Int?
```
Every column of `DataFrame` produces two properties:
* Property for `DataFrameBase<DataFrameType>` returns column
* Property for `DataRowBase<DataFrameType>` returns cell value
4. `df` variable is typed by schema interface:
```kotlin
val temp = df
```
```kotlin
val df = temp as DataFrame<DataFrameType>
```
> _Note, that object instance after typing remains the same_

To log all these additional code executions, use cell magic
```
%trackExecution -all
```

## Schema inheritance
In order to reduce amount of generated code, previously generated `DataSchema` interfaces are reused and only new properties are introduced

Let's filter out all `null` values from `age` column and add one more column of type `Boolean`:
```kotlin
val filtered = df.filter { age != null }.add("isAdult") { age!! > 18 }
```
New schema interface for `filtered` variable will be derived from previously generated `DataFrameType`:
```kotlin
@DataSchema
interface DataFrameType1: DataFrameType
```
Extension properties for data access are generated only for new and overriden members of `DataFrameType1` interface: 
```kotlin
val DataFrameBase<DataFrameType1>.age: DataColumn<Int> get() = this["age"] as DataColumn<Int>
val DataRowBase<DataFrameType1>.age: Int get() = this["age"] as Int
val DataFrameBase<DataFrameType1>.isAdult: DataColumn<Boolean> get() = this["isAdult"] as DataColumn<Boolean>
val DataRowBase<DataFrameType1>.isAdult: String get() = this["isAdult"] as Boolean
```
Then variable `filtered` is typed by new interface:
```kotlin
val temp = filtered
```
```kotlin
val filtered = temp as DataFrame<DataFrameType1>
```

## Custom data schemas
Besides auto-generated schema interfaces, you can explicitly define your own data schema:
```kotlin
@DataSchema
interface Person {
    val name: String
    val age: Int 
}
```
After execution of this cell in Jupyter, extension properties for data access will be generated. Now we can use these properties to create functions for typed `DataFrame`:
```kotlin
fun DataFrame<Person>.splitName() = split { name }.by(",").into("firstName", "lastName")
fun DataFrame<Person>.adults() = filter { age > 18 }
```
These functions will work for any `DataFrame` that matches `Person` schema:
```kotlin
val df = dataFrameOf("name", "age", "weight")(
            "Merton, Alice", 15, 60.0,
            "Marley, Bob", 20, 73.5)
```
Schema of `df` is compatible with `Person`, so auto-generated schema interface will inherit from it:
```kotlin
@DataSchema(isOpen = false)
interface DataFrameType : Person
val DataFrameBase<DataFrameType>.age: DataColumn<Double> get() = this["weight"] as DataColumn<Double>
val DataRowBase<DataFrameType>.age: Double get() = this["weight"] as Int
```
Despite `df` has additional column `weight`, previously defined methods for `DataFrame<Person>` work for it:
```kotlin
df.splitName()
```
|firstName |lastName |age |weight |
|-----------------|----------------|--------|--------------|
|Merton           |Alice           |15      |60.0     |
|Marley           |Bob             |20      |73.5     |
```kotlin
w.adults()
```
|name |age |weight |
|------------|--------|--------------|
|Marley, Bob |20      |73.5     |

## Use external data schemas in Jupyter
Sometimes it is convenient to extract reusable code from Jupyter notebook into Kotlin JVM library. If this code uses [Custom data schemas](#custom-data-schemas), schema interfaces should also be extracted. In order to later support them in Jupyter, you should register them in library [integration class](#https://github.com/Kotlin/kotlin-jupyter/blob/master/docs/libraries.md#integration-using-kotlin-api) with `useDataSchemas` function:
```kotlin
@DataSchema
interface Person {
    val name: String
    val age: Int 
}

fun DataFrame<Person>.countAdults() = count { it[Person::age] > 18 }

@JupyterLibrary
internal class Integration : JupyterIntegration() {
    
    override fun Builder.onLoaded() {
        onLoaded {
            useSchema<Person>()
        }
    }
}
```
After loading this library into Jupyter notebook, schema interfaces for all `DataFrame` variables that match `Person` schema will derive from `Person` 
```kotlin
val df = dataFrameOf("name", "age")(
            "Alice", 15,
            "Bob", 20)
```
Now `df` is assignable to `DataFrame<Person>` and `countAdults` is available: 
```kotlin
df.countAdults()
```

    