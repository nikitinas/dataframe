package org.jetbrains.dataframe.person

import org.jetbrains.dataframe.*
import org.jetbrains.dataframe.annotations.DataSchema
import org.jetbrains.dataframe.impl.columns.asValues

open class BaseTest {

// Data set

    val df = dataFrameOf("name", "age", "city", "weight")(
            "Alice", 15, "London", 54,
            "Bob", 45, "Dubai", 87,
            "Mark", 20, "Moscow", null,
            "Mark", 40, "Milan", null,
            "Bob", 30, "Tokyo", 68,
            "Alice", 20, null, 55,
            "Mark", 30, "Moscow", 90
            )

// Generated Code

    @DataSchema
    interface Person {
        val name: String
        val age: Int
        val city: String?
        val weight: Int?
    }

    val DataRowBase<Person>.name get() = this["name"] as String
    val DataRowBase<Person>.age get() = this["age"] as Int
    val DataRowBase<Person>.city get() = this["city"] as String?
    val DataRowBase<Person>.weight get() = this["weight"] as Int?
    val DataFrameBase<Person>.name get() = this["name"].asValues<String>()
    val DataFrameBase<Person>.age get() = this["age"].asValues<Int>()
    val DataFrameBase<Person>.city get() = this["city"].asValues<String?>()
    val DataFrameBase<Person>.weight get() = this["weight"].asValues<Int?>()

    val typed: DataFrame<Person> = df.typed()

// Manual Column Definitions

    val name by column<String>()
    val age = column<Int>() named "age"
    val city = Person::city.toColumnDef()
    val weight by column<Int?>()
}