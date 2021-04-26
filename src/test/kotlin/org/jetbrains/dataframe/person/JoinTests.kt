package org.jetbrains.dataframe.person

import io.kotlintest.shouldBe
import org.jetbrains.dataframe.*
import org.jetbrains.dataframe.annotations.DataSchema
import org.jetbrains.dataframe.impl.columns.typed
import org.junit.Test

class JoinTests : BaseTest() {

    val df2 = dataFrameOf("name", "origin", "grade", "age")(
            "Alice", "London", 3, "young",
            "Alice", "London", 5, "old",
            "Bob", "Tokyo", 4, "young",
            "Bob", "Paris", 5, "old",
            "Mark", "Moscow", 1, "young",
            "Mark", "Moscow", 2, "old",
            "Bob", "Paris", 4, null
    )

// Generated Code

    @DataSchema
    interface Person2 {
        val name: String
        val origin: String?
        val grade: Int
    }


    val DataRowBase<Person2>.name @JvmName("get-name") get() = this["name"] as String
    val DataRowBase<Person2>.origin get() = this["origin"] as String?
    val DataRowBase<Person2>.grade get() = this["grade"] as Int
    val DataFrameBase<Person2>.name @JvmName("get-name") get() = this["name"].typed<String>()
    val DataFrameBase<Person2>.origin get() = this["origin"].typed<String?>()
    val DataFrameBase<Person2>.grade get() = this["grade"].typed<Int>()

    val typed2: DataFrame<Person2> = df2.typed()

    @Test
    fun `inner join`() {

        val res = typed.innerJoin(typed2) { name and it.city.match(right.origin) }
        res.ncol() shouldBe 6
        res.nrow() shouldBe 7
        res["age_1"].hasNulls shouldBe false
        res.count { name == "Mark" && city == "Moscow" } shouldBe 4
        res.select { city and name }.distinct().nrow() shouldBe 3
        res[Person2::grade].hasNulls shouldBe false
    }

    @Test
    fun `left join`() {

        val res = typed.leftJoin(typed2) { name and it.city.match(right.origin) }

        res.ncol() shouldBe 6
        res.nrow() shouldBe 10
        res["age_1"].hasNulls shouldBe true
        res.select { city and name }.distinct().nrow() shouldBe 6
        res.count { it["grade"] == null } shouldBe 3
        res.age.hasNulls shouldBe false
    }

    @Test
    fun `right join`() {

        val res = typed.rightJoin(typed2) { name and it.city.match(right.origin) }

        res.ncol() shouldBe 6
        res.nrow() shouldBe 9
        res["age_1"].hasNulls shouldBe true
        res.select { city and name }.distinct().nrow() shouldBe 4
        res[Person2::grade].hasNulls shouldBe false
        res.age.hasNulls shouldBe true
        val newEntries = res.filter { it["age"] == null }
        newEntries.nrow() shouldBe 2
        newEntries.all { name == "Bob" && city == "Paris" && weight == null } shouldBe true
    }

    @Test
    fun `outer join`() {

        val res = typed.outerJoin(typed2) { name and it.city.match(right.origin) }
        println(res)
        res.ncol() shouldBe 6
        res.nrow() shouldBe 12
        res.name.hasNulls shouldBe false
        res.columns().filter { it != res.name }.all { it.hasNulls } shouldBe true
        res.select { city and name }.distinct().nrow() shouldBe 7
        val distinct = res.select { name and age and city and weight }.distinct()
        val expected = typed.append("Bob", null, "Paris", null)
        distinct shouldBe expected
    }

    @Test
    fun `filter join`() {

        val res = typed.filterJoin(typed2) { city.match(right.origin) }
        val expected = typed.innerJoin(typed2.select { origin }) { city.match(right.origin) }
        res shouldBe expected
    }

    @Test
    fun `filter not join`() {

        val res = typed.excludeJoin(typed2) { city.match(right.origin) }
        res.nrow() shouldBe 3
        res.city.toSet() shouldBe typed.city.toSet() - typed2.origin.toSet()

        val indexColumn by column<Int>("__index__")
        val withIndex = typed.addRowNumber(indexColumn)
        val joined = withIndex.filterJoin(typed2) { city.match(right.origin) }
        val joinedIndices = joined[indexColumn].toSet()
        val expected = withIndex.filter { !joinedIndices.contains(it[indexColumn]) }.remove(indexColumn)

        res shouldBe expected
    }
}