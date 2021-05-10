package org.jetbrains.dataframe.person

import io.kotest.matchers.shouldBe
import org.jetbrains.dataframe.*
import org.jetbrains.dataframe.annotations.DataSchema
import org.jetbrains.dataframe.impl.columns.typed
import org.junit.Test
import java.io.Serializable

class SpreadTests {

    val df = dataFrameOf("name", "key", "value")(
            "Alice", "age", 15,
            "Alice", "city", "London",
            "Alice", "weight", 54,
            "Bob", "age", 45,
            "Bob", "city", "Dubai",
            "Bob", "weight", 87,
            "Mark", "age", 20,
            "Mark", "city", "Moscow",
            "Mark", "weight", null
    )

// Generated Code

    @DataSchema
    interface Person {
        val name: String
        val key: String
        val value: Any?
    }

    val DataRowBase<Person>.name get() = this["name"] as String
    val DataRowBase<Person>.key get() = this["key"] as String
    val DataRowBase<Person>.value get() = this["value"] as Any?
    val DataFrameBase<Person>.name get() = this["name"].typed<String>()
    val DataFrameBase<Person>.key get() = this["key"].typed<String>()
    val DataFrameBase<Person>.value get() = this["value"].typed<Any?>()

    val typed: DataFrame<Person> = df.typed()

// Tests

    val keyConverter: (String) -> String = { "__$it" }
    val valueConverter: (Any?) -> Any? = { (it as? Int)?.toDouble() ?: it }

    @Test
    fun `spread exists`() {

        val res = typed.select { name and key }.spread { key }.into { it }
        res.ncol() shouldBe 1 + typed.key.ndistinct
        res.nrow() shouldBe typed.name.ndistinct

        val expected = typed.map { (name to key) }.toSet()
        val actual = res.columns().subList(1, res.ncol()).flatMap {
            val columnName = it.name()
            res.map {
                val value = it[columnName] as Boolean
                if(value)
                    (name to columnName)
                else null
            }.filterNotNull()
        }.toSet()

        actual shouldBe expected
        res["age"].type shouldBe getType<Boolean>()
        res["city"].type shouldBe getType<Boolean>()
        res["weight"].type shouldBe getType<Boolean>()
    }

    @Test
    fun `spread to pair`() {

        val res = typed.spread { key }.by { value }.into { it }

        res.ncol() shouldBe 1 + typed.key.ndistinct
        res.nrow() shouldBe typed.name.ndistinct

        val expected = typed.map { (name to key) to value }.toMap()
        val actual = res.columns().subList(1, res.ncol()).flatMap {
            val columnName = it.name()
            res.map { (name to columnName) to it[columnName] }
        }.toMap()

        actual shouldBe expected
        res["age"].type shouldBe getType<Int>()
        res["city"].type shouldBe getType<String>()
        res["weight"].type shouldBe getType<Int?>()
    }

    @Test
    fun `spread to pair with group key and conversion`() {

        val res = typed.spread { key }.by { value }.map { it.toString() }.into { it }

        res.ncol() shouldBe 1 + typed.key.ndistinct
        res.nrow() shouldBe typed.name.ndistinct

        val expected = typed.map { (name to key) to value.toString() }.toMap()
        val actual = res.columns().subList(1, res.ncol()).flatMap {
            val columnName = it.name()
            res.map { (name to columnName) to it[columnName] }
        }.toMap()

        actual shouldBe expected
    }

    @Test
    fun `spread duplicate values`() {

        val first = typed[0]
        val values = first.values.toTypedArray()
        values[2] = 30
        val modified = typed.append(*values)
        val spread = modified.spread { key }.by { value }.into { it }
        spread.ncol() shouldBe 1 + typed.key.ndistinct

        spread["age"].type shouldBe getType<List<Int>>()
        spread["city"].type shouldBe getType<String>()
        spread["weight"].type shouldBe getType<Int?>()

        val expected = modified.filter { key == "age" }.remove { key }.groupBy { name }.aggregate {
            value.cast<Int>().toList() into "age"
        }

        val actual = spread.select("name", "age")

        actual shouldBe expected

        spread["age"][0] shouldBe listOf(15, 30)
    }

    @Test
    fun gather() {

        val res = typed.spread { key }.by { value }.into { it }
        val gathered = res.gather { cols().drop(1) }.into("key", "value")
        gathered shouldBe typed
    }

    @Test
    fun `gather with filter`() {

        val spread = typed.spread { key }.by { value }.into { it }
        val gathered = spread.gather { cols().drop(1) }.where { it != null }.into("key", "value")
        gathered shouldBe typed.filterNotNull { value }
    }

    @Test
    fun `spread with key conversion`() {

        val spread = typed.spread { key }.by { value }.into { keyConverter(it) }
        val gathered = spread.gather { cols().drop(1) }.into("key", "value")
        gathered shouldBe typed.update { key }.with { "__$it" }
    }

    @Test
    fun `spread with value conversion`() {

        val spread = typed.spread { key }.by { value }.map(valueConverter).into { it }
        val gathered = spread.gather { cols().drop(1) }.into("key", "value")
        val expected = typed.update { value }.with { valueConverter(it) as? Serializable }
        gathered shouldBe expected
    }

    @Test
    fun `grouped spread with key and value conversions`() {
        val grouped = typed.groupBy { name }
        val spread1 = grouped.spread { key }.withSingle { valueConverter(value) }.into { keyConverter(it) }

        val spread2 = grouped.aggregate {
            spread { key }.withSingle { valueConverter(value) }.into(keyConverter)
        }
        spread2 shouldBe spread1
        val gathered = spread1.gather { cols().drop(1) }.into("key", "value")
        val expected = typed.update { key }.with { keyConverter(it) }.update { value }.with { valueConverter(it) as? Serializable }
        gathered shouldBe expected
    }

    @Test
    fun `grouped spread with key and value conversions 2`() {
        val spread = typed.groupBy { name }.spread { key }.withSingle { valueConverter(value) }.into(keyConverter)
        val gathered = spread.gather { cols().drop(1) }.into("key", "value")
        val expected = typed.update { key }.with { keyConverter(it) }.update { value }.with { valueConverter(it) as? Serializable }
        gathered shouldBe expected
    }

    @Test
    fun `gather with value conversion`() {

        val spread = typed.spread { key }.by { value }.map(valueConverter).into { it }
        val gathered = spread.gather { cols().drop(1) }.map { (it as? Double)?.toInt() ?: it }.into("key", "value")
        gathered shouldBe typed
    }

    @Test
    fun `gather doubles with value conversion`() {

        val spread = typed.spread { key }.by { value }.map(valueConverter).into { it }
        val gathered = spread.remove("city").gather { colsOf<Double?>() }.mapNotNull { it.toInt() }.into("key", "value")
        val expected = typed.filter { key != "city" }.cast { value }.to<Int>()
        gathered shouldBe expected
    }

    @Test
    fun `gather with name conversion`() {

        val spread = typed.spread { key }.by { value }.into(keyConverter)
        val gathered = spread.gather { cols().drop(1) }.mapNames { it.substring(2) }.into("key", "value")
        gathered shouldBe typed
    }
}