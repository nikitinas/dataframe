package org.jetbrains.dataframe.person

import io.kotest.matchers.shouldBe
import org.jetbrains.dataframe.*
import org.jetbrains.dataframe.annotations.DataSchema
import org.jetbrains.dataframe.columns.typeClass
import org.jetbrains.dataframe.impl.columns.asGroup
import org.jetbrains.dataframe.impl.columns.typed
import org.junit.Test
import java.io.Serializable
import kotlin.reflect.KClass
import kotlin.reflect.full.isSubclassOf

class PivotTests {

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

    val name by column<String>()
    val key by column<String>()
    val value by column<Any?>()

// Tests

    val keyConverter: (String) -> String = { "__$it" }
    val valueConverter: (Any?) -> Any? = { (it as? Int)?.toDouble() ?: it }

    @Test
    fun `pivot matches`() {

        val filtered = typed.drop(1)
        val res = filtered.pivot { key }.withIndex { name }.matches()
        res.ncol() shouldBe 1 + filtered.key.ndistinct()
        res.nrow() shouldBe filtered.name.ndistinct()

        val expected = filtered.map { (name to key) }.toSet()
        val actual = res.columns().subList(1, res.ncol()).flatMap {
            val columnName = it.name()
            res.map {
                val value = it[columnName] as Boolean
                if (value)
                    (it.name to columnName)
                else null
            }.filterNotNull()
        }.toSet()

        actual shouldBe expected
        res["age"].type() shouldBe getType<Boolean>()
        res["city"].type() shouldBe getType<Boolean>()
        res["weight"].type() shouldBe getType<Boolean>()
    }

    @Test
    fun `simple pivot`() {

        val res = typed.pivot { key }.withIndex { name }.value { value }

        res.ncol() shouldBe 1 + typed.key.ndistinct()
        res.nrow() shouldBe typed.name.ndistinct()

        res["age"].type() shouldBe getType<Int>()
        res["city"].type() shouldBe getType<String>()
        res["weight"].type() shouldBe getType<Int?>()

        val expected = typed.map { (name to key) to value }.toMap()
        val actual = res.columns().subList(1, res.ncol()).flatMap {
            val columnName = it.name()
            res.map { (name to columnName) to it[columnName] }
        }.toMap()

        actual shouldBe expected


        typed.pivot { key }.withIndex { name }.into { value } shouldBe res
        df.pivot { key }.withIndex { name }.into { value } shouldBe res
        df.pivot(key).withIndex(name).into(value) shouldBe res
    }

    @Test
    fun `pivot value of`() {

        val res = typed.pivot { key }.withIndex { name }.valueOf { value.toString() }

        res.ncol() shouldBe 1 + typed.key.ndistinct()
        res.nrow() shouldBe typed.name.ndistinct()

        val expected = typed.map { (name to key) to value.toString() }.toMap()
        val actual = res.columns().subList(1, res.ncol()).flatMap {
            val columnName = it.name()
            res.map { (name to columnName) to it[columnName] }
        }.toMap()

        actual shouldBe expected
    }

    @Test
    fun `pivot duplicate values`() {

        val first = typed[0]
        val values = first.values.toTypedArray()
        values[2] = 30
        val modified = typed.append(*values)
        val pivoted = modified.pivot { key }.withIndex { name }.into { value }
        pivoted.ncol() shouldBe 1 + typed.key.ndistinct()

        pivoted["age"].type() shouldBe getType<Many<Int>>()
        pivoted["city"].type() shouldBe getType<String>()
        pivoted["weight"].type() shouldBe getType<Int?>()

        val expected = modified.filter { key == "age" }.remove { key }.groupBy { name }.aggregate {
            value.values().map { it as Int }.wrapValues() into "age"
        }

        val actual = pivoted.select("name", "age")

        actual shouldBe expected

        pivoted["age"][0] shouldBe listOf(15, 30)
    }

    @Test
    fun `pivot with key map`() {
        val pivoted = typed.pivot { key.map { "_$it" } }.withIndex { name }.into { value }
        pivoted.columnNames().drop(1).toSet() shouldBe typed.key.distinct().map { "_$it" }.toSet()
    }

    @Test
    fun `pivot with index map`() {
        val pivoted = typed.pivot { key }.withIndex { name.map { "_$it" } }.into { value }
        pivoted.name shouldBe typed.name.distinct().map { "_$it" }
    }

    @Test
    fun `pivot with value map`() {
        val pivoted = typed.pivot { key }.withIndex { name }.value { value.map { "_$it" } }
        pivoted.map { it.values().drop(1) }.flatten().filterNotNull().distinct() shouldBe typed.value.values()
            .distinct().map { "_$it" }
        typed.pivot { key }.withIndex { name }.into { "_$value" } shouldBe pivoted
    }

    @Test
    fun `pivot two values`() {
        val pivoted = typed.pivot { key }.withIndex { name }.values { value and "type" { value?.javaClass?.kotlin } }
        pivoted.ncol() shouldBe 1 + typed.key.ndistinct()
        pivoted.columns().drop(1).forEach {
            it.kind() shouldBe ColumnKind.Group
            val group = it.asGroup()
            group.columnNames() shouldBe listOf("value", "type")
            val valueType = group["value"].type().classifier
            group.forEach {
                it["type"] shouldBe if (it["value"] != null) valueType else null
            }
        }
        val updated = pivoted.replace { except { name } }.with { it["value"] named it.name() }
        updated shouldBe typed.pivot { key }.withIndex { name }.into { value }
    }

    @Test
    fun `pivot two values group by value`(){
        val type by column<KClass<*>>()
        val pivoted = typed.add(type){ value?.javaClass?.kotlin }
            .pivot { key }.withIndex { name }.groupByValue().values { value and type }
        pivoted.print()
        pivoted.ncol() shouldBe 3
    }

    @Test
    fun `pivot two columns`() {
        val pivoted = typed.add("index") { 1 }.pivot { name and key }.withIndex("index").into { value }

        pivoted.columnNames() shouldBe listOf("index") + typed.name.distinct().values()
        pivoted.nrow() shouldBe 1

        pivoted.columns().drop(1).forEach {
            val group = it.asGroup()
            group.ncol() shouldBe 3
            group.columnNames() shouldBe typed.key.distinct().values()
        }

        val leafColumns = pivoted.getColumnsWithPaths { all().drop(1).dfs() }
        leafColumns.size shouldBe typed.name.ndistinct() * typed.key.ndistinct()
        leafColumns.forEach { it.path.size shouldBe 2 }

        val data = leafColumns.associate { it.path[0] to it.path[1] to it.data[0] }
        val expected = typed.associate { name to key to value }
        data shouldBe expected

        val pivotedNoIndex = typed.pivot { name and key }.into { value }
        pivotedNoIndex shouldBe pivoted.remove("index")
    }

    @Test
    fun `pivot with two index columns`() {
        val pivoted = typed.dropNulls { value }.pivot { value.map { it!!.javaClass.kotlin.simpleName } }
            .withIndex { name and key }.into { value }
        val expected = typed.dropNulls { value }.add {
            "Int" { value as? Int }
            "String" { value as? String }
        }.remove("value")
        pivoted shouldBe expected
    }

    @Test
    fun `pivot two values without index`(){
        val pivoted = typed.pivot { name and key }.values { value and (value.map { it?.javaClass?.kotlin } named "type") }
        pivoted.ncol() shouldBe typed.name.ndistinct() + 1
        pivoted.nrow() shouldBe 2
        pivoted[defaultPivotIndexName].values() shouldBe listOf("value", "type")
        val cols = pivoted.getColumns { all().drop(1).dfs() }
        cols.size shouldBe typed.name.ndistinct() * typed.key.ndistinct()
        cols.forEach {
            it.typeClass shouldBe Any::class
            it.hasNulls() shouldBe it.values().any { it == null }
            it[1]?.javaClass?.kotlin?.isSubclassOf(KClass::class)?.let { it shouldBe true }
        }
    }

    @Test
    fun `resolve column name conflicts`() {
        val replaced = typed.replaceAll("city" to defaultPivotIndexName)
        val pivoted = replaced.pivot { key and name }.values { value and (value named "other") }
        pivoted.ncol() shouldBe 1 + typed.key.ndistinct()
        pivoted.nrow() shouldBe 2
        pivoted.columnNames().filter { it.startsWith(defaultPivotIndexName)}.size shouldBe 2
    }

    @Test
    fun `pivot in group aggregator`() {
        val pivoted = typed.groupBy { name }.aggregate {
            pivot { key }.into { value } into "key"
        }
        pivoted.ncol() shouldBe 2
        pivoted.print()
        pivoted.ungroup("key") shouldBe typed.pivot { key }.withIndex { name }.into { value }
    }

    @Test
    fun `equal pivots`() {
        val expected = typed.pivot { key }.withIndex { name }.into { value }
        typed.groupBy { name }.pivot { key }.value { value } shouldBe expected
        val pivoted = typed.groupBy { name }.aggregate {
            pivot { key }.into { value }
        }
        pivoted.print()
        pivoted shouldBe expected
    }

    @Test
    fun gather() {

        val res = typed.pivot { key }.withIndex { name }.into { value }
        val gathered = res.gather { cols().drop(1) }.into("key", "value")
        gathered shouldBe typed
    }

    @Test
    fun `gather with filter`() {

        val pivoted = typed.pivot { key }.withIndex { name }.into { value }
        val gathered = pivoted.gather { cols().drop(1) }.where { it != null }.into("key", "value")
        gathered shouldBe typed.dropNulls { value }
    }

    @Test
    fun `grouped pivot with key and value conversions`() {
        val grouped = typed.groupBy { name }
        val pivoted1 = grouped.pivot { key.map(keyConverter) }.into { valueConverter(value) }

        val pivoted2 = grouped.aggregate {
            pivot { key.map(keyConverter) }.valueOf { valueConverter(value) }
        }
        pivoted2 shouldBe pivoted1
        val gathered = pivoted1.gather { cols().drop(1) }.into("key", "value")
        val expected =
            typed.update { key }.with { keyConverter(it) }.update { value }.with { valueConverter(it) as? Serializable }
        gathered shouldBe expected
    }

    @Test
    fun `grouped pivot with key and value conversions 2`() {
        val pivoted = typed.groupBy { name }.pivot { key.map(keyConverter) }.into { valueConverter(value) }
        val gathered = pivoted.gather { cols().drop(1) }.into("key", "value")
        val expected =
            typed.update { key }.with { keyConverter(it) }.update { value }.with { valueConverter(it) as? Serializable }
        gathered shouldBe expected
    }

    @Test
    fun `gather with value conversion`() {

        val pivoted = typed.pivot { key }.withIndex {name}.into { valueConverter(value) }
        val gathered = pivoted.gather { cols().drop(1) }.map { (it as? Double)?.toInt() ?: it }.into("key", "value")
        gathered shouldBe typed
    }

    @Test
    fun `gather doubles with value conversion`() {

        val pivoted = typed.pivot { key }.withIndex{ name }.into { valueConverter(value) }
        val gathered = pivoted.remove("city").gather { colsOf<Double?>() }.mapNotNull { it.toInt() }.into("key", "value")
        val expected = typed.filter { key != "city" }.convert { value }.to<Int>()
        gathered shouldBe expected
    }

    @Test
    fun `gather with name conversion`() {

        val pivoted = typed.pivot { key.map(keyConverter) }.withIndex { name }.into { value }
        val gathered = pivoted.gather { cols().drop(1) }.mapNames { it.substring(2) }.into("key", "value")
        gathered shouldBe typed
    }

    @Test
    fun `type arguments inference in pivot with index`() {

        val id by columnOf(1, 1, 2, 2)
        val name by columnOf("set", "list", "set", "list")
        val data by columnOf(setOf(1), listOf(1), setOf(2), listOf(2))
        val df = dataFrameOf(id, name, data)
        df[data].type() shouldBe getType<Collection<Int>>()
        val pivoted = df.pivot { name }.withIndex { id }.value { data }
        pivoted.nrow() shouldBe 2
        pivoted.ncol() shouldBe 3
        pivoted["set"].type() shouldBe getType<java.util.AbstractSet<Int>>()
        pivoted["list"].type() shouldBe getType<java.util.AbstractList<Int>>()
    }

    @Test
    fun `type arguments inference in pivot`() {

        val name by columnOf("set", "list")
        val data by columnOf(setOf(1), listOf(1))
        val df = dataFrameOf(name, data)
        df[data].type() shouldBe getType<Collection<Int>>()
        val pivoted = df.pivot { name }.value { data }
        pivoted.nrow() shouldBe 1
        pivoted.ncol() shouldBe 2
        pivoted["set"].type() shouldBe getType<java.util.AbstractSet<Int>>()
        pivoted["list"].type() shouldBe getType<java.util.AbstractList<Int>>()
    }

    @Test
    fun `pivot with grouping`() {
        val pivoted = typed.pivot { key }.withIndex { name }.withGrouping("keys").into {value}
        pivoted.columnNames() shouldBe listOf("name", "keys")
        pivoted["keys"].asGroup().columnNames() shouldBe typed.key.distinct().values()
    }

    @Test
    fun `pivot matches yes no`() {
        val pivoted = typed.drop(1).pivot { key }.withIndex { name }.matches("yes", "no")
        pivoted.sumBy { values.count { it == "yes" } } shouldBe typed.nrow() - 1
        pivoted.sumBy { values.count { it == "no" } } shouldBe 1
    }

    @Test
    fun `pivot aggregate into`() {
        val pivoted = typed.pivot { key }.withIndex { name }.aggregate {
            value.first() into "value"
        }
        pivoted.columns().drop(1).forEach {
            it.kind() shouldBe ColumnKind.Group
            it.asGroup().columnNames() shouldBe listOf("value")
        }
    }

    @Test
    fun `pivot aggregate several into`() {
        val pivoted = typed.pivot { key }.withIndex { name }.aggregate {
            value.first() into "first value"
            value.last() into "last value"
            "unused"
        }
        pivoted.columns().drop(1).forEach {
            it.kind() shouldBe ColumnKind.Group
            it.asGroup().columnNames() shouldBe listOf("first value", "last value")
        }
    }
}