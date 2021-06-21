package org.jetbrains.dataframe.person

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.doubles.ToleranceMatcher
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import org.jetbrains.dataframe.*
import org.jetbrains.dataframe.max
import org.jetbrains.dataframe.mean
import org.jetbrains.dataframe.meanOf
import org.jetbrains.dataframe.min
import org.jetbrains.dataframe.sum
import org.jetbrains.dataframe.sumOf
import org.jetbrains.dataframe.columns.size
import org.jetbrains.dataframe.columns.toAccessor
import org.jetbrains.dataframe.columns.typeClass
import org.jetbrains.dataframe.impl.columns.asGroup
import org.jetbrains.dataframe.impl.columns.isTable
import org.jetbrains.dataframe.impl.columns.typed
import org.jetbrains.dataframe.impl.trackColumnAccess
import org.jetbrains.dataframe.io.renderValueForStdout
import org.junit.Test
import java.math.BigDecimal
import java.time.LocalDate
import kotlin.IllegalArgumentException
import kotlin.reflect.jvm.jvmErasure

class DataFrameTests : BaseTest() {

    @Test
    fun `create with columns`() {

        dataFrameOf("name", "age", "city", "weight")(df.columns()) shouldBe df

        dataFrameOf("name", "age", "city", "weight")(
            typed.name named "bla",
            typed.age named "",
            typed.city.rename("qq"),
            typed.weight.named("asda")
        ) shouldBe df

        val c1 = typed.name.toList().toColumn()
        val c2 = typed.age.toList().toColumn()
        val c3 = typed.city.toList().toColumn()
        val c4 = typed.weight.toList().toColumn()

        dataFrameOf("name", "age", "city", "weight")(c1, c2, c3, c4) shouldBe df
    }

    @Test
    fun `create with columnOf`() {

        val col = columnOf("Alice", "Bob")
        val d = dataFrameOf("name")(col)
        d.nrow shouldBe 2
        d.columnNames() shouldBe listOf("name")
    }

    @Test
    fun `create with unnamed columns`() {

        val a = columnOf("Alice", "Bob")
        val b = columnOf(1, 2)
        val d = dataFrameOf(a, b)
        d.nrow() shouldBe 2
        d.ncol() shouldBe 2
        d.columnNames() shouldBe listOf("", "")
        d[""] shouldBe d.column(0)
    }

    @Test(expected = IllegalArgumentException::class)
    fun `create with duplicate columns`() {

        val a = columnOf("Alice", "Bob") named "col"
        val b = columnOf(1, 2) named "col"
        val d = dataFrameOf(a, b)
    }

    @Test
    fun `create column reference`() {

        val name by column<String>()
        val col = name.withValues("Alice", "Bob")
        val df = col.toDataFrame()
        df.nrow shouldBe 2
        df.columnNames() shouldBe listOf("name")
    }

    @Test
    fun `add values to column reference`() {

        val name by column<String>()
        val values = listOf("Alice", "Bob")
        val col1 = name.withValues(values)
        val col2 = values.toColumn(name)
        col1 shouldBe col2
    }

    @Test
    fun `guess column type`() {
        val col by columnOf("Alice", 1, 3.5)
        col.type() shouldBe getType<Comparable<*>>()
        val filtered = col.filter { it is String }
        filtered.type() shouldBe getType<Comparable<*>>()
        filtered.guessType().type() shouldBe getType<String>()
    }

    @Test
    fun `create from map`() {
        val data = mapOf("name" to listOf("Alice", "Bob"), "age" to listOf(15, null))
        val df = data.toDataFrame()
        df.ncol() shouldBe 2
        df.nrow() shouldBe 2
        df.columnNames() shouldBe listOf("name", "age")
        df["name"].type() shouldBe getType<String>()
        df["age"].type() shouldBe getType<Int?>()
    }

    @Test
    fun `toMap`() {
        val map = df.toMap()
        map.size shouldBe 4
        map.forEach {
            it.value.size shouldBe df.nrow()
        }
    }

    @Test
    fun `size`() {
        df.size() shouldBe DataFrameSize(df.ncol(), df.nrow())
    }

    @Test
    fun `slicing`() {
        val sliced = typed[1..2]
        sliced.nrow() shouldBe 2
        sliced[0].name shouldBe typed[1].name
    }

    @Test
    fun `access tracking`() {
        trackColumnAccess {
            typed[2].age
        } shouldBe listOf("age")
    }

    @Test
    fun `indexing`() {
        val expected = 45
        val i = 1

        fun Int.check() = this shouldBe expected

        typed[i].age.check()
        typed.age[i].check()

        df[i][age].check()
        df[age][i].check()

        df[i][Person::age].check()
        df[Person::age][i].check()

        df[i].int("age").check()
        (df[i]["age"] as Int).check()

        df["age"].typed<Int>()[i].check()
        (df["age"][i] as Int).check()
    }

    @Test
    fun `null indexing`() {

        val i = 5

        fun String?.check() = this shouldBe null

        typed[i].city.check()
        typed.city[i].check()

        df[i][city].check()
        df[city][i].check()

        df[i][Person::city].check()
        df[Person::city][i].check()

        df[i].nstring("city").check()
        (df[i]["city"] as String?).check()

        df["city"].typed<String?>()[i].check()
        (df["city"][i] as String?).check()
    }

    @Test
    fun `incorrect column nullability`() {

        val col =
            column<Int>().named("weight") // non-nullable column definition is incorrect here, because actual dataframe has nulls in this column

        shouldThrow<NullPointerException> {
            println(df[2][col])
        }
    }

    @Test
    fun `chunked`() {
        val res = df.chunked(2)
        res.size() shouldBe 4
        res.toList().dropLast(1).forEach {
            it!!.nrow() shouldBe 2
        }
    }

    @Test
    fun `update`() {

        fun AnyFrame.check() {
            column(1).name() shouldBe "age"
            ncol() shouldBe typed.ncol()
            this["age"].toList() shouldBe typed.map { age * 2 }
        }

        typed.update { age }.with { it * 2 }.check()
        typed.update { age }.with { it * 2 }.check()
        typed.update(typed.age) { it * 2 }.check()

        df.update { age }.with { it * 2 }.check()
        df.update(age) { it * 2 }.check()
        df.update(age) { it * 2 }.check()

        df.update(Person::age) { it * 2 }.check()

        df.update("age") { int("age") * 2 }.check()
        df.update("age") { "age"<Int>() * 2 }.check()
    }

    @Test
    fun `conditional update`() {

        fun AnyFrame.check() {
            column(1).name() shouldBe "age"
            ncol() shouldBe typed.ncol()
            this["age"].toList() shouldBe typed.map { if (age > 25) null else age }
        }

        typed.update { age }.where { it > 25 }.withNull().check()
        typed.update { age }.where { it > 25 }.withNull().check()
        typed.update(typed.age).where { it > 25 }.withNull().check()

        df.update { age }.where { it > 25 }.withNull().check()
        df.update(age).where { it > 25 }.withNull().check()
        df.update(age).where { it > 25 }.withNull().check()

        df.update(Person::age).where { it > 25 }.withNull().check()

        df.update("age").where { it as Int > 25 }.withNull().check()
        df.update("age").where { it as Int > 25 }.withNull().check()
    }

    @Test
    fun `update cells by index`() {

        val res = typed.update { age }.at(2, 4).with(100)
        val expected = typed.map { if (index == 2 || index == 4) 100 else age }
        res.age.toList() shouldBe expected
    }

    @Test
    fun `update cells by index range`() {

        val res = typed.update { age }.at(2..4).with(100)
        val expected = typed.map { if (index in 2..4) 100 else age }
        res.age.toList() shouldBe expected
    }

    @Test
    fun `null to zero`() {
        val expected = typed.weight.toList().map { it ?: 0 }
        fun AnyFrame.check() {
            this["weight"].toList() shouldBe expected
        }

        typed.fillNulls { it.weight }.with(0).check()
        typed.fillNulls { weight }.with(0).check()
        typed.fillNulls(typed.weight).with(0).check()

        df.fillNulls { weight }.with(0).check()
        df.fillNulls(weight).with(0).check()

        df.fillNulls("weight").with(0).check()

        typed.nullToZero { it.weight }.check()
        typed.nullToZero { weight }.check()
        typed.nullToZero(typed.weight).check()

        df.nullToZero { weight }.check()
        df.nullToZero(weight).check()

        df.nullToZero("weight").check()
    }

    @Test
    fun `resetToNull`() {

        val updated = typed.update { all() }.withNull()

        updated.columns().forEach {
            it.forEach { it shouldBe null }
        }
    }

    @Test
    fun `sort`() {

        val expected = listOf(null, "London", "Dubai", "Tokyo", "Milan", "Moscow", "Moscow")

        fun AnyFrame.check() = this[city].toList() shouldBe expected

        typed.sortBy { name and age.desc }.check()
        typed.sortBy { it.name and it.age.desc }.check()

        df.sortBy { name and age.desc }.check()

        df.sortBy { Person::name and Person::age.desc }.check()

        df.sortBy { "name".cast<String>() and "age".desc }.check()
    }

    @Test
    fun `sort nulls first`() {

        val expected = typed.city.toList().sortedBy { it }

        fun AnyFrame.check() = this[city].toList() shouldBe expected

        typed.sortBy { city }.check()
        df.sortBy { city }.check()
        df.sortBy { col(Person::city) }.check()
        df.sortBy { col("city") }.check()
    }

    @Test
    fun `sort nulls last`() {

        val expected = typed.city.toList().filterNotNull().sortedBy { it } + listOf(null)

        fun AnyFrame.check() = this[city].toList() shouldBe expected

        typed.sortBy { city.nullsLast }.check()
        df.sortBy { city.nullsLast }.check()
        df.sortBy { Person::city.nullsLast }.check()
        df.sortBy { "city".nullsLast }.check()
    }

    @Test
    fun `equals`() {
        typed shouldBe typed.update { age }.with { age }
    }

    @Test
    fun `get group by single key`() {
        typed.groupBy { name }["Mark"] shouldBe typed.filter { name == "Mark" }
    }

    @Test
    fun `get group by complex key`() {
        typed.groupBy { city and name }["Tokyo", "Bob"] shouldBe typed.filter { name == "Bob" && city == "Tokyo" }
    }

    @Test
    fun `get group by partial key`() {
        typed.groupBy { city and name }["Tokyo"] shouldBe typed.filter { city == "Tokyo" }
    }

    @Test
    fun `group and sort`() {
        val expected = typed.sortBy { name.desc and age }
        val actual = typed.groupBy { name }.sortBy { name.desc and age }.ungroup()
        actual shouldBe expected
    }

    @Test
    fun `filter`() {

        val expected = listOf("Bob", "Bob", "Mark")
        fun AnyFrame.check() = this[name].toList() shouldBe expected

        val limit = 20

        typed.filter { it.age > limit && it.weight != null }.check()
        typed.filter { age > limit && it.weight != null }.check()

        df.filter { it[Person::age] > limit && it[Person::weight] != null }.check()

        df.filter { age > limit && weight() != null }.check()
        df.filter { it[age] > limit && this[weight] != null }.check()
        df.filter { age > limit && weight neq null }.check()

        df.filter { it.int("age") > limit && it.nint("weight") != null }.check()
        df.filter { "age"<Int>() > limit && "weight"<Int?>() != null }.check()
    }

    @Test
    fun `drop nulls 1`() {

        fun AnyFrame.check() = rows().forEach { get("weight") shouldNotBe null }

        typed.dropNulls(typed.weight).check()
        typed.dropNulls { weight }.check()
        typed.dropNulls { it.weight }.check()

        df.dropNulls(weight).check()
        df.dropNulls { weight }.check()

        df.dropNulls("weight").check()
    }

    @Test
    fun `drop where all null`() {

        val filtered = typed.update { weight }.where { name == "Alice" }.withNull()
        val expected = typed.nrow() - 1

        fun AnyFrame.check() = nrow() shouldBe expected

        filtered.dropNulls(typed.weight.toAccessor(), typed.city.toAccessor(), whereAllNull = true).check()
        filtered.dropNulls(whereAllNull = true) { weight and city }.check()
        filtered.dropNulls(whereAllNull = true) { it.weight and it.city }.check()

        filtered.dropNulls(Person::weight, Person::city, whereAllNull = true).check()

        filtered.dropNulls(weight, city, whereAllNull = true).check()
        filtered.dropNulls(whereAllNull = true) { weight and city }.check()

        filtered.dropNulls("weight", "city", whereAllNull = true).check()
    }

    @Test
    fun `drop where any null`() {

        val filtered = typed.update { weight }.where { name == "Alice" }.withNull()
        val expected = filtered.count { weight != null && city != null}

        fun AnyFrame.check() = nrow() shouldBe expected

        filtered.dropNulls(typed.weight.toAccessor(), typed.city.toAccessor()).check()
        filtered.dropNulls { weight and city }.check()
        filtered.dropNulls { it.weight and it.city }.check()

        filtered.dropNulls(Person::weight, Person::city).check()

        filtered.dropNulls(weight, city).check()
        filtered.dropNulls { weight and city }.check()

        filtered.dropNulls("weight", "city").check()

        filtered.dropNulls().check()

        filtered.select { weight and city }.dropNulls().check()
    }

    @Test
    fun `select one `() {
        val expected = listOf(typed.age)
        fun AnyFrame.check() = columns() shouldBe expected

        typed.select { age }.check()
        typed.select { it.age }.check()
        typed.select(typed.age).check()

        df.select(Person::age).check()

        df.select { age }.check()
        df.select(age).check()

        df.select("age").check()

        df.select { "age"() }.check()
        df.select { get("age") }.check()
        df.select { this["age"] }.check()
    }

    @Test
    fun `select if`() {
        val expected = listOf(typed.name, typed.city)

        fun AnyFrame.check() = columns() shouldBe expected

        typed.select { cols { it.name().length == 4 } }.check()
        df.select { cols { it.name().length == 4 } }.check()
    }

    @Test
    fun `select two`() {
        val expected = listOf(typed.age, typed.city)
        fun AnyFrame.check() = columns() shouldBe expected

        typed.select { age and city }.check()
        typed.select { it.age and it.city }.check()
        typed.select(typed.age, typed.city).check()

        typed.select(Person::age, Person::city).check()

        df.select { age and city }.check()
        df.select(age, city).check()
        df[age, city].check()

        df.select { "age" and "city" }.check()
        df.select("age", "city").check()
        df["age", "city"].check()
    }

    @Test
    fun `select by type`() {
        val selected = typed.select { colsOf<String?>() }
        selected shouldBe typed.select { name and city }
    }

    @Test
    fun `select by type not nullable`() {
        val selected = typed.select { colsOf<String> { !it.hasNulls() } }
        selected shouldBe typed.select { name }
    }

    @Test
    fun `move one column`() {
        val moved = typed.moveTo(1) { city }
        val expected = typed.select { cols(name, city, age, weight) }
        moved shouldBe expected
    }

    @Test
    fun `move several columns`() {
        val moved = typed.moveTo(2) { name and city }
        val expected = typed.select { cols(age, weight, name, city) }
        moved shouldBe expected
    }

    @Test
    fun `move several columns to left`() {
        val moved = typed.moveToLeft { weight and age }
        val expected = typed.select { cols(weight, age, name, city) }
        moved shouldBe expected
    }

    @Test
    fun `move several columns to right`() {
        val moved = typed.moveToRight { weight and name }
        val expected = typed.select { cols(age, city, weight, name) }
        moved shouldBe expected
    }

    @Test
    fun `select with rename 2`() {
        val res = typed.select { name named "Name" }
        res.columnNames() shouldBe listOf("Name")
        df.select { name named "Name"} shouldBe res
        df.select { it["name"] named "Name"} shouldBe res
    }

    @Test
    fun `select with map and rename`() {
        val res = typed.select { name.map { it.lowercase() } named "Name" }
        res.columnNames() shouldBe listOf("Name")
        res["Name"].values() shouldBe typed.name.values().map { it.lowercase() }
        df.select { name.map { it.lowercase() } named "Name" } shouldBe res
        df.select { it[Person::name].map { it.lowercase() } named "Name" } shouldBe res
        df.select { string("name").map { it.lowercase() } named "Name" } shouldBe res
    }

    @Test
    fun `get column with map`() {
        val converted = name.map { it.lowercase() }
        val res = df[converted]
        res.values() shouldBe typed.name.values().map { it.lowercase() }
    }

    @Test
    fun `get column by accessor`() {
        val res = df[0..1][name]
        res.size() shouldBe 2
    }

    @Test
    fun `groupBy`() {

        fun AnyFrame.check() {
            nrow() shouldBe 3
            this["name"].toList() shouldBe listOf("Alice", "Bob", "Mark")
            this["n"].toList() shouldBe listOf(2, 2, 3)
            this["old count"].toList() shouldBe listOf(0, 2, 2)
            this["median age"].toList() shouldBe listOf(17.5, 37.5, 30.0)
            this["min age"].toList() shouldBe listOf(15, 30, 20)
            this["oldest origin"].toList() shouldBe listOf(null, "Dubai", "Milan")
            this["youngest origin"].toList() shouldBe listOf("London", "Tokyo", "Moscow")
            this["all with weights"].toList() shouldBe listOf(true, true, false)
            this["from London"].toList() shouldBe listOf(1, 0, 0)
            this["from Dubai"].toList() shouldBe listOf(0, 1, 0)
            this["from Moscow"].toList() shouldBe listOf(0, 0, 2)
            this["from Milan"].toList() shouldBe listOf(0, 0, 1)
            this["from Tokyo"].toList() shouldBe listOf(0, 1, 0)
            this["from null"].toList() shouldBe listOf(1, 0, 0)
            this["ages"].toList() shouldBe listOf(listOf(15, 20), listOf(45, 30), listOf(20, 40, 30))
        }

        typed.groupBy { name }.aggregate {
            nrow() into "n"
            count { age > 25 } into "old count"
            median { age } into "median age"
            min { age } into "min age"
            all { weight != null } into "all with weights"
            maxBy { age }.city into "oldest origin"
            sortBy { age }.first().city into "youngest origin"
            pivot { city.map { "from $it"} }.count()
            age.toList() into "ages"
        }.check()

        typed.groupBy { it.name }.aggregate {
            it.nrow() into "n"
            it.count { it.age > 25 } into "old count"
            it.median { it.age } into "median age"
            it.min { it.age } into "min age"
            it.all { it.weight != null } into "all with weights"
            it.maxBy { it.age }.city into "oldest origin"
            it.sortBy { it.age }.first().city into "youngest origin"
            it.pivot { it.city.map { "from $it" } }.count()
            it.age.toList() into "ages"
        }.check()

        df.groupBy(name).aggregate {
            nrow() into "n"
            count { age > 25 } into "old count"
            median(age) into "median age"
            min(age) into "min age"
            all { weight neq null } into "all with weights"
            maxBy(age)[city] into "oldest origin"
            sortBy(age).first()[city] into "youngest origin"
            pivot(city.map { "from $it" }).count()
            it[age].toList() into "ages"
        }.check()

        df.groupBy(Person::name).aggregate {
            nrow() into "n"
            count { it[Person::age] > 25 } into "old count"
            median(Person::age) into "median age"
            min(Person::age) into "min age"
            all { Person::weight neq null } into "all with weights"
            maxBy(Person::age)[Person::city] into "oldest origin"
            sortBy(Person::age).first()[Person::city] into "youngest origin"
            pivot { it[Person::city].map { "from $it" } }.count()
            it[Person::age].toList() into "ages"
        }.check()

        df.groupBy("name").aggregate {
            nrow() into "n"
            count { int("age") > 25 } into "old count"
            median { int("age") } into "median age"
            min { int("age") } into "min age"
            all { get("weight") != null } into "all with weights"
            maxBy { int("age") }.get("city") into "oldest origin"
            sortBy("age").first()["city"] into "youngest origin"
            pivot { it["city"].map { "from $it" } }.count()
            it["age"].toList() into "ages"
        }.check()
    }

    @Test
    fun `groupBy invoked at column`() {

        typed.weight.groupBy(typed.name).mean() shouldBe typed.groupBy { name }.mean { weight }
    }

    @Test
    fun `groupBy meanOf`() {
        typed.groupBy { name }.meanOf { age * 2 } shouldBe typed.groupBy { name }
            .aggregate { mean { age } * 2 into "mean" }
    }

    @Test
    fun `min`() {
        val expected = 15

        fun Any?.check() = this shouldBe expected

        typed.minOf { age }.check()
        typed.min { it.age }.check()
        typed.age.min().check()

        df.min { age }.check()
        df.min(age).check()
        df[age].min().check()

        df.min { int("age") }.check()
        df.min("age").check()
        df["age"].typed<Int>().min().check()
    }

    @Test
    fun `nullable max`() {
        val expected = 90

        fun Int?.check() = this shouldBe expected

        typed.max { weight }.check()
        typed.max { it.weight }.check()
        typed.weight.max().check()

        df.max { weight }.check()
        df.max(weight).check()
        df[weight].max().check()

        df.max { nint("weight") }.check()
        df["weight"].typed<Int?>().max().check()
        (df.max("weight") as Int?).check()
    }

    @Test
    fun `nullable minBy`() {
        val expected = "Alice"

        fun AnyRow?.check() = this!![name] shouldBe expected

        typed.dropNulls { weight }.minBy { weight }.check()
        typed.dropNulls { it.weight }.minBy { it.weight }.check()

        df.dropNulls(weight).minBy { weight }.check()

        df.dropNulls("weight").minBy { int("weight") }.check()
        df.dropNulls("weight").minBy("weight").check()
    }

    @Test
    fun `maxBy`() {
        val expected = "Bob"

        fun AnyRow?.check() = this!![name] shouldBe expected

        typed.maxBy { age }.check()
        typed.maxBy { it.age }.check()
        typed.maxBy(typed.age).check()

        df.maxBy { age }.check()
        df.maxBy(age).check()

        df.maxBy { int("age") }.check()
        df.maxBy("age").check()
    }

    @Test
    fun `add one column`() {
        val now = 2020
        val expected = typed.map { now - age }

        fun AnyFrame.check() = this["year"].toList() shouldBe expected

        typed.add("year") { now - age }.check()
        typed.add("year") { now - it.age }.check()

        df.add("year") { now - age }.check()

        df.add("year") { now - int("age") }.check()
        df.add("year") { now - "age"<Int>() }.check()
    }

    @Test
    fun `add several columns`() {
        val now = 2020
        val expected = typed.map { now - age }

        fun AnyFrame.check() = (1..3).forEach { this["year$it"].toList() shouldBe expected }

        typed.add {
            "year1" { now - age }
            "year2"(now - age)
            now - age into "year3"
        }.check()
    }

    @Test
    fun `remove one column`() {

        val expected = listOf("name", "city", "weight")
        fun check(body: () -> AnyFrame) = body().columnNames() shouldBe expected

        check { typed - { age } }
        check { typed - { it.age } }
        check { typed.remove { age } }
        check { typed.remove { it.age } }

        check { df - { age } }
        check { df - age }
        check { df.remove(age) }

        check { df - "age" }
        check { df.remove("age") }
    }

    @Test
    fun `remove two columns`() {

        val expected = listOf("name", "city")
        fun check(body: () -> AnyFrame) = body().columnNames() shouldBe expected

        check { typed - { age and weight } }
        check { typed - { it.age and it.weight } }
        check { typed - { age } - { weight } }
        check { typed - { it.age } - { it.weight } }
        check { typed.remove { age and weight } }
        check { typed.remove { it.age and it.weight } }

        check { df - { age and weight } }
        check { df - age - weight }
        check { df - { age } - { weight } }
        check { df.remove(age, weight) }

        check { df - { "age" and "weight" } }
        check { df - "age" - "weight" }
        check { df.remove("age", "weight") }
    }

    @Test
    fun `merge similar dataframes`() {

        val res = typed + typed + typed
        res.name.size() shouldBe 3 * typed.nrow()
        res.rows().forEach { it.values() shouldBe typed[it.index % typed.nrow()].values() }
    }

    @Test
    fun `merge different dataframes`() {

        val height by column<Int>()
        val heightOrNull = height.nullable()

        val other = dataFrameOf(name, height)(
            "Bill", 135,
            "Mark", 160
        ).typed<Unit>()

        val res = typed.union(other)
        res.nrow() shouldBe typed.nrow() + other.nrow()
        res.take(typed.nrow()).rows().forEach { it[heightOrNull] == null }
        val q = res.takeLast(other.nrow())
        q.rows().forEach { it[name] shouldBe other[it.index][name] }
        q.rows().forEach { it[heightOrNull] shouldBe other[it.index][height] }
    }

    @Test
    fun `row to frame`() {
        typed[1].toDataFrame().name.size() shouldBe 1
    }

    @Test
    fun `compare comparable`() {
        val new = df.add("date") { LocalDate.now().minusDays(index.toLong()) }
        val date by column<LocalDate>()
        new.filter { date >= LocalDate.now().minusDays(3) }.nrow() shouldBe 4
    }

    @Test
    fun `union dataframes with different type of the same column`() {
        val df2 = dataFrameOf("age")(32.6, 56.3, null)
        df2["age"].type() shouldBe getType<Double?>()
        val merged = df.union(df2)
        merged["age"].type() shouldBe getType<Number?>()
        val updated = merged.update("age") { "age"<Number?>()?.toDouble() }
        updated["age"].type() shouldBe getType<Double?>()
    }

    @Test
    fun `distinct`() {
        typed.select { name and city }.distinct().nrow() shouldBe 6
        val d = typed.distinct { name and city }
        d.nrow() shouldBe 6
        d.ncol() shouldBe 2
    }

    @Test
    fun `distinct by`() {
        typed.distinctBy { name }.nrow() shouldBe 3
        typed.distinctBy { name and city }.nrow() shouldBe 6
        typed.distinctByExpr { age / 10 }.nrow() shouldBe 4
        typed.distinctBy { age / 10 }.nrow() shouldBe 4
        typed.distinctByExpr { city?.get(0) }.nrow() shouldBe 5
    }

    @Test
    fun `addRow`() {
        val res = typed.append("Bob", null, "Paris", null)
        res.nrow() shouldBe typed.nrow() + 1
        res.name.type() shouldBe getType<String>()
        res.age.type() shouldBe getType<Int?>()
        res.city.type() shouldBe getType<String?>()
        res.weight.type() shouldBe getType<Int?>()

        val row = res.last()
        row.name shouldBe "Bob"
        row["age"] shouldBe null
        row.city shouldBe "Paris"
        row.weight shouldBe null
    }

    @Test
    fun `rename`() {

        fun AnyFrame.check() {
            this["name2"].toList() shouldBe typed.name.toList()
            this["age2"].toList() shouldBe typed.age.toList()
            this.columnNames() shouldBe listOf("name2", "age2", "city", "weight")
            this.tryGetColumn("age") shouldBe null
        }
        typed.rename("name" to "name2", "age" to "age2").check()
        typed.rename { name and age }.into("name2", "age2").check()
        typed.rename { name and age }.into { it.name + "2" }.check()
    }

    @Test
    fun `select with rename`() {

        val expected = typed.select { name and age }.rename { all() }.into { it.name + 2 }
        typed.select { name into "name2" and age.into("age2") } shouldBe expected
        df.select { name("name2") and age("age2") } shouldBe expected
    }

    @Test
    fun `nunique`() {
        typed.name.ndistinct() shouldBe 3
    }

    @Test
    fun `encode names`() {
        val encoding = typed.name.distinct().addRowNumber("name_id")
        val res = typed.leftJoin(encoding)
        res["name_id"].toList() shouldBe listOf(0, 1, 2, 2, 1, 0, 2)
    }

    @Test
    fun `pivot matches`() {
        val pivoted = typed.pivot { city }.groupBy { name and age and weight }.matches()
        pivoted.ncol() shouldBe typed.ncol() + typed.city.ndistinct() - 1

        for (row in 0 until typed.nrow()) {
            val city = typed[row][city].toString()
            pivoted[row][city] shouldBe true
            for (col in typed.ncol() until pivoted.ncol()) {
                val column = pivoted.column(col)
                val pivotedValue = column.typed<Boolean>()[row]
                val colName = column.name()
                pivotedValue shouldBe (colName == city)
            }
        }
    }

    @Test
    fun `pivot matches equality`() {
        val res1 = typed.pivot { city }.groupBy {name}.matches()
        val res2 = typed.groupBy { name }.pivot { city }.matches()
        val res3 = typed.groupBy { name }.aggregate {
            pivot { city }.matches()
        }
        res2 shouldBe res1
        res3 shouldBe res1
    }

    @Test
    fun `pivot to bool with conversion`() {
        val filtered = typed.dropNulls { city }
        val res = filtered.pivot { city.lowercase() }.groupBy { name and age }.matches()
        val cities = filtered.city.toList().map {it!!.lowercase()}
        val gathered =
            res.gather { colsOf<Boolean> { cities.contains(it.name()) } }.where { it }.into("city")
        val expected = filtered.select { name and age and city.map { it!!.lowercase()} }.moveToRight { city }
        gathered shouldBe expected
    }

    @Test
    fun `pivot to bool distinct rows`() {
        val res = typed.pivot { city }.groupBy { name and age }.matches()
        res.ncol() shouldBe 2 + typed.city.ndistinct()
        for (i in 0 until typed.nrow()) {
            val city = typed[i][city]
            for (j in typed.ncol() until res.ncol()) {
                res.column(j).typed<Boolean>().get(i) shouldBe (res.column(j).name() == city.toString())
            }
        }
    }

    @Test
    fun `pivot to bool merged rows`() {
        val selected = typed.select { name and city }
        val res = typed.pivot { city }.groupBy { name }.matches()

        res.ncol() shouldBe selected.city.ndistinct() + 1
        res.nrow() shouldBe selected.name.ndistinct()
        val trueValuesCount = res.columns().drop(1).sumOf { it.typed<Boolean>().toList().count { it } }
        trueValuesCount shouldBe selected.distinct().nrow()

        val pairs = (1 until res.ncol()).flatMap { i ->
            val col = res.column(i).typed<Boolean>()
            res.filter { it[col] }.map { name to col.name() }
        }.toSet()

        pairs shouldBe typed.map { name to city.toString() }.toSet()
    }

    @Test
    fun `pivot to matrix`() {

        val others by column<Many<String>>("other")
        val other by column<String>()
        val sum by column<Int>()

        val names = typed.name.distinct().toMany()

        val src = typed.select { name }
            .add(others) { names }
            .split { others }.intoRows()
            .add(sum) { name.length + other().length }

        val matrix = src.pivot { other }.groupBy {name }.with { sum }
        matrix.ncol() shouldBe 1 + names.size
    }

    @Test
    fun `gather bool`() {
        val pivoted = typed.pivot { city }.groupBy {name}.matches()
        val res = pivoted.gather { colsOf<Boolean>() }.where { it }.into("city")
        val sorted = res.sortBy { name and city }
        sorted shouldBe typed.select { name and city.map {it.toString()} }.distinct().sortBy { name and city }
    }

    @Test
    fun `merge rows keep nulls`() {

        val merged = typed.select { name and city }.mergeRows(dropNulls = false) { city }

        val cityList by column<Many<String?>>().named("city")
        merged[cityList].sumOf { it.size } shouldBe typed.city.size
        merged[cityList].type() shouldBe getType<Many<String?>>()

        val expected = typed.groupBy { name }.aggregate { it.city.toSet() into "city" }
        val actual = merged.update { cityList }.with { it.toSet() }

        actual shouldBe expected

        // check that default value for 'dropNulls' is false
        typed.select { name and city }.mergeRows { city } shouldBe merged
    }

    @Test
    fun `merge rows drop nulls`() {

        val merged = typed.select { name and city }.mergeRows(dropNulls = true) { city }

        val cityList by column<Many<String>>().named("city")
        merged[cityList].sumOf { it.size } shouldBe typed.city.dropNulls().size
        merged[cityList].type() shouldBe getType<Many<String>>()

        val expected = typed.dropNulls {city}.groupBy { name }.aggregate { it.city.toSet() as Set<String> into "city" }
        val actual = merged.update { cityList }.with { it.toSet() }

        actual shouldBe expected
    }

    @Test
    fun splitRows() {
        val selected = typed.select { name and city }
        val nested = selected.mergeRows(dropNulls = false) { city }
        val mergedCity by columnList<String?>("city")
        val res = nested.split { mergedCity }.intoRows()
        res.sortBy { name } shouldBe selected.sortBy { name }
    }

    @Test
    fun mergeCols() {
        val merged = typed.merge { age and city and weight }.into("info")
        merged.ncol() shouldBe 2
        merged.nrow() shouldBe typed.nrow()
        for (row in 0 until typed.nrow()) {
            val list = merged[row]["info"] as List<Any?>
            list.size shouldBe 3
            list[0] shouldBe typed.age[row]
            list[1] shouldBe typed.city[row]
            list[2] shouldBe typed.weight[row]
        }
    }

    @Test
    fun joinColsToString() {
        val merged = typed.merge { age and city and weight }.by(", ").into("info")
        merged.ncol() shouldBe 2
        merged.nrow() shouldBe typed.nrow()
        for (row in 0 until typed.nrow()) {
            val joined = merged[row]["info"] as String
            joined shouldBe typed.age[row].toString() + ", " + typed.city[row] + ", " + typed.weight[row]
        }
    }

    @Test
    fun splitCol() {
        val merged = typed.merge { age and city and weight }.into("info")
        val info by columnList<Any>()
        val res = merged.split(info).into("age", "city", "weight")
        res shouldBe typed
    }

    @Test
    fun splitStringCol() {
        val merged = typed.merge { age and city and weight }.by(" - ").into("info")
        val info by column<String>()
        val res = merged.split { info }.by("-", trim = true).into("age", "city", "weight")
        val expected = typed.update { age and city and weight }.with { it.toString() }
        res shouldBe expected
    }

    @Test
    fun splitStringCol2() {
        val merged = typed.merge { age and city and weight }.by(",").into("info")
        val info by column<String>()
        val res = merged.split(info).into("age", "city", "weight")
        val expected = typed.update { age and city and weight }.with { it.toString() }
        res shouldBe expected
    }

    @Test
    fun splitStringColGenerateNames() {
        val merged = typed.merge { age and city and weight }.by(",").into("info")
        val info by column<String>()
        val res = merged.split(info).into("age") { "extra$it" }
        res.columnNames() shouldBe listOf("name", "age", "extra1", "extra2")
    }

    @Test
    fun splitStringColWithDefaultgenerator() {
        val merged = typed.merge { age and city and weight }.by(",").into("info")
        val info by column<String>()
        val res = merged.split(info).into("age")
        res.columnNames() shouldBe listOf("name", "age", "splitted1", "splitted2")
    }

    @Test
    fun splitAgeIntoDigits() {

        fun digits(num: Int) = sequence {
            var k = num
            while (k > 0) {
                yield(k % 10)
                k /= 10
            }
        }.toList()

        val res = typed.split { age }.by { digits(it) }.into { "digit$it" }
    }

    @Test
    fun splitStringCol3() {
        val merged = typed.merge { age and city and weight }.by(", ").into("info")
        val info by column<String?>()
        val res = merged.split { info }.by(",").into("age", "city", "weight")
        val expected = typed.update { age and city and weight }.with { it.toString() }
        res shouldBe expected
    }

    @Test
    fun splitStringCols() {
        val merged = typed.merge { name and city }.by(", ").into("nameAndCity")
            .merge { age and weight }.into("info")
        val nameAndCity by column<String>()
        val info by columnList<Number?>()
        val res = merged.split { nameAndCity and info }.intoMany { src, _ ->
            when (src.name) {
                "nameAndCity" -> listOf("name", "city")
                else -> listOf("age", "weight")
            }
        }
        val expected = typed.update { city }.with { it.toString() }.move { city }.to(1)
        res shouldBe expected
    }

    @Test
    fun `merge cols with conversion`() {
        val pivoted = typed.groupBy { name }.pivot { city }.count()
        val res = pivoted.merge { intCols() }.by { it.filterNotNull().sum() }.into("cities")
        val expected = typed.select { name and city }.groupBy { name }.count("cities")
        res shouldBe expected
    }

    @Test
    fun `generic column type`() {
        val d = typed.update { city }.with { it?.toCharArray()?.toList() ?: emptyList() }
        println(d.city.type())
    }

    @Test
    fun `column group by`() {

        fun DataFrame<Person>.check() {
            ncol() shouldBe 3
            nrow() shouldBe typed.nrow()
            columnNames() shouldBe listOf("name", "Int", "String")
            val intGroup = this["Int"].asFrame()
            intGroup.columnNames() shouldBe listOf("age", "weight")

            val res = listOf(
                this.name,
                this["Int"]["age"],
                this["String"]["city"],
                this["Int"]["weight"]
            ).asDataFrame<Person>()
            res shouldBe typed
        }
        typed.group { cols { it != name } }.into { type.jvmErasure.simpleName!! }.check()
        typed.group { age and city and weight }.into { type.jvmErasure.simpleName!! }.check()
    }

    @Test
    fun `column group`() {

        val grouped = typed.move { age and name and city }.under("info")
        grouped.ncol() shouldBe 2
        grouped.columnNames() shouldBe listOf("info", "weight")
        val res = listOf(
            grouped["info"]["name"],
            grouped["info"]["age"],
            grouped["info"]["city"],
            grouped.weight
        ).asDataFrame<Person>()
        res shouldBe typed
    }

    @Test
    fun `column ungroup`() {

        val info by columnGroup()
        val res = typed.move { age and city }.under("info").ungroup { info }
        res shouldBe typed
    }


    @Test
    fun `empty group by`() {
        val ungrouped = typed.filter { false }.groupBy { name }.ungroup()
        ungrouped.nrow() shouldBe 0
        ungrouped.ncol() shouldBe 0
    }

    @Test
    fun `column stats`() {
        typed.age.mean() shouldBe typed.age.toList().mean()
        typed.age.min() shouldBe typed.age.toList().minOrNull()
        typed.age.max() shouldBe typed.age.toList().maxOrNull()
        typed.age.sum() shouldBe typed.age.toList().sum()
    }

    @Test
    fun `row to string`() {
        typed[0].toString() shouldBe "{ name:Alice, age:15, city:London, weight:54 }"
    }

    @Test
    fun `range slice`() {
        typed[3..5].name.toList() shouldBe typed.name.toList().subList(3, 6)
    }

    @Test
    fun `range slice two times`() {
        typed[3..5][1..2].name.toList() shouldBe typed.name.toList().subList(4, 6)
    }

    @Test
    fun `move to position`() {

        typed.column(1) shouldBe typed.age
        val moved = typed.move { age }.to(2)
        moved.column(2) shouldBe typed.age
        moved.ncol() shouldBe typed.ncol()
    }

    @Test
    fun `forEachIn`() {

        val cities by columnGroup()
        val pivoted = typed.pivot { city }.groupBy { name and weight }.withGrouping(cities).with { age }
        pivoted.print()
        var sum = 0
        pivoted.forEachIn({ cities.children() }) { row, column -> column[row]?.let { sum += it as Int } }
        sum shouldBe typed.age.sum()
    }

    @Test
    fun `parse`() {

        val toStr = typed.update { weight }.notNull { it.toString() }
        val weightStr = "weight".toColumnOf<String?>()
        val parsed = toStr.convert { weightStr }.toInt()
        parsed shouldBe typed
    }

    @Test
    fun digitize() {

        val a = 20
        val b = 40
        val expected = typed.age.toList().map {
            when {
                it < a -> 0
                it < b -> 1
                else -> 2
            }
        }
        typed.age.digitize(a, b).toList() shouldBe expected

        val expectedRight = typed.age.toList().map {
            when {
                it <= a -> 0
                it <= b -> 1
                else -> 2
            }
        }
        typed.age.digitize(a, b, right = true).toList() shouldBe expectedRight
    }

    @Test
    fun corr() {
        val fixed = typed.fillNulls { weight }.with(60)
        val res = fixed.corr()
        res.ncol() shouldBe 3
        res.nrow() shouldBe 2
        res["age"][0] shouldBe 1.0
        res["weight"][0] shouldBe res["age"][1]
        res["weight"][0] as Double should ToleranceMatcher(0.9, 1.0)
    }

    @Test
    fun `aggregate into grouped column`() {

        val d = typed.groupBy { name }.aggregate {
            val row = meanFor { age and weight }
            row into "mean"
        }
        d.ncol() shouldBe 2
        d["mean"].isGroup() shouldBe true
        val mean = d.getColumnGroup("mean")
        mean.ncol() shouldBe 2
        mean.columnNames() shouldBe listOf("age", "weight")
        mean.columns().forEach {
            it.type() shouldBe getType<Double>()
        }
    }

    @Test
    fun `mean for all columns`() {

        val d = typed.groupBy { name }.mean()
        d.columnNames() shouldBe listOf("name", "age", "weight")
        d.nrow() shouldBe typed.name.ndistinct()
        d["age"].type() shouldBe getType<Double>()
        d["weight"].type() shouldBe getType<Double>()
    }

    @Test
    fun `aggregate into table column`() {

        val d = typed.groupBy { name }.aggregate {
            val row = select { age and weight }
            row into "info"
        }
        d.ncol() shouldBe 2
        d["info"].isTable() shouldBe true
        val info = d.frameColumn("info")
        info.forEach {
            it!!.ncol() shouldBe 2
            it.columnNames() shouldBe listOf("age", "weight")
            it.columns().forEach {
                it.typeClass shouldBe Int::class
            }
        }
    }

    @Test
    fun `union table columns`() {

        val grouped = typed.addRowNumber("id").groupBy { name }.plain()
        val dfs = (0 until grouped.nrow()).map {
            grouped[it..it]
        }
        val dst = dfs.union().toGrouped().ungroup().sortBy("id").remove("id")
        dst shouldBe typed
    }

    @Test
    fun `set column`() {

        val copy = typed.select { all() }
        copy["new"] = copy.age
        copy.ncol() shouldBe typed.ncol() + 1
        copy["new"].toList() shouldBe typed.age.toList()
    }

    @Test
    fun `columns sum`() {

        val name by columnOf("Alice", "Bob", "Mark")
        val age by columnOf(15, 20, 24)
        val df = name + age

        df.columnNames() shouldBe listOf("name", "age")
        df.nrow() shouldBe 3
    }

    @Test
    fun convert1() {

        val res = typed.convert { age }.to<Double>()
        res.age.typeClass shouldBe Double::class
        res["age"].all { it is Double } shouldBe true
    }

    @Test
    fun convert2() {

        val res = typed.convert { weight }.to<BigDecimal>()
        res.weight.typeClass shouldBe BigDecimal::class
        res["weight"].all { it == null || it is BigDecimal } shouldBe true
    }

    @Test
    fun convert3() {

        val res = typed.convert { all() }.to<String>()
        res.columns().forEach { it.typeClass shouldBe String::class }
        res.columns().map { it.hasNulls() } shouldBe typed.columns().map { it.hasNulls() }
    }

    @Test
    fun convertToDate() {

        val time by columnOf("2020-01-06", "2020-01-07")
        val df = dataFrameOf(time)
        val casted = df.convert(time).toDate()
        casted[time].type() shouldBe getType<LocalDate>()
    }

    @Test
    fun replace() {

        val res = typed.replace { age }.with(2021 - typed.age)
        val expected = typed.update { age }.with { 2021 - age }
        res shouldBe expected
    }

    @Test
    fun `replace with rename`() {

        val res = typed.replace { age }.with { it.rename("age2") }
        res shouldBe typed.rename { age }.into("age2")
    }

    @Test(expected = IllegalArgumentException::class)
    fun `replace exception`() {
        typed.replace { intCols() }.with(typed.name)
    }

    @Test
    fun `replace two columns`() {
        val res = typed.replace { age and weight }.with(typed.age * 2, typed.weight * 2)
        val expected = typed.update { age and weight }.with { it?.times(2) }
        res shouldBe expected
    }

    @Test
    fun `replace with expression`() {

        val res = typed.replace { age }.with { 2021 - age named "year" }
        val expected = typed.update { age }.with { 2021 - age }.rename { age }.into("year")
        res shouldBe expected
    }

    @Test
    fun `add dataframe`() {
        val first = typed.select { name and age }
        val second = typed.select { city and weight }
        first.add(second) shouldBe typed
        first.add(second.columns()) shouldBe typed
        first + second.columns() shouldBe typed
    }

    @Test
    fun splitUnequalLists() {
        val values by columnOf(1, 2, 3, 4)
        val list1 by columnOf(manyOf(1, 2, 3), manyOf(), manyOf(1, 2), null)
        val list2 by columnOf(manyOf(1, 2), manyOf(1, 2), manyOf(1, 2), manyOf(1))
        val df = dataFrameOf(values, list1, list2)
        val res = df.explode { list1 and list2 }
        val expected = dataFrameOf(values.name(), list1.name(), list2.name())(
            1, 1, 1,
            1, 2, 2,
            1, 3, null,
            2, null, 1,
            2, null, 2,
            3, 1, 1,
            3, 2, 2,
            4, null, 1
        )
        res shouldBe expected
    }

    @Test
    fun splitUnequalListAndFrames() {
        val values by columnOf(1, 2, 3)
        val list1 by columnOf(manyOf(1, 2, 3), manyOf(1), manyOf(1, 2))
        val frames by listOf(manyOf(1, 2), manyOf(1, 2), manyOf(1, 2)).map {
            val data = column("data", it)
            val dataStr = column("dataStr", it.map { it.toString() })
            dataFrameOf(data, dataStr)
        }.toColumn()

        val df = dataFrameOf(values, list1, frames)
        val res = df.explode { list1 and frames }.ungroup(frames)
        val expected = dataFrameOf(values.name(), list1.name(), "data", "dataStr")(
            1, 1, 1, "1",
            1, 2, 2, "2",
            1, 3, null, null,
            2, 1, 1, "1",
            2, null, 2, "2",
            3, 1, 1, "1",
            3, 2, 2, "2"
        )
        res shouldBe expected
    }

    @Test
    fun `update nullable column with not null`() {
        val df = dataFrameOf("name", "value")("Alice", 1, null, 2)
        df.update("name").at(0).with("ALICE")
    }

    @Test
    fun `mean all columns`() {
        typed.mean().values() shouldBe listOf(typed.age.mean(), typed.weight.mean())
    }

    @Test
    fun `mean by string`() {
        typed.mean("weight") shouldBe typed.weight.mean()
    }

    @Test
    fun `create column with single string value`() {
        val frameCol by columnOf(typed, null, typed)
        frameCol.kind() shouldBe ColumnKind.Frame
        frameCol.name() shouldBe "frameCol"

        val mapCol by columnOf(typed.name, typed.city)
        mapCol.kind() shouldBe ColumnKind.Group
        mapCol.name() shouldBe "mapCol"

        val valueCol = columnOf("Alice") named "person"
        valueCol.kind() shouldBe ColumnKind.Value
        valueCol.name() shouldBe "person"
    }

    @Test
    fun `append many`() {
        val res = typed.append(
            "John", 22, "New York", 46,
            "Philip", 25, "Chelyabinsk", 36
        )
        res.nrow shouldBe typed.nrow + 2
    }

    @Test
    fun `first last`() {
        typed.first() shouldBe typed[0]
        typed.last() shouldBe typed[typed.nrow - 1]
        typed.city.first() shouldBe typed[0].city
        typed.city.last() shouldBe typed[typed.nrow - 1].city
    }

    @Test
    fun `select several rows`() {
        df[2, 5].nrow() shouldBe 2
        df[0, 3, 5] shouldBe df[listOf(0, 3, 5)]
        df[3, 5][name, age] shouldBe df[name, age][3, 5]
    }

    @Test
    fun `select several column values`() {
        typed.name[2, 5, 6] shouldBe typed.name.slice(listOf(2, 5, 6))
    }

    @Test
    fun `get by column accessors`() {
        val animal by columnOf("cat", "snake", "dog")
        val age by columnOf(2.5, 3.0, 0.5)
        val visits by columnOf(1, 3, 2)

        val df = dataFrameOf(animal, age, visits)

        val d1 = df[1..1][animal, age]
        d1.ncol() shouldBe 2
        d1.nrow() shouldBe 1

        val d2 = df[0..1]["animal", "age"]
        d2.ncol() shouldBe 2
        d2.nrow() shouldBe 2

        val r1 = df[1][animal, age]
        r1.values() shouldBe d1.single().values()

        val r2 = df[0]["animal", "age"]
        r2 shouldBe df[animal, age][0]
    }

    @Test
    fun between() {

        typed.filter { age.between(20, 40, false) }.nrow() shouldBe 2

        typed.filter { age in 20..40 }.nrow() shouldBe 5

        typed.age.between(20, 40).count { it } shouldBe 5
    }

    @Test
    fun iterators() {

        var counter = 0
        for (a in df) counter++
        counter shouldBe df.nrow()

        var ageSum = 0
        for (a in typed.age)
            ageSum += a

        ageSum shouldBe typed.age.sum()
    }

    @Test
    fun `create with random`() {
        val df = dataFrameOf('a'..'f').randomInt(3)
        df.nrow() shouldBe 3
        df.ncol() shouldBe ('a'..'f').count()
        df.columns().forEach { it.type() shouldBe getType<Int>() }
    }

    @Test
    fun `create with list builder`() {
        val df = dataFrameOf(4..10 step 2) { h -> List(10) { h } }
        df.nrow() shouldBe 10
        df.ncol() shouldBe 4
        df.columns().forEach { col -> col.forEach { it shouldBe col.name().toInt() } }
    }

    @Test
    fun `create with vararg header and builder`() {
        val df = dataFrameOf("first", "secon", "third") { name -> name.toCharArray().toList() }
        df.nrow() shouldBe 5
        df.ncol() shouldBe 3
        df.columns().forEach { col -> col.name() shouldBe col.values().joinToString("") }
    }

    @Test
    fun `create with vararg doubles and fill equal`() {
        val df = dataFrameOf(1.0, 2.5).fill(5, true)
        df.nrow() shouldBe 5
        df.ncol() shouldBe 2
        df.columns().forEach { col -> col.forEach { it shouldBe true } }
    }

    @Test
    fun `create with list of names and fill nulls`() {
        val names = listOf("first", "second")
        val df = dataFrameOf(names).nulls<Double>(10)
        df.nrow() shouldBe 10
        df.ncol() shouldBe 2
        df.columns().forEach { col -> (col.type() == getType<Double?>() && col.allNulls()) shouldBe true }
    }

    @Test
    fun `create with list of names and fill true`() {
        val first by column<Boolean>()
        val second by column<Boolean>()
        val df = dataFrameOf(first, second).fill(5) { true }
        df.nrow() shouldBe 5
        df.ncol() shouldBe 2
        df.columns().forEach { col -> (col.type() == getType<Boolean>() && col.all { it == true }) shouldBe true }
    }

    @Test
    fun `create with int range header and int range data `() {
        val df = dataFrameOf(1..5) { 1..5 }
        df.nrow() shouldBe 5
        df.ncol() shouldBe 5
        df.columns().forEach { col -> col.forEachIndexed { row, value -> value shouldBe row + 1 } }
    }

    @Test
    fun `get typed column by name`() {
        val col = df.getColumn<String>("name")
        col[0].substring(0, 3) shouldBe "Ali"
    }

    @Test
    fun `select all after`() {
        typed.select { allAfter(age) } shouldBe typed.select { city and weight }
        typed.select { allSince(age) } shouldBe typed.select { age and city and weight }
        typed.select { allBefore(age) } shouldBe typed.select { name }
        typed.select { allUntil(age) } shouldBe typed.select { name and age }
    }

    @Test
    fun `cols of type`() {
        val stringCols = typed.select { colsOf<String?>() }
        stringCols.columnNames() shouldBe listOf("name", "city")
    }

    @Test
    fun neighbours() {
        typed[2].neighbours(-1..1).toList() shouldBe listOf(typed[1], typed[2], typed[3])
    }

    @Test
    fun `get row value by selector`() {
        val selector: RowSelector<Person, Int> = { it.age * 2 }
        val added = typed.add("new") { it[selector] }
        added shouldBe typed.add("new") { age * 2 }
    }

    @Test
    fun `render nested data frames to string`() {
        val rendered = typed.drop(1).groupBy { name }.groups.asIterable().joinToString("\n") { renderValueForStdout(it) }
        rendered shouldBe """
            [2 x 4]
            [3 x 4]
            [1 x 4] { name:Alice, age:20, weight:55 }
        """.trimIndent()
    }

    @Test
    fun `drop where any na`(){
        val updated = typed.update { weight }.with { if(name=="Alice") Double.NaN else it?.toDouble() }
        val expected = updated.count { city != null && !(ndouble("weight")?.isNaN() ?: true) }

        fun AnyFrame.check() = nrow() shouldBe expected

        updated.dropNa { city and weight }.check()
        updated.dropNa(city, weight).check()
        updated.dropNa("city", "weight").check()
        updated.dropNa(Person::city, Person::weight).check()
    }

    @Test
    fun `drop where all na`(){
        val updated = typed.update { weight }.with { if(name=="Alice") Double.NaN else it?.toDouble() }
        val expected = updated.count { city != null || !(ndouble("weight")?.isNaN() ?: true) }

        fun AnyFrame.check() = nrow() shouldBe expected

        updated.dropNa(whereAllNa = true) { city and weight }.check()
        updated.dropNa(city, weight, whereAllNa = true).check()
        updated.dropNa("city", "weight", whereAllNa = true).check()
        updated.dropNa(Person::city, Person::weight, whereAllNa = true).check()
    }

    @Test
    fun sortWith() {
        typed.sortWith { r1, r2 -> when{
            r1.name < r2.name -> -1
            r1.name > r2.name -> 1
            else -> -r1.age.compareTo(r2.age)
        } } shouldBe typed.sortBy { name and age.desc }

        val comparator = Comparator<DataRow<Person>> { r1, r2 -> -r1.name.compareTo(r2.name)}
        typed.sortWith(comparator) shouldBe typed.sortByDesc { name }
    }

    @Test
    fun sortByDescDesc() {
        typed.sortByDesc { name.desc and age } shouldBe typed.sortBy { name and age.desc }
    }

    @Test
    fun `get column by columnRef with data`() {
        val col by columnOf(1,2,3)
        val df = col.toDataFrame()
        df[1..2][col].values() shouldBe listOf(2,3)
    }

    @Test
    fun `get by column`() {
        typed[1..2][{ typed.age }].size() shouldBe typed.age.size()
    }

    @Test
    fun `null column test`() {
        val df = dataFrameOf("col")(null, null)
        df["col"].kind() shouldBe ColumnKind.Value
        df["col"].type() shouldBe getType<Any?>()
    }

    @Test
    fun `groupBy with map`() {
        typed.groupBy { name.map { it.lowercase() } }.plain().name.values() shouldBe typed.name.distinct().lowercase().values()
    }

    @Test
    fun `groupBy none`() {
        val grouped = typed.groupBy { none() }
        grouped.keys.ncol shouldBe 0
        grouped.groups.size shouldBe 1
        val values = grouped.values()
        values.nrow shouldBe 1
        values.columns().forEach {
            it.typeClass shouldBe Many::class
            (it[0] as Many<*>).size shouldBe typed.nrow()
        }
        values.explode() shouldBe typed
    }

    @Test
    fun `pivot max`() {
        val pivoted = typed.pivot { city }.groupBy { name }.max { age }
        pivoted.single { name == "Mark" }["Moscow"] shouldBe 30
    }

    @Test
    fun `pivot all values`() {
        val pivoted = typed.pivot { city }.groupBy { name }.values()
        pivoted.ncol shouldBe 1 + typed.city.ndistinct()
        pivoted.columns().drop(1).forEach {
            it.kind() shouldBe ColumnKind.Group
            it.asGroup().columnNames() shouldBe listOf("age", "weight")
        }
    }

    @Test
    fun `pivot mean values`() {
        val pivoted = typed.pivot { city }.groupBy { name }.mean()
        pivoted.columns().drop(1).forEach {
            it.kind() shouldBe ColumnKind.Group
            val group = it.asGroup()
            group.columnNames() shouldBe listOf("age", "weight")
            group.columns().forEach {
                it.type() shouldBe getType<Double?>()
            }
        }
    }

    @Test
    fun `aggregate dataframe with pivot`(){
        val summary = typed.aggregate {
            count() into "count"
            pivot { name }.max { age }
            sum { weight } into "total weight"
        }
        val expected = dataFrameOf("count", "Alice", "Bob", "Mark", "total weight")(7, 20, 45, 40, 354)[0]
        summary shouldBe expected
    }

    @Test
    fun `pivot grouped max`() {

        val pivoted = typed.pivot{ name }.groupBy{ city }.max()
        pivoted.columns().drop(1).forEach {
            it.kind() shouldBe ColumnKind.Group
            val group = it.asGroup()
            group.columnNames() shouldBe listOf("age", "weight")
        }
    }

    @Test
    fun `find the longest string`() {
        val longestCityName = "Taumatawhakatangihangakoauauotamateaturipukakapikimaungahoronukupokaiwhenuakitanatahu"
        val updated = typed.update { city }.where { it == "Dubai" }.with(longestCityName)
        updated.valuesNotNull { stringCols() }.maxByOrNull { it.length } shouldBe longestCityName
    }
}