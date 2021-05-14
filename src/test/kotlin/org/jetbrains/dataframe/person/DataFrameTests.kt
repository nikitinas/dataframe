package org.jetbrains.dataframe.person

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.doubles.ToleranceMatcher
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import org.jetbrains.dataframe.*
import org.jetbrains.dataframe.columns.named
import org.jetbrains.dataframe.columns.typeClass
import org.jetbrains.dataframe.impl.columns.isTable
import org.jetbrains.dataframe.impl.columns.typed
import org.jetbrains.dataframe.impl.trackColumnAccess
import org.jetbrains.dataframe.io.print
import org.junit.Test
import java.math.BigDecimal
import java.time.LocalDate
import kotlin.IllegalArgumentException
import kotlin.reflect.jvm.jvmErasure

class DataFrameTests : BaseTest() {

    @Test
    fun `create with columns`() {

        dataFrameOf("name", "age", "city", "weight")(df.columns()) shouldBe df

        dataFrameOf("name", "age", "city", "weight")(typed.name named "bla", typed.age named "", typed.city.rename("qq"), typed.weight.named("asda")) shouldBe df

        val c1 = typed.name.toList().toColumn()
        val c2 = typed.age.toList().toColumn()
        val c3 = typed.city.toList().toColumn()
        val c4 = typed.weight.toList().toColumn()

        dataFrameOf("name", "age", "city", "weight")(c1,c2,c3,c4) shouldBe df
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
    fun `create column reference`(){

        val name by column<String>()
        val col = name.withValues("Alice", "Bob")
        val df = col.toDataFrame()
        df.nrow shouldBe 2
        df.columnNames() shouldBe listOf("name")
    }

    @Test
    fun `add values to column reference`(){

        val name by column<String>()
        val values = listOf("Alice", "Bob")
        val col1 = name.withValues(values)
        val col2 = values.toColumn(name)
        col1 shouldBe col2
    }

    @Test
    fun `guess column type`(){
        val col by columnOf("Alice", 1, 3.5)
        col.type() shouldBe getType<Any>()
        val filtered = col.filter { it is String }
        filtered.type() shouldBe getType<Any>()
        filtered.guessType().type() shouldBe getType<String>()
    }

    @Test
    fun `create from map`(){
        val data = mapOf("name" to listOf("Alice", "Bob"), "age" to listOf(15,null))
        val df = data.toDataFrame()
        df.ncol() shouldBe 2
        df.nrow() shouldBe 2
        df.columnNames() shouldBe listOf("name", "age")
        df["name"].type() shouldBe getType<String>()
        df["age"].type() shouldBe getType<Int?>()
    }

    @Test
    fun `toMap`(){
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

        val col = column<Int>().named("weight") // non-nullable column definition is incorrect here, because actual dataframe has nulls in this column

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
        typed.groupBy { name }.get("Mark") shouldBe typed.filter { name == "Mark" }
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
    fun `filterNotNull 1`() {

        fun AnyFrame.check() = rows().forEach { get("weight") shouldNotBe null }

        typed.filterNotNull(typed.weight).check()
        typed.filterNotNull { weight }.check()
        typed.filterNotNull { it.weight }.check()

        df.filterNotNull(weight).check()
        df.filterNotNull { weight }.check()

        df.filterNotNull("weight").check()
    }

    @Test
    fun `filterNotNull 2`() {

        val expected = typed.rows().count { it.city != null && it.weight != null }
        fun AnyFrame.check() = nrow() shouldBe expected

        typed.filterNotNull(typed.weight, typed.city).check()
        typed.filterNotNull { weight and city }.check()
        typed.filterNotNull { it.weight and it.city }.check()

        df.filterNotNull(Person::weight, Person::city).check()

        df.filterNotNull(weight, city).check()
        df.filterNotNull { weight and city }.check()

        df.filterNotNull("weight", "city").check()
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

        typed.groupBy { name and age }.print()

        typed.groupBy { name }.aggregate {
            nrow() into "n"
            count { age > 25 } into "old count"
            median { age } into "median age"
            min { age } into "min age"
            all { weight != null } into "all with weights"
            maxBy { age }.city into "oldest origin"
            sortBy { age }.first().city into "youngest origin"
            countBy { city } into { "from $it" }
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
            it.countBy { it.city } into { "from $it" }
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
            countBy(city) into { "from $it" }
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
            countBy(Person::city) into { "from $it" }
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
            countBy("city") into { "from $it" }
            it["age"].toList() into "ages"
        }.check()
    }

    @Test
    fun `groupBy invoked at column`() {

        typed.weight.groupBy(typed.name).mean() shouldBe typed.groupBy { name }.mean("weight") { weight }
    }

    @Test
    fun `min`() {
        val expected = 15

        fun Int?.check() = this shouldBe expected

        typed.min { age }.check()
        typed.min { it.age }.check()
        typed.age.min().check()

        df.min { age(it) }.check()
        df.min(age).check()
        df[age].min().check()

        df.min { int("age") }.check()
        df.min { "age"<Int>() }.check()
        df["age"].typed<Int>().min().check()
        (df.min("age") as Int?).check()
    }

    @Test
    fun `nullable max`() {
        val expected = 90

        fun Int?.check() = this shouldBe expected

        typed.max { weight }.check()
        typed.max { it.weight }.check()
        typed.weight.max().check()

        df.max { weight() }.check()
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

        typed.filterNotNull { weight }.minBy { weight!! }.check()
        typed.filterNotNull { it.weight }.minBy { it.weight!! }.check()

        df.filterNotNull(weight).minBy { weight()!! }.check()

        df.filterNotNull("weight").minBy { "weight"<Int>() }.check()
        df.filterNotNull("weight").minBy("weight").check()
    }

    @Test
    fun `maxBy`() {
        val expected = "Bob"

        fun AnyRow?.check() = this!![name] shouldBe expected

        typed.maxBy { age }.check()
        typed.maxBy { it.age }.check()
        typed.maxBy(typed.age).check()

        df.maxBy { age() }.check()
        df.maxBy(age).check()

        df.maxBy { "age"<Int>() }.check()
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
        res.rows().forEach { it.values shouldBe typed[it.index % typed.nrow()].values }
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
    }

    @Test
    fun `distinct by`() {
        typed.distinctBy { name }.nrow() shouldBe 3
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
    fun `spread exists`() {
        val spread = typed.spread { city }.into { it }
        spread.ncol() shouldBe typed.ncol() + typed.city.ndistinct() - 2

        for (row in 0 until typed.nrow()) {
            val city = typed[row][city]
            if (city != null) spread[row][city] shouldBe true
            for (col in typed.ncol() until spread.ncol()) {
                val column = spread.column(col)
                val spreadValue = column.typed<Boolean>()[row]
                val colName = column.name()
                spreadValue shouldBe (colName == city)
            }
        }
    }

    @Test
    fun `spread equality`() {
        val res1 = typed.select { name and city }.spread { city }.into { it }
        val res2 = typed.groupBy { name }.spread { city }.into { it }
        val res3 = typed.groupBy { name }.aggregate {
            spread { city } into { it }
        }
        res2 shouldBe res1
        res3 shouldBe res1
    }

    @Test
    fun `spread to bool with conversion`() {
        val res = typed.spread { city }.into { it?.decapitalize() }
        val cities = typed.city.toList().filterNotNull()
        val gathered =
            res.gather { colsOf<Boolean> { cities.contains(it.name().capitalize()) } }.where { it }.into("city")
        val expected = typed.update { city }.with { it?.decapitalize() }.moveToRight { city }
        gathered shouldBe expected
    }

    @Test
    fun `spread to bool distinct rows`() {
        val res = typed.spread { city }.into { it }
        res.ncol() shouldBe typed.ncol() + typed.city.ndistinct() - 2

        for (i in 0 until typed.nrow()) {
            val city = typed[i][city]
            if (city != null) res[i][city] == true
            for (j in typed.ncol() until res.ncol()) {
                res.column(j).typed<Boolean>().get(i) shouldBe (res.column(j).name() == city)
            }
        }
    }

    @Test
    fun `spread to bool merged rows`() {
        val selected = typed.select { name and city }
        val res = selected.spread { city }.into { it }

        res.ncol() shouldBe selected.city.ndistinct()
        res.nrow() shouldBe selected.name.ndistinct()
        val trueValuesCount = res.columns().drop(1).sumBy { it.typed<Boolean>().toList().count { it } }
        trueValuesCount shouldBe selected.filterNotNull { city }.distinct().nrow()

        val pairs = (1 until res.ncol()).flatMap { i ->
            val col = res.column(i).typed<Boolean>()
            res.filter { it[col] }.map { name to col.name() }
        }.toSet()

        pairs shouldBe typed.filter { city != null }.map { name to city!! }.toSet()
    }

    @Test
    fun `spread to matrix`() {

        val others by column<List<String>>("other")
        val other by column<String>()
        val sum by column<Int>()

        val names = typed.name.distinct().toList()

        val src = typed.select { name }
            .add(others) { names }
            .split { others }.intoRows()
            .add(sum) { name.length + other().length }

        val matrix = src.spread { other }.by { sum }.into { it }
        matrix.ncol() shouldBe 1 + names.size

        println(matrix)
    }

    @Test
    fun `gather bool`() {
        val selected = typed.select { name and city }
        val spread = selected.spread { city }.into { it }
        val res = spread.gather { colsOf<Boolean>() }.where { it }.into("city")
        val sorted = res.sortBy { name and city }
        sorted shouldBe selected.filterNotNull { city }.distinct().sortBy { name and city }
    }

    @Test
    fun mergeRows() {
        val selected = typed.select { name and city }
        val res = selected.mergeRows { city }
        val cityList by column<List<String?>>().named("city")
        val expected = selected.map { name to city }.groupBy({ it.first }) { it.second }.mapValues { it.value.toSet() }
        val actual = res.map { name to it[cityList] }.toMap().mapValues { it.value.toSet() }
        actual shouldBe expected
    }

    @Test
    fun splitRows() {
        val selected = typed.select { name and city }
        val nested = selected.mergeRows { city }
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
        res.print()
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
        val spread = typed.groupBy { name }.countBy { city }
        val res = spread.merge { colsOf<Int>() }.by { it.sum() }.into("cities")
        val expected = typed.select { name and city }.filter { city != null }.groupBy { name }.countInto("cities")
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
    fun `basic math`() {
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
        val spread = typed.spread { city }.by { age }.into(cities)
        var sum = 0
        spread.forEachIn({ cities.children() }) { row, column -> column[row]?.let { sum += it as Int } }
        sum shouldBe typed.age.sum()
    }

    @Test
    fun `parse`() {

        val toStr = typed.update { weight }.notNull { it.toString() }
        val weightStr = "weight".toColumnOf<String?>()
        val parsed = toStr.cast { weightStr }.toInt()
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
        res.print()
        res.ncol() shouldBe 3
        res.nrow() shouldBe 2
        res["age"][0] shouldBe 1.0
        res["weight"][0] shouldBe res["age"][1]
        res["weight"][0] as Double should ToleranceMatcher(0.9, 1.0)
    }

    @Test
    fun `aggregate into grouped column`() {

        val d = typed.groupBy { name }.aggregate {
            val row = select { age and weight }.mean()
            addValue("mean", row)
        }
        d.ncol() shouldBe 2
        d["mean"].isGroup() shouldBe true
        val mean = d.getGroup("mean")
        mean.ncol() shouldBe 2
        mean.columnNames() shouldBe listOf("age", "weight")
        mean.columns().forEach {
            it.type() shouldBe getType<Double>()
        }
    }

    @Test
    fun `aggregate into table column`() {

        val d = typed.groupBy { name }.aggregate {
            val row = select { age and weight }
            addValue("info", row)
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
    fun cast1() {

        val res = typed.cast { age }.to<Double>()
        res.age.typeClass shouldBe Double::class
        res["age"].all { it is Double } shouldBe true
    }

    @Test
    fun cast2() {

        val res = typed.cast { weight }.to<BigDecimal>()
        res.weight.typeClass shouldBe BigDecimal::class
        res["weight"].all { it == null || it is BigDecimal } shouldBe true
    }

    @Test
    fun cast3() {

        val res = typed.cast { all() }.to<String>()
        res.columns().forEach { it.typeClass shouldBe String::class }
        res.columns().map { it.hasNulls() } shouldBe typed.columns().map { it.hasNulls() }
    }

    @Test
    fun castToDate() {

        val time by columnOf("2020-01-06", "2020-01-07")
        val df = dataFrameOf(time)
        val casted = df.cast(time).toDate()
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
        val list1 by columnOf(listOf(1, 2, 3), listOf(), listOf(1, 2), null)
        val list2 by columnOf(listOf(1, 2), listOf(1, 2), listOf(1, 2), listOf(1))
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
        val list1 by columnOf(listOf(1, 2, 3), listOf(1), listOf(1, 2))
        val frames by listOf(listOf(1, 2), listOf(1, 2), listOf(1, 2)).map {
            val data = column("data", it)
            val dataStr = column("dataStr", it.map { it.toString() })
            data + dataStr
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
        typed.mean().values shouldBe listOf(typed.age.mean(), typed.weight.mean())
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
        val res = typed.append("John", 22, "New York", 46,
                    "Philip", 25, "Chelyabinsk", 36)
        res.nrow shouldBe typed.nrow + 2
    }

    @Test
    fun `first last`() {
        typed.first() shouldBe typed[0]
        typed.last() shouldBe typed[typed.nrow-1]
        typed.city.first() shouldBe typed[0].city
        typed.city.last() shouldBe typed[typed.nrow-1].city
    }

    @Test
    fun `select several rows`() {
        df[2, 5].nrow() shouldBe 2
        df[0,3,5] shouldBe df[listOf(0,3,5)]
        df[3,5][name, age] shouldBe df[name, age][3,5]
    }

    @Test
    fun `select several column values`() {
        typed.name[2,5,6] shouldBe typed.name.slice(listOf(2, 5, 6))
    }
}