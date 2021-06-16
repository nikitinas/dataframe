package org.jetbrains.dataframe.person

import io.kotest.assertions.fail
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import org.jetbrains.dataframe.AnyFrame
import org.jetbrains.dataframe.AnyRow
import org.jetbrains.dataframe.ColumnKind
import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.DataFrameBase
import org.jetbrains.dataframe.DataRow
import org.jetbrains.dataframe.DataRowBase
import org.jetbrains.dataframe.Many
import org.jetbrains.dataframe.add
import org.jetbrains.dataframe.after
import org.jetbrains.dataframe.annotations.DataSchema
import org.jetbrains.dataframe.append
import org.jetbrains.dataframe.at
import org.jetbrains.dataframe.by
import org.jetbrains.dataframe.colsDfsOf
import org.jetbrains.dataframe.column
import org.jetbrains.dataframe.columnGroup
import org.jetbrains.dataframe.columnList
import org.jetbrains.dataframe.columnOf
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.ColumnGroup
import org.jetbrains.dataframe.columns.toAccessor
import org.jetbrains.dataframe.dataFrameOf
import org.jetbrains.dataframe.distinct
import org.jetbrains.dataframe.dropNulls
import org.jetbrains.dataframe.duplicate
import org.jetbrains.dataframe.emptyDataFrame
import org.jetbrains.dataframe.emptyMany
import org.jetbrains.dataframe.explode
import org.jetbrains.dataframe.filter
import org.jetbrains.dataframe.forEach
import org.jetbrains.dataframe.frameColumn
import org.jetbrains.dataframe.getColumnPath
import org.jetbrains.dataframe.getType
import org.jetbrains.dataframe.group
import org.jetbrains.dataframe.groupBy
import org.jetbrains.dataframe.impl.codeGen.CodeGenerator
import org.jetbrains.dataframe.impl.codeGen.InterfaceGenerationMode
import org.jetbrains.dataframe.impl.codeGen.generate
import org.jetbrains.dataframe.impl.columns.asGroup
import org.jetbrains.dataframe.impl.columns.asTable
import org.jetbrains.dataframe.impl.columns.isTable
import org.jetbrains.dataframe.impl.columns.typed
import org.jetbrains.dataframe.index
import org.jetbrains.dataframe.insert
import org.jetbrains.dataframe.into
import org.jetbrains.dataframe.intoRows
import org.jetbrains.dataframe.inward
import org.jetbrains.dataframe.isEmpty
import org.jetbrains.dataframe.isGroup
import org.jetbrains.dataframe.join
import org.jetbrains.dataframe.map
import org.jetbrains.dataframe.mapNotNullGroups
import org.jetbrains.dataframe.max
import org.jetbrains.dataframe.mergeRows
import org.jetbrains.dataframe.minus
import org.jetbrains.dataframe.move
import org.jetbrains.dataframe.moveTo
import org.jetbrains.dataframe.moveToLeft
import org.jetbrains.dataframe.moveToRight
import org.jetbrains.dataframe.pivot
import org.jetbrains.dataframe.plus
import org.jetbrains.dataframe.print
import org.jetbrains.dataframe.remove
import org.jetbrains.dataframe.rename
import org.jetbrains.dataframe.select
import org.jetbrains.dataframe.sortBy
import org.jetbrains.dataframe.split
import org.jetbrains.dataframe.subcolumn
import org.jetbrains.dataframe.sumOf
import org.jetbrains.dataframe.toDefinition
import org.jetbrains.dataframe.toGrouped
import org.jetbrains.dataframe.toMany
import org.jetbrains.dataframe.toTop
import org.jetbrains.dataframe.typed
import org.jetbrains.dataframe.under
import org.jetbrains.dataframe.ungroup
import org.jetbrains.dataframe.update
import org.jetbrains.dataframe.with
import org.jetbrains.dataframe.with2
import org.jetbrains.dataframe.withIndex
import org.jetbrains.dataframe.withNull
import org.junit.Test

class DataFrameTreeTests : BaseTest() {

    @DataSchema
    interface NameAndCity {
        val name: String
        val city: String?
    }

    @DataSchema
    interface GroupedPerson {
        val nameAndCity: DataRow<NameAndCity>
        val age: Int
        val weight: Int?
    }

    val df2 = df.move { name and city }.under("nameAndCity")
    val typed2 = df2.typed<GroupedPerson>()

    val DataRowBase<NameAndCity>.name @JvmName("get-name-row") get() = this["name"] as String
    val DataRowBase<NameAndCity>.city @JvmName("get-city-row") get() = this["city"] as String?
    val DataFrameBase<NameAndCity>.name @JvmName("get-name") get() = this["name"].typed<String>()
    val DataFrameBase<NameAndCity>.city @JvmName("get-city") get() = this["city"].typed<String?>()

    val DataRowBase<GroupedPerson>.age @JvmName("get-age-row") get() = this["age"] as Int
    val DataRowBase<GroupedPerson>.weight @JvmName("get-weight-row") get() = this["weight"] as Int?
    val DataRowBase<GroupedPerson>.nameAndCity get() = this["nameAndCity"] as DataRowBase<NameAndCity>
    val DataFrameBase<GroupedPerson>.age @JvmName("get-age") get() = this["age"].typed<Int>()
    val DataFrameBase<GroupedPerson>.weight @JvmName("get-weight") get() = this["weight"].typed<Int?>()
    val DataFrameBase<GroupedPerson>.nameAndCity get() = this["nameAndCity"] as ColumnGroup<NameAndCity>

    val nameAndCity by columnGroup()
    val nameInGroup = nameAndCity.subcolumn<String>("name")

    @Test
    fun create() {
        val nameAndCity by columnOf(typed.name, typed.city)
        val df3 = nameAndCity + typed.age + typed.weight
        df3 shouldBe df2
    }

    @Test
    fun createFrameColumn() {
        val rowsColumn by columnOf(typed[0..3], typed[4..5], typed[6..6])
        val df = dataFrameOf(rowsColumn).toGrouped { rowsColumn }
        val res = df.ungroup()
        res shouldBe typed
    }

    @Test
    fun createFrameColumn2() {
        val id by column(typed.indices())
        val groups by id.map { typed[it..it] }
        val df = dataFrameOf(id, groups)
        df.nrow() shouldBe typed.nrow()
        df.forEach {
            val rowId = it[id]
            groups() shouldBe typed[rowId..rowId]
        }
    }

    @Test
    fun `select dfs under group`() {
        df2.select { nameAndCity.colsDfsOf<String>() } shouldBe typed2.select { nameAndCity.name }
        df2.select { nameAndCity.colsDfsOf<String?>() } shouldBe typed2.select { nameAndCity.name and nameAndCity.city }
    }

    @Test
    fun `selects`() {
        df2.select { nameAndCity.cols() } shouldBe typed2.nameAndCity.select { all() }
        df2.select { nameAndCity.cols { !it.hasNulls() } } shouldBe typed2.select { nameAndCity.name }
        df2.select { nameAndCity.cols(0..1) } shouldBe typed2.nameAndCity.select { all() }
        df2.select { nameAndCity.col(1) } shouldBe typed2.select { nameAndCity.city }
        df2.select { nameAndCity.col("city") } shouldBe typed2.select { nameAndCity.city }
        df2.select { nameAndCity.cols("city", "name") } shouldBe typed2.select { nameAndCity.city and nameAndCity.name }
        df2.select { nameAndCity.cols(name, city) } shouldBe typed2.select { nameAndCity.all() }
        df2.select { nameAndCity[name] } shouldBe typed2.nameAndCity.select { name }
        df2.select { nameAndCity.cols().drop(1) } shouldBe typed2.nameAndCity.select { city }

        typed2.select { nameAndCity.cols() } shouldBe typed2.nameAndCity.select { all() }
        typed2.select { nameAndCity.cols { !it.hasNulls() } } shouldBe typed2.select { nameAndCity.name }
        typed2.select { nameAndCity.cols(0..1) } shouldBe typed2.nameAndCity.select { all() }
        typed2.select { nameAndCity.col(1) } shouldBe typed2.select { nameAndCity.city }
        typed2.select { nameAndCity.col("city") } shouldBe typed2.select { nameAndCity.city }
        typed2.select {
            nameAndCity.cols(
                "city",
                "name"
            )
        } shouldBe typed2.select { nameAndCity.city and nameAndCity.name }
        typed2.select { nameAndCity.cols(name, city) } shouldBe typed2.select { nameAndCity.all() }
        typed2.select { nameAndCity[name] } shouldBe typed2.nameAndCity.select { name }
        typed2.select { nameAndCity.cols().drop(1) } shouldBe typed2.nameAndCity.select { city }

        df2.select { col(1) } shouldBe typed2.select { age }
        df2.select { nameInGroup } shouldBe typed2.nameAndCity.select { name }

        df2[nameInGroup] shouldBe typed2.nameAndCity.name
    }

    @Test
    fun getColumnPath() {
        typed2.getColumnPath { nameAndCity["city"] }.size shouldBe 2
        typed2.getColumnPath { nameAndCity.col("city") }.size shouldBe 2
        typed2.getColumnPath { nameAndCity.col(1) }.size shouldBe 2
    }

    @Test
    fun `group indexing`() {

        df2[nameAndCity][city] shouldBe typed.city
        typed2.nameAndCity.city shouldBe typed.city
        df2["nameAndCity"]["city"] shouldBe typed.city
    }

    @Test
    fun `update`() {
        val expected = typed.select { city.rename("nameAndCity") and age and weight }

        df2.update { nameAndCity }.with { it[city] } shouldBe expected
        df2.update { nameAndCity }.with { this[nameAndCity][city] } shouldBe expected
        typed2.update { nameAndCity }.with { nameAndCity.city } shouldBe expected
        typed2.update { nameAndCity }.with { it.city } shouldBe expected
    }

    @Test
    fun `slice`() {

        val expected = typed[0..2].name
        val actual = typed2[0..2].nameAndCity.name
        actual shouldBe expected
    }

    @Test
    fun `filter`() {

        val expected = typed.filter { city == null }.select { weight }
        typed2.filter { nameAndCity.city == null }.select { weight } shouldBe expected
        df2.filter { it[nameAndCity][city] == null }.select { weight } shouldBe expected
    }

    @Test
    fun `select`() {
        val expected = typed.select { name and age }
        typed2.select { nameAndCity.name and age } shouldBe expected
        df2.select { it[nameAndCity][name] and age } shouldBe expected
    }

    @Test
    fun `sort`() {
        val expected = typed.sortBy { name and age }.moveTo(1) { city }
        typed2.sortBy { nameAndCity.name and age }.ungroup { nameAndCity } shouldBe expected
    }

    @Test
    fun `move`() {

        val actual = typed2.move { nameAndCity.name }.into("name")
        actual.columnNames() shouldBe listOf("nameAndCity", "name", "age", "weight")
        actual.getColumnGroup("nameAndCity").columnNames() shouldBe listOf("city")
    }

    @Test
    fun `groupBy`() {

        val expected = typed.groupBy { name }.max { age }
        typed2.groupBy { nameAndCity.name }.max { age } shouldBe expected
    }

    @Test
    fun `distinct`() {

        val duplicated = typed2 + typed2
        duplicated.nrow() shouldBe typed2.nrow() * 2
        val dist = duplicated.nameAndCity.distinct()
        dist shouldBe typed2.nameAndCity.distinct()
        dist.nrow() shouldBe typed2.nrow() - 1
    }

    @Test
    fun selectDfs() {

        val cols = typed2.select { colsDfs { it.hasNulls } }
        cols shouldBe typed2.select { nameAndCity.city and weight }
    }

    @Test
    fun splitRows() {
        val selected = typed2.select { nameAndCity }
        val nested = selected.mergeRows(dropNulls = false) { nameAndCity.city }
        val mergedCity by columnList<String?>("city")
        val res = nested.split { nameAndCity[mergedCity] }.intoRows()
        val expected = selected.sortBy { nameAndCity.name }
        val actual = res.sortBy { nameAndCity.name }
        actual shouldBe expected
    }

    @Test
    fun pivot() {

        val modified = df.append("Alice", 55, "Moscow", 100)
        val df2 = modified.move { name and city }.under("nameAndCity")
        val typed2 = df2.typed<GroupedPerson>()

        val expected = modified.typed<Person>().groupBy { name and city }.map { key, group ->
            val value = if(key.city == "Moscow") group.age.toMany()
                        else group.age[0]
            (key.name to key.city.toString()) to value
        }.plus("Bob" to "Moscow" to emptyMany<Int>()).toMap()

        fun <T> DataFrame<T>.check() {
            print()
            ncol() shouldBe 1 + typed2.nameAndCity.city.ndistinct()
            this[name] shouldBe typed.name.distinct()
            val data = columns().drop(1)
            data.forEach {
                if (it.name() == "Moscow") it.type() shouldBe getType<Many<Int>>()
                else it.type() shouldBe getType<Int?>()
            }

            val actual = data.flatMap { col ->
                val city = col.name()
                map { row -> (row[name] to city) to col[row.index] }.filter { it.second != null }
            }.toMap()
            actual shouldBe expected
        }

        typed2.pivot { nameAndCity.city }.withIndex { nameAndCity.name }.into { age }.check()
        df2.pivot(nameAndCity[city]).withIndex { nameAndCity[name] }.into(age).check()
        df2.pivot { it[GroupedPerson::nameAndCity][NameAndCity::city] }.withIndex { it[GroupedPerson::nameAndCity][NameAndCity::name] }.into(GroupedPerson::age).check()
        df2.pivot { it["nameAndCity"]["city"] }.withIndex { it["nameAndCity"]["name"] }.into("age").check()
    }

    @Test
    fun `pivot grouped column`() {
        val grouped = typed.group { age and weight }.into("info")
        val pivoted = grouped.pivot { city }.withIndex { name }.into("info")
        pivoted.ncol() shouldBe typed.city.ndistinct() + 1

        val expected =
            typed.rows().groupBy { it.name to (it.city ?: "null") }.mapValues { it.value.map { it.age to it.weight } }
        val dataCols = pivoted.columns().drop(1)

        dataCols.forEach { (it.isGroup() || it.isTable()) shouldBe true }

        val names = pivoted.name
        dataCols.forEach { col ->
            val city = col.name()
            (0 until pivoted.nrow()).forEach { row ->
                val name = names[row]
                val value = col[row]
                val expValues = expected[name to city]
                when {
                    expValues == null -> when (value) {
                        null -> {
                        }
                        is AnyRow -> value.isEmpty() shouldBe true
                        is AnyFrame -> value.ncol() shouldBe 0
                    }
                    expValues.size == 1 -> {
                        value shouldNotBe null
                        val single =
                            if (value is AnyRow) value else if (value is AnyFrame) value[0] else fail("invalid value type")
                        single.size() shouldBe 2
                        single.int("age") to single.nint("weight") shouldBe expValues[0]
                    }
                    else -> {
                        val df = value as? AnyFrame
                        df shouldNotBe null
                        df!!.map { int("age") to nint("weight") }
                            .sortedBy { it.first } shouldBe expValues.sortedBy { it.first }
                    }
                }
            }
        }
    }

    @Test
    fun splitCols() {

        val split = typed2.split { nameAndCity.name }.by { it.toCharArray().toList() }.inward().into { "char$it" }
        split.columnNames() shouldBe typed2.columnNames()
        split.nrow() shouldBe typed2.nrow()
        split.nameAndCity.columnNames() shouldBe typed2.nameAndCity.columnNames()
        val nameGroup = split.nameAndCity.name.asGroup()
        nameGroup.name() shouldBe "name"
        nameGroup.isGroup() shouldBe true
        nameGroup.ncol() shouldBe typed2.nameAndCity.name.map { it.length }.max()
        nameGroup.columnNames() shouldBe (1..nameGroup.ncol()).map { "char$it" }
    }

    @Test
    fun `split into rows`() {

        val split = typed2.split { nameAndCity.name }.by { it.toCharArray().toList() }.intoRows()
        val merged = split.mergeRows { nameAndCity.name }
        val joined = merged.update { nameAndCity.name }.cast<List<Char>>().with { it.joinToString("") }
        joined shouldBe typed2
    }

    @Test
    fun `all except`() {
        val info by columnGroup()
        val moved = typed.group { except(name) }.into(info)
        val actual = moved.select { except(info) }
        actual shouldBe typed.select { name }
    }

    @Test
    fun `move and group`() {
        val info by columnGroup()
        val moved = typed.group { except(name) }.into(info)
        val grouped = moved.groupBy { except(info) }.plain()
        grouped.nrow() shouldBe typed.name.ndistinct()
    }

    @Test
    fun `merge rows into table`() {

        val info by columnGroup()
        val moved = typed.group { except(name) }.into(info)
        val merged = moved.mergeRows { info }
        val grouped = typed.groupBy { name }.mapNotNullGroups { remove { name } }
        val expected = grouped.plain().rename(grouped.groups).into(info)
        merged shouldBe expected
    }

    @Test
    fun `update grouped column to table`() {
        val info by columnGroup()
        val grouped = typed.group { age and weight }.into(info)
        val updated = grouped.update(info).with2 { row, column -> column.asGroup().df }
        val col = updated[info.name()]
        col.kind() shouldBe ColumnKind.Frame
        val table = col.asTable()
        table.schema.value.columns.map { it.key }.sorted() shouldBe typed.select { age and weight }.columnNames()
            .sorted()
    }

    @Test
    fun extensionPropertiesTest() {
        val code = CodeGenerator.create().generate<GroupedPerson>(
            interfaceMode = InterfaceGenerationMode.None,
            extensionProperties = true
        ).declarations
        val dataFrameBase = DataFrameBase::class.qualifiedName
        val dataFrameRowBase = DataRowBase::class.qualifiedName
        val dataFrameRow = DataRow::class.qualifiedName
        val className = GroupedPerson::class.qualifiedName
        val shortName = GroupedPerson::class.simpleName!!
        val nameAndCity = NameAndCity::class.qualifiedName
        val groupedColumn = ColumnGroup::class.qualifiedName
        val columnData = DataColumn::class.qualifiedName
        val expected = """
            val $dataFrameBase<$className>.age: $columnData<kotlin.Int> @JvmName("${shortName}_age") get() = this["age"] as $columnData<kotlin.Int>
            val $dataFrameRowBase<$className>.age: kotlin.Int @JvmName("${shortName}_age") get() = this["age"] as kotlin.Int
            val $dataFrameBase<$className>.nameAndCity: $groupedColumn<$nameAndCity> @JvmName("${shortName}_nameAndCity") get() = this["nameAndCity"] as $groupedColumn<$nameAndCity>
            val $dataFrameRowBase<$className>.nameAndCity: $dataFrameRow<$nameAndCity> @JvmName("${shortName}_nameAndCity") get() = this["nameAndCity"] as $dataFrameRow<$nameAndCity>
            val $dataFrameBase<$className>.weight: $columnData<kotlin.Int?> @JvmName("${shortName}_weight") get() = this["weight"] as $columnData<kotlin.Int?>
            val $dataFrameRowBase<$className>.weight: kotlin.Int? @JvmName("${shortName}_weight") get() = this["weight"] as kotlin.Int?
        """.trimIndent()
        code shouldBe expected
    }

    @Test
    fun parentColumnTest() {
        val res = typed2.move { colsDfs { it.depth > 0 } }.toTop { it.parent!!.name + "-" + it.name }
        res.ncol() shouldBe 4
        res.columnNames() shouldBe listOf("nameAndCity-name", "nameAndCity-city", "age", "weight")
    }

    @Test
    fun `group cols`() {

        val joined = typed2.move { dfs() }.into { path(it.path.joinToString(".")) }
        val grouped = joined.group { nameContains(".") }.into { it.name.substringBefore(".") }
        val expected = typed2.rename { nameAndCity.all() }.into { it.path.joinToString(".") }
        grouped shouldBe expected
    }

    @Test
    fun rename() {
        val res = typed2.rename { nameAndCity.all() }.into { it.name.capitalize() }
        res.nameAndCity.columnNames() shouldBe typed2.nameAndCity.columnNames().map { it.capitalize() }
    }

    @Test
    fun moveAfter() {
        val moved = typed2.move { age }.after { nameAndCity.name }
        moved.ncol() shouldBe 2
        moved.nameAndCity.ncol() shouldBe 3
        moved.nameAndCity.select { all() } shouldBe dataFrameOf(
            typed2.nameAndCity.name,
            typed2.age,
            typed2.nameAndCity.city
        )
    }

    @Test
    fun moveAfter2() {
        val moved = typed2.move { nameAndCity.name }.after { age }
        moved.ncol() shouldBe 4
        moved.nameAndCity.ncol() shouldBe 1
        moved.remove { nameAndCity } shouldBe typed2.select { age and nameAndCity.name and weight }
    }

    @Test
    fun splitFrameColumnsIntoRows() {
        val grouped = typed.groupBy { city }
        val groupCol = grouped.groups.name()
        val plain = grouped.plain()
        val res =
            plain.split(groupCol).intoRows().remove { it[groupCol]["city"] }.ungroup(groupCol).sortBy { name and age }
        res shouldBe typed.sortBy { name and age }.moveToLeft { city }
    }

    @Test
    fun explodeFrameColumnWithNulls() {
        val grouped = typed.groupBy { city }
        val groupCol = grouped.groups.toDefinition()
        val plain = grouped.plain()
            .update { groupCol }.at(1).withNull()
            .update { groupCol }.at(2).with { emptyDataFrame(0) }
            .update { groupCol }.at(3).with { it.filter { false } }
        val res = plain.explode(dropEmpty = false) { groupCol }
        val expected = plain[groupCol].sumOf { Math.max(it?.nrow() ?: 0, 1) }
        res.nrow() shouldBe expected
    }

    @Test
    fun `join with left path`() {
        val joined = (typed2 - { weight }).join(typed - { city }) { nameAndCity.name.match(right.name) and age }
        joined shouldBe typed2
    }

    @Test
    fun `join with right path`() {
        val joined = (typed - { city }).join(typed2 - { weight }) { name.match(right.nameAndCity.name) and age }
        val expected = typed.moveToRight { city }.move { city }.under("nameAndCity")
        joined shouldBe expected
    }

    @Test
    fun `join by map column`() {
        val nameAndAge by columnGroup()
        val cityFirst by column<String>(nameAndAge)
        val grouped = typed.group { name and age }.into(nameAndAge).add(cityFirst) { city?.get(0) }
        grouped[nameAndAge].ncol() shouldBe 3

        val left = grouped - { weight }
        val right = grouped - { city }
        val joined = left.join(right) { nameAndAge }
        joined shouldBe grouped
    }

    @Test
    fun `join by frame column`() {
        val left = typed.groupBy { name }.mapGroups { it?.remove { name and city } }
        val right =
            typed.update { name }.with { it.reversed() }.groupBy { name }.mapGroups { it?.remove { name and city } }
        val groupCol = left.groups.toAccessor()
        val joined = left.plain().join(right.plain()) { groupCol }
        joined.ncol() shouldBe 3
        val name_1 by column<String>()
        joined.columnNames() shouldBe listOf(typed.name.name(), groupCol.name(), name_1.name())
        joined[groupCol].kind() shouldBe ColumnKind.Frame
        joined.select { cols(0, 1) } shouldBe left.plain()
        joined.select { cols(2, 1) }.rename(name_1).into(typed.name) shouldBe right.plain()
        joined.name shouldBe left.keys.name
        joined.forEach { name_1() shouldBe name.reversed() }
    }

    @Test
    fun `add frame column`() {

        val frameCol by frameColumn()
        val added = typed2.add(frameCol) { nameAndCity.duplicate(3) }
        added[frameCol].kind() shouldBe ColumnKind.Frame
    }

    @Test
    fun `insert column`() {

        val colName = "reversed"
        fun DataFrame<GroupedPerson>.check() {
            nameAndCity.ncol() shouldBe 3
            nameAndCity.columnNames() shouldBe listOf(
                typed2.nameAndCity.name.name(),
                colName,
                typed2.nameAndCity.city.name()
            )
        }

        typed2.insert(colName) { nameAndCity.name.reversed() }.after { nameAndCity.name }.check()
    }

    @Test
    fun append() {
        val res = typed2.append(listOf("Bill", "San Francisco"), null, 66)
        res.nrow() shouldBe typed2.nrow() + 1
        res.nameAndCity.last().values() shouldBe listOf("Bill", "San Francisco")
        res.age.hasNulls() shouldBe true
    }

    @Test
    fun `append nulls`() {
        val res = typed2.append(null, null, null)
        res.nrow() shouldBe typed2.nrow() + 1
        res.nameAndCity.last().values() shouldBe listOf(null, null)
        res.age.hasNulls() shouldBe true
        res.nameAndCity.name.hasNulls() shouldBe true
    }


    @Test
    fun `create data frame from map column`() {
        val df = dataFrameOf(typed.name, typed2.nameAndCity)
        df.nrow() shouldBe typed.nrow()
    }

    @Test
    fun `column group properties`() {
        typed2.nameAndCity.name() shouldBe "nameAndCity"
        val renamed = typed2.nameAndCity.rename("newName")
        renamed.name() shouldBe "newName"
        renamed.select { name } shouldBe typed2.select { nameAndCity.name }
        renamed.filter { name.startsWith("A") }.nrow() shouldBe typed.count { name.startsWith("A") }
    }

    @Test
    fun `distinct at column group`() {
        typed2.nameAndCity.distinct().filter { name.startsWith("A") } shouldBe typed.select { name and city }.distinct()
            .filter { name.startsWith("A") }
    }

    @Test
    fun `check column path`() {
        typed2.getColumnPath { nameAndCity.name }.size shouldBe 2
    }

    @Test
    fun `filter not null without arguments`() {
        typed2.dropNulls() shouldBe typed.dropNulls { weight }.group {name and city}.into("nameAndCity")
    }

    @Test
    fun `select group`() {
        val groupCol = typed2[nameAndCity]
        typed2.select { groupCol and age }.columnNames() shouldBe listOf("nameAndCity", "age")
    }
}