package org.jetbrains.dataframe

import io.kotlintest.shouldBe
import org.intellij.lang.annotations.Language
import org.jetbrains.dataframe.annotations.DataSchema
import org.jetbrains.dataframe.columns.ColumnGroup
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.impl.columns.asGroup
import org.jetbrains.dataframe.io.readJsonStr
import org.junit.Test

class GatherTests {

    //region Data Source

    @Language("json")
    val df = """
            [
                {
                    "name": "abc",
                    "normal": {
                        "c1": "a",
                        "c2": "b",
                        "c3": "c"
                    },
                    "reversed": {
                        "c1": "c",
                        "c2": "b",
                        "c3": "a"
                    },
                    "first": {
                        "c1": "c"
                    }
                },
                {
                    "name": "qw",
                    "normal": {
                        "c1": "q",
                        "c2": "w"
                    },
                    "reversed": {
                        "c1": "w",
                        "c2": "q"
                    },
                    "first": {
                        "c1": "q"
                    }
                }
            ]
        """.let {
        DataFrame.readJsonStr(it)
    }

    //endregion

    val generatedCode = df.generateCode("Marker")

    //region Generated code

    @DataSchema(isOpen = false)
    interface Marker1{
        val c1: String
        val c2: String
        val c3: String?
    }
    val DataFrameBase<Marker1>.c1: DataColumn<String> @JvmName("Marker1_c1") get() = this["c1"] as DataColumn<String>
    val DataRowBase<Marker1>.c1: String @JvmName("Marker1_c1") get() = this["c1"] as String
    val DataFrameBase<Marker1>.c2: DataColumn<String> @JvmName("Marker1_c2") get() = this["c2"] as DataColumn<String>
    val DataRowBase<Marker1>.c2: String @JvmName("Marker1_c2") get() = this["c2"] as String
    val DataFrameBase<Marker1>.c3: DataColumn<String?> @JvmName("Marker1_c3") get() = this["c3"] as DataColumn<String?>
    val DataRowBase<Marker1>.c3: String? @JvmName("Marker1_c3") get() = this["c3"] as String?
    @DataSchema(isOpen = false)
    interface Marker2{
        val c1: String
        val c2: String
        val c3: String?
    }
    val DataFrameBase<Marker2>.c1: DataColumn<String> @JvmName("Marker2_c1") get() = this["c1"] as DataColumn<String>
    val DataRowBase<Marker2>.c1: String @JvmName("Marker2_c1") get() = this["c1"] as String
    val DataFrameBase<Marker2>.c2: DataColumn<String> @JvmName("Marker2_c2") get() = this["c2"] as DataColumn<String>
    val DataRowBase<Marker2>.c2: String @JvmName("Marker2_c2") get() = this["c2"] as String
    val DataFrameBase<Marker2>.c3: DataColumn<String?> @JvmName("Marker2_c3") get() = this["c3"] as DataColumn<String?>
    val DataRowBase<Marker2>.c3: String? @JvmName("Marker2_c3") get() = this["c3"] as String?
    @DataSchema(isOpen = false)
    interface Marker3{
        val c1: String
    }
    val DataFrameBase<Marker3>.c1: DataColumn<String> @JvmName("Marker3_c1") get() = this["c1"] as DataColumn<String>
    val DataRowBase<Marker3>.c1: String @JvmName("Marker3_c1") get() = this["c1"] as String
    @DataSchema
    interface Marker{
        val name: String
        val normal: DataRow<Marker1>
        val reversed: DataRow<Marker2>
        val first: DataRow<Marker3>
    }
    val DataFrameBase<Marker>.first: ColumnGroup<*> @JvmName("Marker_first") get() = this["first"] as ColumnGroup<*>
    val DataRowBase<Marker>.first: org.jetbrains.dataframe.AnyRow @JvmName("Marker_first") get() = this["first"] as org.jetbrains.dataframe.AnyRow
    val DataFrameBase<Marker>.name: DataColumn<String> @JvmName("Marker_name") get() = this["name"] as DataColumn<String>
    val DataRowBase<Marker>.name: String @JvmName("Marker_name") get() = this["name"] as String
    val DataFrameBase<Marker>.normal: ColumnGroup<*> @JvmName("Marker_normal") get() = this["normal"] as ColumnGroup<*>
    val DataRowBase<Marker>.normal: org.jetbrains.dataframe.AnyRow @JvmName("Marker_normal") get() = this["normal"] as org.jetbrains.dataframe.AnyRow
    val DataFrameBase<Marker>.reversed: ColumnGroup<*> @JvmName("Marker_reversed") get() = this["reversed"] as ColumnGroup<*>
    val DataRowBase<Marker>.reversed: org.jetbrains.dataframe.AnyRow @JvmName("Marker_reversed") get() = this["reversed"] as org.jetbrains.dataframe.AnyRow

    //endregion

    val typed = df.typed<Marker>()

    @Test
    fun gather() {

        val mode by column<String>()
        val gathered = typed.gather { except(name) }.into(mode)

        val expected = typed.groupBy { name }.mapNotNullGroups {

            val cols = columns().drop(1).map { it.asGroup() } // drop 'name' column
            val dataRows = cols.map { it[0] }

            val newDf = listOf(
                    name.replaceAll(MutableList(cols.size){ name[0] }),
                    mode.withValues(cols.map { it.name() }, false),
                column("c1", dataRows.map { it.tryGet("c1") as? String}),
                column("c2", dataRows.map { it.tryGet("c2") as? String}),
                column("c3", dataRows.map { it.tryGet("c3") as? String})
            ).asDataFrame<Unit>()

            newDf
        }.ungroup()

        gathered shouldBe expected
    }

    @Test
    fun `generated code is fully typed`() {
        generatedCode.contains("<*>") shouldBe false
    }
}