package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.AnyColumn
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.FrameColumn
import org.jetbrains.dataframe.columns.ColumnGroup
import org.jetbrains.dataframe.columns.ValueColumn
import org.jetbrains.dataframe.columns.name
import org.jetbrains.dataframe.columns.size
import org.jetbrains.dataframe.columns.type
import org.jetbrains.dataframe.columns.values
import org.jetbrains.dataframe.impl.columns.asGroup
import org.jetbrains.dataframe.impl.columns.asTable
import org.jetbrains.dataframe.impl.columns.isTable
import org.jetbrains.dataframe.impl.columns.toColumns
import org.jetbrains.dataframe.impl.createDataCollector

fun <T> DataFrame<T>.explode(vararg columns: Column) = explode { columns.toColumns() }
fun <T> DataFrame<T>.explode(vararg columns: String) = explode { columns.toColumns() }

fun <T> DataFrame<T>.explode(dropEmpty: Boolean = true, selector: ColumnsSelector<T, *>): DataFrame<T> {

    val columns = getColumnsWithPaths(selector)

    val rowExpandSizes = indices.map { row ->
        columns.maxOf {
            val n = when (val value = it.data[row]) {
                is AnyFrame -> value.nrow()
                is Many<*> -> value.size
                else -> 1
            }
            if(!dropEmpty && n == 0) 1
            else n
        }
    }

    val outputRowsCount = rowExpandSizes.sum()

    fun splitIntoRows(df: AnyFrame, data: Set<ColumnPath>): AnyFrame {

        val newColumns: List<AnyColumn> = df.columns().map { col ->

            val isTargetColumn = data.contains(listOf(col.name))
            if (col is ColumnGroup<*>) { // go to nested columns recursively
                val group = col.asGroup()
                val newData = data.mapNotNull {
                    if (it.isNotEmpty() && it[0] == col.name) it.drop(1) else null
                }.toSet()
                val newDf = splitIntoRows(group.df, newData)
                DataColumn.create(col.name, newDf)
            } else if (isTargetColumn) { // values in current column will be splitted
                when (col) {
                    is FrameColumn<*> -> {
                        val newDf = col.values.mapIndexed { row, frame ->
                            val expectedSize = rowExpandSizes[row]
                            when {
                                frame != null -> {
                                    assert(frame.nrow <= expectedSize)
                                    frame.appendNulls(expectedSize - frame.nrow)
                                }
                                expectedSize > 0 -> DataFrame.empty(expectedSize)
                                else -> null
                            }
                        }.union()

                        DataColumn.create(col.name, newDf)
                    }
                    is ValueColumn<*> -> {
                        val collector = createDataCollector(outputRowsCount)
                        col.asSequence().forEachIndexed { rowIndex, value ->
                            val list = valueToList(value, splitStrings = false)
                            val expectedSize = rowExpandSizes[rowIndex]
                            list.forEach { collector.add(it) }
                            repeat (expectedSize - list.size) {
                                collector.add(null)
                            }
                        }
                        collector.toColumn(col.name)
                    }
                    else -> error("")
                }
            } else { // values in current column will be duplicated
                val collector = createDataCollector<Any?>(outputRowsCount, col.type)
                for (row in 0 until col.size) {
                    val expandSize = rowExpandSizes[row]
                    if (expandSize > 0) {
                        val value = col[row]
                        repeat(expandSize) {
                            collector.add(value)
                        }
                    }
                }
                if (col.isTable()) DataColumn.create(
                    col.name,
                    collector.values as List<AnyFrame?>,
                    collector.hasNulls,
                    col.asTable().schema // keep original schema
                )
                else collector.toColumn(col.name)
            }
        }
        return newColumns.asDataFrame<Unit>()
    }

    return splitIntoRows(this, columns.map { it.path }.toSet()).typed()
}