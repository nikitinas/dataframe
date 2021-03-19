package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.ColumnWithPath
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.impl.columns.asGroup
import org.jetbrains.dataframe.impl.columns.asTable
import org.jetbrains.dataframe.impl.columns.isTable
import org.jetbrains.dataframe.impl.createDataCollector

fun <T> DataFrame<T>.splitRows(selector: ColumnsSelector<T, List<*>?>) =
    doSplitRows(this, getColumnsWithPaths(selector))

fun <T> doSplitRows(df: DataFrame<T>, columns: List<ColumnWithPath<List<*>?>>): DataFrame<T> {

    val rowExpandSizes = (0 until df.nrow()).map { row ->
        columns.maxOf { it.data[row]?.size ?: 0 }
    }

    val alignedColumnsData = columns.map {
        val list = it.data.toList()
        val alignedLists: List<List<*>> = list.mapIndexed { rowIndex, rowList ->

            val expectedListSize = rowExpandSizes[rowIndex]
            val actualSize = rowList?.size ?: 0

            if (actualSize != expectedListSize) {
                rowList?.let { it + arrayOfNulls(expectedListSize - actualSize) }
                    ?: arrayOfNulls<Any?>(expectedListSize).asList()
            } else rowList ?: emptyList()
        }
        it.path to alignedLists
    }.toMap()

    val outputRowsCount = rowExpandSizes.sum()

    fun splitIntoRows(df: AnyFrame, data: Map<ColumnPath, List<List<*>>>): AnyFrame {

        val newColumns = df.columns().map { col ->
            if (col.isGroup()) {
                val group = col.asGroup()
                val newData = data.mapNotNull {
                    if (it.key.isNotEmpty() && it.key[0] == col.name()) it.key.drop(1) to it.value else null
                }.toMap()
                val newDf = splitIntoRows(group.df, newData)
                DataColumn.create(col.name(), newDf)
            } else {
                val targetData = data[listOf(col.name())]
                if (targetData != null) {
                    assert(!col.isTable())
                    val collector = createDataCollector(outputRowsCount)
                    for (row in 0 until col.size)
                        targetData[row].forEach { collector.add(it) }
                    collector.toColumn(col.name())
                } else {
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
                        col.name(),
                        collector.values as List<AnyFrame?>,
                        lazy { col.asTable().schema }
                    )
                    else collector.toColumn(col.name())
                }
            }
        }
        return newColumns.asDataFrame<Unit>()
    }

    return splitIntoRows(df, alignedColumnsData).typed()
}