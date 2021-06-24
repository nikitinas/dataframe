package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.AnyCol
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.impl.GroupedDataFrameImpl
import org.jetbrains.dataframe.impl.columns.toColumnSet
import org.jetbrains.dataframe.impl.columns.toColumns
import org.jetbrains.dataframe.impl.nameGenerator
import kotlin.reflect.KProperty

public fun <T> DataColumn<T>.groupBy(vararg cols: AnyCol): GroupedDataFrame<Unit, Unit> = groupBy(cols.toList())
public fun <T> DataColumn<T>.groupBy(cols: Iterable<AnyCol>): GroupedDataFrame<Unit, Unit> =
    (cols + this).asDataFrame<Unit>().groupBy { cols(0 until ncol() - 1) }

public fun <T> DataFrame<T>.groupBy(cols: Iterable<Column>): GroupedDataFrame<T, T> = groupBy { cols.toColumnSet() }
public fun <T> DataFrame<T>.groupBy(vararg cols: KProperty<*>): GroupedDataFrame<T, T> = groupBy { cols.toColumns() }
public fun <T> DataFrame<T>.groupBy(vararg cols: String): GroupedDataFrame<T, T> = groupBy { cols.toColumns() }
public fun <T> DataFrame<T>.groupBy(vararg cols: Column): GroupedDataFrame<T, T> = groupBy { cols.toColumns() }
public fun <T> DataFrame<T>.groupBy(cols: ColumnsSelector<T, *>): GroupedDataFrame<T, T> {
    val nameGenerator = nameGenerator(columnForGroupedData.name())
    val keyColumns = get(cols).map {
        val currentName = it.name()
        val uniqueName = nameGenerator.addUnique(currentName)
        if (uniqueName != currentName) it.rename(uniqueName)
        else it
    }
    val groups = (0 until nrow())
        .map { index -> keyColumns.map { it[index] } to index }
        .groupBy({ it.first }) { it.second }.toList()

    val keyIndices = groups.map { it.second[0] }

    val keyColumnsToInsert = keyColumns.map {
        val column = it.slice(keyIndices)
        val path = listOf(it.name())
        ColumnToInsert(path, column, null)
    }

    val keyColumnsDf = insertColumns(keyColumnsToInsert).typed<T>()

    val permutation = groups.flatMap { it.second }
    val sorted = getRows(permutation)

    var lastIndex = 0
    val startIndices = groups.asSequence().map {
        val start = lastIndex
        lastIndex += it.second.size
        start
    }

    val groupedColumn = DataColumn.create(columnForGroupedData.name(), sorted, startIndices, false)

    val df = keyColumnsDf + groupedColumn
    return GroupedDataFrameImpl(df, groupedColumn, cols)
}

public inline fun <T, reified R> DataFrame<T>.groupByNew(name: String = "key", noinline expression: RowSelector<T, R?>): GroupedDataFrame<T, T> =
    add(name, expression).groupBy(name)

internal val columnForGroupedData by column<AnyFrame>("groups")
