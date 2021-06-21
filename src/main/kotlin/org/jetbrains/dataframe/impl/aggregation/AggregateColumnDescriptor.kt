package org.jetbrains.dataframe.impl.aggregation

import org.jetbrains.dataframe.aggregation.AggregateColumnsSelector
import org.jetbrains.dataframe.ColumnPath
import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.UnresolvedColumnsPolicy
import org.jetbrains.dataframe.columns.ColumnWithPath
import org.jetbrains.dataframe.columns.shortPath
import org.jetbrains.dataframe.impl.aggregation.receivers.AggregateReceiverInternal
import org.jetbrains.dataframe.resolve

internal class AggregateColumnDescriptor<C>(
    val column: ColumnWithPath<C>,
    val default: C? = null,
    val newPath: ColumnPath? = null
) : ColumnWithPath<C> by column

internal fun <T, C> DataFrame<T>.getAggregateColumn(selector: AggregateColumnsSelector<T, C>) =
    getAggregateColumns(selector).single()

internal fun <T, C> DataFrame<T>.getAggregateColumns(selector: AggregateColumnsSelector<T, C>): List<AggregateColumnDescriptor<C>> {
    val columns = selector.toColumns().resolve(this, UnresolvedColumnsPolicy.Create)
    return columns.map {
        when (val col = it) {
            is AggregateColumnDescriptor<*> -> col as AggregateColumnDescriptor<C>
            else -> AggregateColumnDescriptor(it, null, null)
        }
    }
}

internal fun <T, C> AggregateReceiverInternal<T>.getPath(col: AggregateColumnDescriptor<C>, isSingle: Boolean) =
    col.newPath ?: if (isSingle) pathForSingleColumn(col.data) else col.data.shortPath()