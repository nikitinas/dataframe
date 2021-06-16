package org.jetbrains.dataframe.impl.aggregation.modes

import org.jetbrains.dataframe.aggregation.Aggregatable
import org.jetbrains.dataframe.aggregation.DataFrameAggregations
import org.jetbrains.dataframe.aggregation.receivers.AggregateColumnsSelector
import org.jetbrains.dataframe.impl.aggregation.aggregators.Aggregator
import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.DataRow
import org.jetbrains.dataframe.impl.aggregation.getAggregateColumns
import org.jetbrains.dataframe.impl.aggregation.getPath

internal fun <T, C, R> Aggregator<*, R>.aggregateFor(
    data: Aggregatable<T>,
    columns: AggregateColumnsSelector<T, C?>
): DataFrame<T> = data.aggregateFor(columns, cast())

internal fun <T, C, R> Aggregatable<T>.aggregateFor(
    columns: AggregateColumnsSelector<T, C>,
    aggregator: Aggregator<C, R>
) = aggregateBase {
    val cols = getAggregateColumns(columns)
    val isSingle = cols.size == 1
    cols.forEach { col ->
        val path = getPath(col, isSingle)
        yield(path, aggregator.aggregate(col.data), col.type, col.default)
    }
}

internal fun <T, C, R> Aggregator<*, R>.aggregateFor(
    data: DataFrameAggregations<T>,
    columns: AggregateColumnsSelector<T, C?>
): DataRow<T> = aggregateFor(data as Aggregatable<T>, columns)[0]