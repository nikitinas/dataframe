package org.jetbrains.dataframe.impl.aggregation.modes

import org.jetbrains.dataframe.aggregation.DataFrameAggregations
import org.jetbrains.dataframe.aggregation.GroupByAggregations
import org.jetbrains.dataframe.aggregation.PivotAggregations
import org.jetbrains.dataframe.impl.aggregation.aggregators.Aggregator
import org.jetbrains.dataframe.ColumnsSelector
import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.impl.emptyPath
import org.jetbrains.dataframe.impl.pathOf

@PublishedApi
internal fun <T, C, R> Aggregator<*, R>.aggregateAll(
    data: DataFrameAggregations<T>,
    columns: ColumnsSelector<T, C>
): C? = data.aggregateAll(cast(), columns)

internal fun <T, R, C> Aggregator<*, R>.aggregateAll(
    name: String?,
    data: GroupByAggregations<T>,
    columns: ColumnsSelector<T, C>
): DataFrame<T> = data.aggregateAll(cast(), columns, name)

internal fun <T, R, C> Aggregator<*, R>.aggregateAll(
    data: PivotAggregations<T>,
    columns: ColumnsSelector<T, C>
): DataFrame<T> = data.aggregateAll(cast(), columns)

internal fun <T, C, R> DataFrameAggregations<T>.aggregateAll(
    aggregator: Aggregator<C, R>,
    columns: ColumnsSelector<T, C>
): R? = aggregator.aggregate(get(columns))

internal fun <T, C, R> GroupByAggregations<T>.aggregateAll(
    aggregator: Aggregator<C, R>,
    columns: ColumnsSelector<T, C>,
    name: String?
): DataFrame<T> = aggregateBase {
    val cols = get(columns)
    if (cols.size == 1)
        yield(pathOf(name ?: cols[0].name()), aggregator.aggregate(cols[0]))
    else
        yield(pathOf(name ?: aggregator.name), aggregator.aggregate(cols))
}

internal fun <T, C, R> PivotAggregations<T>.aggregateAll(
    aggregator: Aggregator<C, R>,
    columns: ColumnsSelector<T, C>
): DataFrame<T> = aggregateBase {
        val cols = get(columns)
        if (cols.size == 1)
            yield(emptyPath(), aggregator.aggregate(cols[0]))
        else
            yield(emptyPath(), aggregator.aggregate(cols))
    }