package org.jetbrains.dataframe.impl.aggregation.modes

import org.jetbrains.dataframe.*
import org.jetbrains.dataframe.impl.aggregation.aggregateInternal
import org.jetbrains.dataframe.impl.aggregation.aggregators.Aggregator
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
    data: GroupedPivotAggregations<T>,
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
): DataFrame<T> = aggregateInternal {
    val cols = df[columns]
    if (cols.size == 1) {
        yield(pathOf(name ?: cols[0].name()), aggregator.aggregate(cols[0]))
    } else {
        yield(pathOf(name ?: aggregator.name), aggregator.aggregate(cols))
    }
}

internal fun <T, C, R> GroupedPivotAggregations<T>.aggregateAll(
    aggregator: Aggregator<C, R>,
    columns: ColumnsSelector<T, C>
): DataFrame<T> = aggregateInternal {
    val cols = df[columns]
    if (cols.size == 1) {
        yield(emptyPath(), aggregator.aggregate(cols[0]))
    } else {
        yield(emptyPath(), aggregator.aggregate(cols))
    }
}
