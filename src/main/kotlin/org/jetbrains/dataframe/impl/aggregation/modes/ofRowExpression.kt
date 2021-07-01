package org.jetbrains.dataframe.impl.aggregation.modes

import org.jetbrains.dataframe.*
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.impl.aggregation.aggregateInternal
import org.jetbrains.dataframe.impl.aggregation.aggregators.Aggregator
import org.jetbrains.dataframe.impl.emptyPath
import org.jetbrains.dataframe.impl.pathOf

@PublishedApi
internal inline fun <C, reified V, R> Aggregator<V, R>.aggregateOf(
    values: Iterable<C>,
    noinline transform: (C) -> V
): R? = aggregate(values.asSequence().map(transform).asIterable(), V::class)

@PublishedApi
internal inline fun <T, reified C, R> Aggregator<*, R>.aggregateOf(
    frame: DataFrame<T>,
    crossinline expression: RowSelector<T, C>
): R? = (this as Aggregator<C, R>).aggregateOf(frame.rows()) { expression(it, it) } // TODO: inline

@PublishedApi
internal inline fun <T, reified C, R> Aggregator<*, R>.of(
    data: DataFrameAggregations<T>,
    crossinline expression: RowSelector<T, C>
): R? = aggregateOf(data as DataFrame<T>, expression)

@PublishedApi
internal inline fun <C, reified V, R> Aggregator<V, R>.of(
    data: DataColumn<C>,
    crossinline expression: (C) -> V
): R? = aggregateOf(data.values()) { expression(it) } // TODO: inline

@PublishedApi
internal inline fun <T, reified C, reified R> Aggregator<*, R>.of(
    resultName: String? = null,
    data: GroupByAggregations<T>,
    crossinline expression: RowSelector<T, C>
): DataFrame<T> = data.aggregateOf(resultName, expression, this as Aggregator<C, R>)

@PublishedApi
internal inline fun <T, reified C, reified R> Aggregator<*, R>.of(
    data: GroupedPivotAggregations<T>,
    crossinline expression: RowSelector<T, C>
): DataFrame<T> = data.aggregateOf(expression, this as Aggregator<C, R>)

@PublishedApi
internal inline fun <T, reified C, reified R> GroupByAggregations<T>.aggregateOf(
    resultName: String?,
    crossinline selector: RowSelector<T, C>,
    aggregator: Aggregator<C, R>
): DataFrame<T> {
    val path = pathOf(resultName ?: "value")
    val type = getType<R>()
    return aggregateInternal {
        val value = aggregator.aggregateOf(df, selector)
        yield(path, value, type, null, false)
    }
}

@PublishedApi
internal inline fun <T, reified C, R> GroupedPivotAggregations<T>.aggregateOf(
    crossinline selector: RowSelector<T, C>,
    aggregator: Aggregator<C, R>
): DataFrame<T> = aggregateInternal {
    yield(emptyPath(), aggregator.aggregateOf(df, selector))
}
