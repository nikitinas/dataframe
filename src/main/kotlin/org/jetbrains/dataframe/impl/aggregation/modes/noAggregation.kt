package org.jetbrains.dataframe.impl.aggregation.modes

import org.jetbrains.dataframe.GroupByAggregations
import org.jetbrains.dataframe.impl.aggregation.aggregateInternal
import org.jetbrains.dataframe.impl.aggregation.receivers.AggregateBodyInternal

@PublishedApi
internal fun <T, R> GroupByAggregations<T>.aggregateValue(
    resultName: String,
    body: AggregateBodyInternal<T, R>
) = aggregateInternal {
    val value = body(this)
    yield(listOf(resultName), value)
}