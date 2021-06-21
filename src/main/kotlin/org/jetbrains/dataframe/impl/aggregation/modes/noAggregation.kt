package org.jetbrains.dataframe.impl.aggregation.modes

import org.jetbrains.dataframe.AggregateBody
import org.jetbrains.dataframe.GroupByAggregations

@PublishedApi
internal fun <T, R> GroupByAggregations<T>.aggregateValue(
    resultName: String,
    body: AggregateBody<T, R>
) = aggregate {
    val value = body(this)
    yield(listOf(resultName), value)
}