package org.jetbrains.dataframe.impl.aggregation.modes

import org.jetbrains.dataframe.AggregateBody
import org.jetbrains.dataframe.aggregation.Aggregatable

@PublishedApi
internal fun <T, R> Aggregatable<T>.aggregateValue(
    resultName: String,
    body: AggregateBody<T, R>
) = aggregateBase {
    val value = body(this)
    yield(listOf(resultName), value)
}