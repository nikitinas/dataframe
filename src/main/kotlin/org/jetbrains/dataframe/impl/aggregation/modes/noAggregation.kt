package org.jetbrains.dataframe.impl.aggregation.modes

import org.jetbrains.dataframe.AggregateBody
import org.jetbrains.dataframe.aggregation.Aggregatable

@PublishedApi
internal inline fun <T, reified R> Aggregatable<T>.aggregateValue(
    resultName: String,
    crossinline body: AggregateBody<T, R>
) = aggregateBase {
    val value = body(this)
    yield(listOf(resultName), value)
}