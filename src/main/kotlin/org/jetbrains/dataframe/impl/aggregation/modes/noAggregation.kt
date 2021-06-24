package org.jetbrains.dataframe.impl.aggregation.modes

import org.jetbrains.dataframe.AggregateBody
import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.GroupByAggregations
import org.jetbrains.dataframe.impl.aggregation.aggregateInternal
import org.jetbrains.dataframe.impl.aggregation.receivers.internal

@PublishedApi
internal fun <T, R> GroupByAggregations<T>.aggregateValue(
    resultName: String,
    body: AggregateBody<T, R>
): DataFrame<T> = aggregateInternal {
    val value = body.internal()(this)
    yield(listOf(resultName), value)
}
