package org.jetbrains.dataframe.impl.aggregation

import org.jetbrains.dataframe.ColumnsSelector
import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.aggregation.Aggregatable
import org.jetbrains.dataframe.impl.aggregation.receivers.AggregateBodyInternal

internal interface AggregatableInternal<out T>: Aggregatable<T> {

    fun <R> aggregateInternal(body: AggregateBodyInternal<T, R>): DataFrame<T>

    fun remainingColumnsSelector(): ColumnsSelector<*, *>
}

internal fun <T> Aggregatable<T>.remainingColumnsSelector() = (this as AggregatableInternal).remainingColumnsSelector()

@PublishedApi
internal fun <T, R> Aggregatable<T>.aggregateInternal(body: AggregateBodyInternal<T, R>) = (this as AggregatableInternal).aggregateInternal(body)