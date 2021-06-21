package org.jetbrains.dataframe.impl.aggregation

import org.jetbrains.dataframe.AggregateBody
import org.jetbrains.dataframe.ColumnsSelector
import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.aggregation.Aggregatable

internal interface AggregatableInternal<out T>: Aggregatable<T> {

    fun <R> aggregateInternal(body: AggregateBody<T, R>): DataFrame<T>

    fun remainingColumnsSelector(): ColumnsSelector<*, *>
}

internal fun <T> Aggregatable<T>.remainingColumnsSelector() = (this as AggregatableInternal).remainingColumnsSelector()

@PublishedApi
internal fun <T, R> Aggregatable<T>.aggregateInternal(body: AggregateBody<T, R>) = (this as AggregatableInternal).aggregateInternal(body)