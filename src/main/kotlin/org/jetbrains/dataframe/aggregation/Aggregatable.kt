package org.jetbrains.dataframe.aggregation

import org.jetbrains.dataframe.AggregateBody
import org.jetbrains.dataframe.ColumnsSelector
import org.jetbrains.dataframe.DataFrame

interface Aggregatable<out T> {

    // TODO: move to internal
    fun <R> aggregateBase(body: AggregateBody<T, R>): DataFrame<T>

    // TODO: move to internal
    fun remainingColumnsSelector(): ColumnsSelector<*, *>
}