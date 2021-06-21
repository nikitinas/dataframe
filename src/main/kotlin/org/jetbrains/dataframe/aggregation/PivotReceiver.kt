package org.jetbrains.dataframe.aggregation

abstract class PivotReceiver<out T>: AggregateReceiver<T>()

typealias PivotAggregateBody<T, R> = PivotReceiver<T>.(PivotReceiver<T>) -> R