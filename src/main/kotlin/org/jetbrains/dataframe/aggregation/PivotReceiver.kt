package org.jetbrains.dataframe.aggregation

public abstract class PivotReceiver<out T> : AggregateReceiver<T>()

public typealias PivotAggregateBody<T, R> = PivotReceiver<T>.(PivotReceiver<T>) -> R
