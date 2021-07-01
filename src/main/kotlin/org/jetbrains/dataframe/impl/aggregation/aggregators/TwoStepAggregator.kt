package org.jetbrains.dataframe.impl.aggregation.aggregators

import org.jetbrains.dataframe.columns.DataColumn
import kotlin.reflect.KClass

internal class TwoStepAggregator<C, R>(
    name: String,
    aggregateWithClass: (Iterable<C>, KClass<*>) -> R?,
    private val aggregateValues: (Iterable<R>) -> R?,
    override val preservesType: Boolean
) : AggregatorBase<C, R>(name, aggregateWithClass) {

    override fun aggregate(columns: Iterable<DataColumn<C?>>) = aggregateValues(columns.mapNotNull { aggregate(it) })

    class Factory<C, R>(
        private val aggregateWithClass: (Iterable<C>, KClass<*>) -> R?,
        private val aggregateValues: (Iterable<R>) -> R?,
        private val preservesType: Boolean
    ) : AggregatorProvider<C, R> {
        override fun create(name: String) = TwoStepAggregator(name, aggregateWithClass, aggregateValues, preservesType)
    }
}
