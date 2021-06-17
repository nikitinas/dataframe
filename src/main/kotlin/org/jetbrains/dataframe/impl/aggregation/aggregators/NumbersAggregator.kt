package org.jetbrains.dataframe.impl.aggregation.aggregators

import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.impl.commonNumberClass
import kotlin.reflect.KClass

internal class NumbersAggregator<C : Number>(name: String, aggregate: (Iterable<C>, KClass<*>) -> C?) :
    AggregatorBase<C, C>(name, aggregate) {

    override fun aggregate(columns: Iterable<DataColumn<C?>>): C? {
        val columnValues = columns.mapNotNull { aggregate(it) }
        val classes = columnValues.map { it.javaClass.kotlin }
        return aggregate(columnValues, classes.commonNumberClass())
    }

    class Factory(private val aggregate: Iterable<Number>.(KClass<*>) -> Number?) : AggregatorProvider<Number, Number> {
        override fun create(name: String) = NumbersAggregator(name, aggregate)
    }

    override val preservesType = false
}