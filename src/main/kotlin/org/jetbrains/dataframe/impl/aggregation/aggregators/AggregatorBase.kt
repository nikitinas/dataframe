package org.jetbrains.dataframe.impl.aggregation.aggregators

import org.jetbrains.dataframe.asIterable
import org.jetbrains.dataframe.asSequence
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.typeClass
import kotlin.reflect.KClass

internal abstract class AggregatorBase<C, R>(
    override val name: String,
    protected val aggregator: (Iterable<C>, KClass<*>) -> R?
) : Aggregator<C, R> {

    override fun aggregate(column: DataColumn<C?>): R? = if (column.hasNulls()) {
        aggregate(column.asSequence().filterNotNull().asIterable(), column.typeClass)
    } else aggregate(column.asIterable() as Iterable<C>, column.typeClass)

    override fun aggregate(values: Iterable<C>, clazz: KClass<*>): R? = aggregator(values, clazz)
}