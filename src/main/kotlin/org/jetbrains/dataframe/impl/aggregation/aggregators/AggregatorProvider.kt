package org.jetbrains.dataframe.impl.aggregation.aggregators

import kotlin.reflect.KProperty

internal interface AggregatorProvider<C, R> {

    operator fun getValue(obj: Any?, property: KProperty<*>) = create(property.name)

    fun create(name: String): Aggregator<C, R>
}