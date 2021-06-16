package org.jetbrains.dataframe.impl.aggregation.aggregators

import kotlin.reflect.KProperty

internal class AggregatorOptionSwitch<P>(val name: String, val getAggregator: (P) -> AggregatorProvider<*, *>) {

    private val cache = mutableMapOf<P, Aggregator<*, *>>()

    operator fun invoke(option: P) = cache.getOrPut(option) { getAggregator(option).create(name) }

    class Factory<P>(val getAggregator: (P) -> AggregatorProvider<*, *>) {
        operator fun getValue(obj: Any?, property: KProperty<*>) = AggregatorOptionSwitch(property.name, getAggregator)
    }
}