package org.jetbrains.dataframe.impl.aggregation.aggregators

import kotlin.reflect.KProperty

@PublishedApi
internal class AggregatorOptionSwitch<P, C, R>(val name: String, val getAggregator: (P) -> AggregatorProvider<C, R>) {

    private val cache = mutableMapOf<P, Aggregator<C, R>>()

    operator fun invoke(option: P) = cache.getOrPut(option) { getAggregator(option).create(name) }

    class Factory<P, C, R>(val getAggregator: (P) -> AggregatorProvider<C, R>) {
        operator fun getValue(obj: Any?, property: KProperty<*>) = AggregatorOptionSwitch(property.name, getAggregator)
    }
}
