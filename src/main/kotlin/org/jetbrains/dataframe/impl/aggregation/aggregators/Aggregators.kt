package org.jetbrains.dataframe.impl.aggregation.aggregators

import org.jetbrains.dataframe.mean
import org.jetbrains.dataframe.std
import org.jetbrains.dataframe.sum
import kotlin.reflect.KClass

@PublishedApi
internal object Aggregators {

    private fun <C> preservesType(aggregate: Iterable<C>.() -> C?) =
        TwoStepAggregator.Factory<C, C>({ values, _ -> aggregate(values) }, aggregate, true)

    private fun <C, R> changesType(aggregate1: Iterable<C>.(KClass<*>) -> R, aggregate2: Iterable<R>.() -> R) =
        TwoStepAggregator.Factory(aggregate1, aggregate2, false)

    private fun extendsNumbers(aggregate: Iterable<Number>.(KClass<*>) -> Number?) =
        NumbersAggregator.Factory(aggregate)

    private fun <P, C, R> withOption(getAggregator: (P) -> AggregatorProvider<C, R>) =
        AggregatorOptionSwitch.Factory(getAggregator)

    val min by preservesType<Comparable<Any?>> { minOrNull() }
    val max by preservesType<Comparable<Any?>> { maxOrNull() }
    val std by changesType<Number, Double>({ std(it) }) { std() }
    val mean by withOption<Boolean, Number, Double> { skipNa ->
        changesType({ mean(it, skipNa) }) { mean(skipNa) }
    }
    val sum by extendsNumbers { sum(it) }
}