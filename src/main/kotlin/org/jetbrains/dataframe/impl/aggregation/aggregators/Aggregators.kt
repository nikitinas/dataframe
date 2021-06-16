package org.jetbrains.dataframe.impl.aggregation.aggregators

import org.jetbrains.dataframe.mean
import org.jetbrains.dataframe.std
import org.jetbrains.dataframe.sum
import kotlin.reflect.KClass

@PublishedApi
internal object Aggregators {

    private fun <C> aggregator(aggregate: Iterable<C>.() -> C?) =
        TwoStepAggregator.Factory<C, C>({ values, _ -> aggregate(values) }, aggregate)

    private fun <C, R> fixedTypeAggregator(aggregate1: Iterable<C>.(KClass<*>) -> R, aggregate2: Iterable<R>.() -> R) =
        TwoStepAggregator.Factory(aggregate1, aggregate2)

    private fun numbersAggregator(aggregate: Iterable<Number>.(KClass<*>) -> Number?) =
        NumbersAggregator.Factory(aggregate)

    private fun <P> optioned(getAggregator: (P) -> AggregatorProvider<*, *>) =
        AggregatorOptionSwitch.Factory(getAggregator)

    val min by aggregator<Comparable<Any?>> { minOrNull() }
    val max by aggregator<Comparable<Any?>> { maxOrNull() }
    val std by fixedTypeAggregator<Number, Double>({ std(it) }) { std() }
    val mean by optioned<Boolean> { skipNaN ->
        fixedTypeAggregator<Number, Double>({
            mean(
                it,
                skipNaN
            )
        }) { mean(skipNaN) }
    }
    val sum by numbersAggregator { sum(it) }
}