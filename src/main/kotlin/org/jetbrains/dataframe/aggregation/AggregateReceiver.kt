package org.jetbrains.dataframe.aggregation

import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.NamedValue
import org.jetbrains.dataframe.getType
import org.jetbrains.dataframe.impl.aggregation.ValueWithDefault
import org.jetbrains.dataframe.impl.aggregation.receivers.internal

interface AggregateReceiverWithDefault<out T> : DataFrame<T> {

    infix fun <R> R.default(defaultValue: R): Any = when (this) {
        is NamedValue -> this.also { it.default = defaultValue }
        else -> ValueWithDefault(this, defaultValue)
    }
}

abstract class AggregateReceiver<out T> : AggregateReceiverWithDefault<T> {

    inline infix fun <reified R> R.into(name: String) = internal().yield(listOf(name), this, getType<R>())
}


