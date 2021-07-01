package org.jetbrains.dataframe.aggregation

import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.NamedValue
import org.jetbrains.dataframe.getType
import org.jetbrains.dataframe.impl.aggregation.ValueWithDefault
import org.jetbrains.dataframe.impl.aggregation.receivers.internal

public interface AggregateReceiverWithDefault<out T> : DataFrame<T> {

    public infix fun <R> R.default(defaultValue: R): Any = when (this) {
        is NamedValue -> this.also { it.default = defaultValue }
        else -> ValueWithDefault(this, defaultValue)
    }
}

public abstract class AggregateReceiver<out T> : AggregateReceiverWithDefault<T> {

    public inline infix fun <reified R> R.into(name: String): NamedValue = internal().yield(listOf(name), this, getType<R>())
}
