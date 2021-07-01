package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.BaseColumn
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.typeClass
import org.jetbrains.dataframe.columns.values
import org.jetbrains.dataframe.impl.aggregation.aggregators.Aggregators
import org.jetbrains.dataframe.impl.aggregation.modes.of

public interface DataColumnAggregations<out T> : BaseColumn<T> {

    public fun count(predicate: Predicate<T>? = null): Int = if (predicate == null) size() else values().count(predicate)
}

// region inlines

// region min

public fun <T : Comparable<T>> DataColumn<T?>.min(): T? = asSequence().filterNotNull().minOrNull()

public fun <T, R : Comparable<R>> DataColumn<T>.minBy(selector: (T) -> R): T? = values.minByOrNull(selector)

// endregion

// region max

public fun <T : Comparable<T>> DataColumn<T?>.max(): T? = asSequence().filterNotNull().maxOrNull()

public fun <T, R : Comparable<R>> DataColumn<T>.maxBy(selector: (T) -> R): T? = values.maxByOrNull(selector)

// endregion

// region sum

public fun <T : Number> DataColumn<T>.sum(): T = values.sum(typeClass)

public inline fun <T, reified R : Number> DataColumn<T>.sumOf(crossinline expression: (T) -> R): R? =
    Aggregators.sum.cast<R>().of(this, expression)

// endregion

// region mean

public fun <T : Number> DataColumn<T?>.mean(skipNa: Boolean = false): Double = Aggregators.mean(skipNa).aggregate(this) ?: Double.NaN

// endregion

// endregion
