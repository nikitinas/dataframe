package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.BaseColumn
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.typeClass
import org.jetbrains.dataframe.columns.values
import org.jetbrains.dataframe.impl.aggregation.aggregators.Aggregators
import org.jetbrains.dataframe.impl.aggregation.modes.of

interface DataColumnAggregations<out T>: BaseColumn<T> {

    fun count(predicate: Predicate<T>? = null) = if(predicate == null) size() else values().count(predicate)
}

// region inlines

// region min

fun <T: Comparable<T>> DataColumn<T?>.min() = asSequence().filterNotNull().minOrNull()

fun <T, R : Comparable<R>> DataColumn<T>.minBy(selector: (T) -> R) = values.asSequence().minByOrNull(selector)

// endregion

// region max

fun <T: Comparable<T>> DataColumn<T?>.max() = asSequence().filterNotNull().maxOrNull()

fun <T, R : Comparable<R>> DataColumn<T>.maxBy(selector: (T) -> R) = values.asSequence().maxByOrNull(selector)

// endregion

// region sum

fun <T : Number> DataColumn<T>.sum() = values.sum(typeClass)

inline fun <T, reified R : Number> DataColumn<T>.sumOf(crossinline expression: (T) -> R): R? =
    Aggregators.sum.cast<R>().of(this, expression)

// endregion

// region mean

fun <T: Number> DataColumn<T?>.mean(skipNa: Boolean = false): Double = Aggregators.mean(skipNa).aggregate(this) ?: Double.NaN

// endregion

// endregion