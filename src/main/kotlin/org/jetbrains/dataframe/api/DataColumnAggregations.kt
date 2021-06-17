package org.jetbrains.dataframe.api

import org.jetbrains.dataframe.Predicate
import org.jetbrains.dataframe.asSequence
import org.jetbrains.dataframe.columns.BaseColumn
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.typeClass
import org.jetbrains.dataframe.columns.values
import org.jetbrains.dataframe.impl.aggregation.aggregators.Aggregators
import org.jetbrains.dataframe.impl.aggregation.modes.of
import org.jetbrains.dataframe.sum

interface DataColumnAggregations<out T>: BaseColumn<T> {

    fun count(predicate: Predicate<T>? = null) = if(predicate == null) size() else values().count(predicate)
}

// region inlines

fun <T: Number> DataColumn<T?>.mean(skipNa: Boolean = false): Double = Aggregators.mean(skipNa).aggregate(this) ?: Double.NaN

inline fun <T, reified R : Number> DataColumn<T>.sumOf(crossinline expression: (T) -> R): R? =
    Aggregators.sum.cast<R>().of(this, expression)

fun <T: Comparable<T>> DataColumn<T?>.min() = asSequence().filterNotNull().minOrNull()
fun <T: Comparable<T>> DataColumn<T?>.max() = asSequence().filterNotNull().maxOrNull()

fun <T : Number> DataColumn<T>.sum() = values.sum(typeClass)

fun <T, R : Comparable<R>> DataColumn<T>.minBy(selector: (T) -> R) = values.asSequence().minByOrNull(selector)
fun <T, R : Comparable<R>> DataColumn<T>.maxBy(selector: (T) -> R) = values.asSequence().maxByOrNull(selector)

// endregion