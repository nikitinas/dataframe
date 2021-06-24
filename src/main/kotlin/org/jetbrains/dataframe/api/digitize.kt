package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.DataColumn
import kotlin.reflect.KClass

public fun DataColumn<Double>.digitize(vararg bins: Int, right: Boolean = false): DataColumn<Int> = digitize(bins.toList(), Double::class, right)

public fun <T : Comparable<T>> DataColumn<T>.digitize(vararg bins: T, right: Boolean = false): DataColumn<Int> = digitize(bins.toList(), right)

public fun <T : Comparable<T>> DataColumn<T>.digitize(bins: List<Int>, kclass: KClass<T>, right: Boolean = false): DataColumn<Int> = digitize(
    bins.toList().map { org.jetbrains.dataframe.impl.convert(it, kclass) },
    right
)

public fun <T : Comparable<T>> DataColumn<T>.digitize(bins: List<T>, right: Boolean = false): DataColumn<Int> {
    // TODO: use binary search
    // TODO: support descending order of bins
    val predicate: (T, T) -> Boolean = if (right) { value, bin -> value <= bin } else { value, bin -> value < bin }

    return map { value ->
        val index = bins.indexOfFirst { predicate(value, it) }
        if (index == -1) bins.size
        else index
    }
}
