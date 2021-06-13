package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.columns.DataColumn
import kotlin.reflect.KProperty

inline fun <reified T : Comparable<T>> DataColumn<T?>.median() = asSequence().filterNotNull().asIterable().median()

inline fun <T, reified D : Comparable<D>> DataFrame<T>.median(col: ColumnReference<D?>): Double = get(col).median()
inline fun <T, reified D : Comparable<D>> DataFrame<T>.median(crossinline selector: RowSelector<T, D?>): Double =
    rows().asSequence().map {
        selector(
            it,
            it
        )
    }.filterNotNull().asIterable().median()

inline fun <T, reified D : Comparable<D>> DataFrame<T>.median(col: KProperty<D?>): Double = get(col).median()

inline fun <reified T : Comparable<T>> Iterable<T>.median(): Double {
    val sorted = sorted()
    val size = sorted.size
    val index = size / 2
    return when (T::class) {
        Double::class -> if (size % 2 == 0) (sorted[index - 1] as Double + sorted[index] as Double) / 2.0 else sorted[index] as Double
        Int::class -> if (size % 2 == 0) (sorted[index - 1] as Int + sorted[index] as Int) / 2.0 else (sorted[index] as Int).toDouble()
        Long::class -> if (size % 2 == 0) (sorted[index - 1] as Long + sorted[index] as Long) / 2.0 else (sorted[index] as Long).toDouble()
        else -> throw IllegalArgumentException()
    }
}

inline fun <T, G, reified R : Comparable<R>> GroupedDataFrame<T, G>.median(columnName: String = "median", noinline selector: RowSelector<G, R>) =
    aggregate { median(selector) into columnName }