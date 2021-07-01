package org.jetbrains.dataframe.impl.aggregation

import org.jetbrains.dataframe.*
import org.jetbrains.dataframe.aggregation.Aggregatable
import org.jetbrains.dataframe.columns.AnyCol
import org.jetbrains.dataframe.columns.guessColumnType

internal inline fun <T> Aggregatable<T>.remainingColumns(crossinline predicate: (AnyCol) -> Boolean): ColumnsSelector<T, Any?> =
    remainingColumnsSelector().filter { predicate(it.data) }

internal fun <T> Aggregatable<T>.comparableColumns() = remainingColumns { it.isComparable() } as ColumnsSelector<T, Comparable<Any?>>

internal fun <T> Aggregatable<T>.numberColumns() = remainingColumns { it.isNumber() } as ColumnsSelector<T, Number>

internal fun NamedValue.toColumnWithPath() = path to guessColumnType(
    path.last(),
    listOf(value),
    type,
    guessType,
    default
)
