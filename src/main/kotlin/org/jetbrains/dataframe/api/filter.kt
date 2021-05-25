package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.impl.columns.toColumnSet
import org.jetbrains.dataframe.impl.columns.toColumns
import kotlin.reflect.KProperty

fun <T> DataFrame<T>.filter(predicate: RowFilter<T>): DataFrame<T> =
        (0 until nrow()).filter {
            val row = get(it)
            predicate(row, row)
        }.let { get(it) }


fun <T> DataColumn<T>.filter(predicate: Predicate<T>) = slice(isMatching(predicate))

internal fun <T> DataFrame<T>.filterFast(predicate: VectorizedRowFilter<T>) = this[predicate(this)]