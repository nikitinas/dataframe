package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.AnyCol
import org.jetbrains.dataframe.columns.DataColumn

fun <T> DataFrame<T>.count(predicate: RowFilter<T>) = rows().count { predicate(it, it) }

fun <T, G> GroupedDataFrame<T, G>.countInto(columnName: String) = aggregate {
    nrow() into columnName
}

fun <T, G> GroupedDataFrame<T, G>.count() = countInto("count")

fun AnyCol.count() = size()

fun <T> DataColumn<T>.count(predicate: Predicate<T>) = values().count(predicate)