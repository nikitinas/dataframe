package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.DataColumn

fun <T> DataFrame<T>.drop(predicate: RowFilter<T>) = filter { !predicate(it, it) }


fun <T> DataColumn<T>.drop(predicate: Predicate<T>) = filter { !predicate(it) }
