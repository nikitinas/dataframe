package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.DataColumn

//fun <T> DataFrame<T>.count(predicate: RowFilter<T>? = null) = if(predicate == null) nrow() else rows().count { predicate(it, it) }

//fun <T> DataColumn<T>.count(predicate: Predicate<T>? = null) = if(predicate == null) size() else values().count(predicate)