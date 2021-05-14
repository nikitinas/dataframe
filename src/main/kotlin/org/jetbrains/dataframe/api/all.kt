package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.values

fun <T> DataColumn<T>.all(predicate: Predicate<T>) = values.all(predicate)