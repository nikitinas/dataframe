package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.values

public fun <T> DataColumn<T>.all(predicate: Predicate<T>): Boolean = values.all(predicate)
