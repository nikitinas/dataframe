package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.DataColumn

public fun <T> DataColumn<T>.asIterable(): Iterable<T> = values()

public fun <T> DataFrame<T>.asIterable(): Iterable<DataRow<T>> = rows()
