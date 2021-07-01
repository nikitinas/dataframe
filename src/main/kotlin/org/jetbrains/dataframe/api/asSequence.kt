package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.DataColumn

public fun <T> DataColumn<T>.asSequence(): Sequence<T> = asIterable().asSequence()

public fun <T> DataFrame<T>.asSequence(): Sequence<DataRow<T>> = asIterable().asSequence()
