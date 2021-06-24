package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.DataColumn

public fun <T : Comparable<T>> DataColumn<T>.between(left: T, right: T, includeBoundaries: Boolean = true): DataColumn<Boolean> = map { it.between(left, right, includeBoundaries) }
