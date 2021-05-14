package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.DataColumn

fun <T: Comparable<T>> DataColumn<T>.between(left: T, right: T, includeBoundaries: Boolean = true) = map { it.between(left, right, includeBoundaries) }