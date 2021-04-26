package org.jetbrains.dataframe.api

import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.asDataFrame
import org.jetbrains.dataframe.withValues

fun <T> DataFrame<T>.appendNulls(numberOfRows: Int = 1): DataFrame<T> {
    require(numberOfRows >= 0)
    if (numberOfRows == 0) return this
    return columns().map { col ->
        col.withValues(col.values + arrayOfNulls(numberOfRows), true)
    }.asDataFrame()
}