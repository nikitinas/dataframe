package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.FrameColumn

public fun <T> DataFrame<T>.chunked(size: Int): FrameColumn<T> {
    val startIndices = (0 until nrow() step size)
    return DataColumn.create("", this, startIndices, false)
}
