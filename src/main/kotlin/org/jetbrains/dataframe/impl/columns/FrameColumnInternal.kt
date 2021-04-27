package org.jetbrains.dataframe.impl.columns

import org.jetbrains.dataframe.columns.FrameColumn
import org.jetbrains.dataframe.internal.schema.DataFrameSchema

internal interface FrameColumnInternal<out T> : FrameColumn<T> {
    val schema: Lazy<DataFrameSchema>
}

internal fun <T> FrameColumn<T>.internal() = this as FrameColumnInternal<T>