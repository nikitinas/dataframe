package org.jetbrains.dataframe

import org.jetbrains.dataframe.impl.GroupedDataFrameImpl
import org.jetbrains.dataframe.impl.columns.asTable

fun <T, G> DataFrame<T>.toGrouped(selector: ColumnSelector<T, DataFrame<G>?>): GroupedDataFrame<T, G> {
    val column = column(selector).asTable()
    return GroupedDataFrameImpl(this, column)
}