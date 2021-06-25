package org.jetbrains.dataframe.impl.groupBy

import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.DataRow
import org.jetbrains.dataframe.GroupedDataRow
import org.jetbrains.dataframe.columns.FrameColumn

internal class GroupedDataRowImpl<T, G>(private val row: DataRow<T>, private val frameCol: FrameColumn<G>): GroupedDataRow<T, G>, DataRow<T> by row {

    override fun group() = groupOrNull()!!

    override fun groupOrNull() = frameCol[row.index()]
}