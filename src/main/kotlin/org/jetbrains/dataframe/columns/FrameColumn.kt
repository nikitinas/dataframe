package org.jetbrains.dataframe.columns

import org.jetbrains.dataframe.ColumnKind
import org.jetbrains.dataframe.DataFrame


interface FrameColumn<out T> : DataColumn<DataFrame<T>?>, NestedColumn<T> {

    override fun kind() = ColumnKind.Frame

    fun distinct(): FrameColumn<T>
}