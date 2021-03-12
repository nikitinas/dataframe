package org.jetbrains.dataframe.impl.columns.missing

import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.columns.FrameColumn

internal class MissingFrameColumn<T>: MissingDataColumn<DataFrame<T>?>(), FrameColumn<T> {
    override val df: DataFrame<T>
        get() = throw UnsupportedOperationException()


    override fun kind() = super.kind()

    override fun distinct(): FrameColumn<T> {
        throw UnsupportedOperationException()
    }
}