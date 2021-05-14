package org.jetbrains.dataframe.impl.columns.missing

import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.internal.schema.DataFrameSchema
import org.jetbrains.dataframe.impl.columns.FrameColumnInternal

internal class MissingFrameColumn<T>: MissingDataColumn<DataFrame<T>?>(), FrameColumnInternal<T> {

    override fun kind() = super.kind()

    override fun distinct() = throw UnsupportedOperationException()

    override val schema: Lazy<DataFrameSchema>
        get() = throw UnsupportedOperationException()
}