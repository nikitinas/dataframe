package org.jetbrains.dataframe.impl.columns.missing

import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.internal.schema.DataFrameSchema
import org.jetbrains.dataframe.columns.FrameColumn
import org.jetbrains.dataframe.columns.MapColumn
import org.jetbrains.dataframe.impl.columns.FrameColumnInternal
import kotlin.reflect.KType

internal class MissingFrameColumn<T>: MissingDataColumn<DataFrame<T>?>(), FrameColumnInternal<T> {

    override fun kind() = super.kind()

    override fun distinct(): FrameColumn<T> {
        throw UnsupportedOperationException()
    }

    override val schema: DataFrameSchema
        get() = throw UnsupportedOperationException()
}