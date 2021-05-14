package org.jetbrains.dataframe.impl.columns

import org.jetbrains.dataframe.*
import org.jetbrains.dataframe.columns.ColumnWithPath
import org.jetbrains.dataframe.columns.ColumnGroup

internal class FrameColumnWithParent<T>(parent: ColumnGroup<*>, source: FrameColumnInternal<T>) : DataColumnWithParentImpl<DataFrame<T>?>(parent, source), FrameColumnInternal<T> by source {

    override fun kind() = super<FrameColumnInternal>.kind()

    override fun resolve(context: ColumnResolutionContext): List<ColumnWithPath<DataFrame<T>?>> {
        return super<DataColumnWithParentImpl>.resolve(context)
    }

    override fun resolveSingle(context: ColumnResolutionContext): ColumnWithPath<DataFrame<T>?>? {
        return super<DataColumnWithParentImpl>.resolveSingle(context)
    }
}