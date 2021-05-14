package org.jetbrains.dataframe.impl.columns

import org.jetbrains.dataframe.ColumnResolutionContext
import org.jetbrains.dataframe.columns.ColumnWithPath
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.ColumnGroup

internal abstract class DataColumnWithParentImpl<T>(override val parent: ColumnGroup<*>, val source: DataColumn<T>) : DataColumnWithParent<T>, DataColumn<T> by source {

    override fun equals(other: Any?) = checkEquals(other)

    override fun hashCode() = getHashCode()

    override fun resolveSingle(context: ColumnResolutionContext): ColumnWithPath<T>? {
        return super<DataColumnWithParent>.resolveSingle(context)
    }

    override fun resolve(context: ColumnResolutionContext): List<ColumnWithPath<T>> {
        return super<DataColumnWithParent>.resolve(context)
    }
}