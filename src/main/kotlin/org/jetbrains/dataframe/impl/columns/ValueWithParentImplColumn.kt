package org.jetbrains.dataframe.impl.columns

import org.jetbrains.dataframe.ColumnResolutionContext
import org.jetbrains.dataframe.columns.ColumnWithPath
import org.jetbrains.dataframe.columns.ColumnGroup
import org.jetbrains.dataframe.columns.ValueColumn

internal class ValueWithParentImplColumn<T>(parent: ColumnGroup<*>, source: ValueColumn<T>) : DataColumnWithParentImpl<T>(parent, source), ValueColumn<T> by source {

    override fun kind() = super<ValueColumn>.kind()

    override fun resolve(context: ColumnResolutionContext): List<ColumnWithPath<T>> {
        return super<DataColumnWithParentImpl>.resolve(context)
    }

    override fun resolveSingle(context: ColumnResolutionContext): ColumnWithPath<T>? {
        return super<DataColumnWithParentImpl>.resolveSingle(context)
    }
}