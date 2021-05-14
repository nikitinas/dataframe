package org.jetbrains.dataframe.impl.columns

import org.jetbrains.dataframe.ColumnResolutionContext
import org.jetbrains.dataframe.columns.ColumnWithPath
import org.jetbrains.dataframe.columns.Column

internal interface DataColumnWithParent<C> : ColumnWithParent<C>, Column<C> {

    override fun resolveSingle(context: ColumnResolutionContext): ColumnWithPath<C>? {
        return super<ColumnWithParent>.resolveSingle(context)
    }
}