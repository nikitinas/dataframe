package org.jetbrains.dataframe.columns

import org.jetbrains.dataframe.ColumnResolutionContext

public interface SingleColumn<out C> : Columns<C> {

    override fun resolve(context: ColumnResolutionContext): List<ColumnWithPath<C>> = resolveSingle(context)?.let { listOf(it) } ?: emptyList()

    public fun resolveSingle(context: ColumnResolutionContext): ColumnWithPath<C>?
}
