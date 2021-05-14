package org.jetbrains.dataframe.columns

import org.jetbrains.dataframe.ColumnResolutionContext

interface SingleColumn<out C> : Columns<C> {

    override fun resolve(context: ColumnResolutionContext) = resolveSingle(context)?.let { listOf(it) } ?: emptyList()

    fun resolveSingle(context: ColumnResolutionContext): ColumnWithPath<C>?
}