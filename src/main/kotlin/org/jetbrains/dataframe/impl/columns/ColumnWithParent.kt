package org.jetbrains.dataframe.impl.columns

import org.jetbrains.dataframe.ColumnResolutionContext
import org.jetbrains.dataframe.MapColumnReference
import org.jetbrains.dataframe.columns.BaseColumn
import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.columns.ColumnWithPath

internal interface ColumnWithParent<out C> : ColumnReference<C> {

    val parent: MapColumnReference?

    val source: BaseColumn<C>

    override fun path() = parent?.path()?.plus(name()) ?: listOf(name())

    override fun resolveSingle(context: ColumnResolutionContext): ColumnWithPath<C>? {

        val parentDef = parent
        val (targetDf, pathPrefix) = when(parentDef) {
            null -> context.df to emptyList()
            else -> {
                val parentCol = parentDef.resolveSingle(context) ?: return null
                val group = parentCol.data.asGroup()
                group.df to parentCol.path
            }
        }

        val data = targetDf.getColumn<C>(name(), context.unresolvedColumnsPolicy)
        return data?.addPath(pathPrefix + name(), context.df)
    }
}