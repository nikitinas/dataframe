package org.jetbrains.dataframe.columns

import org.jetbrains.dataframe.AnyRow
import org.jetbrains.dataframe.ColumnPath
import org.jetbrains.dataframe.ColumnResolutionContext
import org.jetbrains.dataframe.impl.columns.getColumn
import org.jetbrains.dataframe.impl.columns.addPath
import org.jetbrains.dataframe.name

interface ColumnReference<out C> : SingleColumn<C> {

    fun name(): String

    fun path(): ColumnPath = listOf(name)

    operator fun invoke(row: AnyRow) = row[this]

    override fun resolveSingle(context: ColumnResolutionContext): ColumnWithPath<C>? {
        return context.df.getColumn<C>(name, context.unresolvedColumnsPolicy)?.addPath(listOf(name), context.df)
    }

}