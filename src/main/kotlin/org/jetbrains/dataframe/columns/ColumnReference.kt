package org.jetbrains.dataframe.columns

import org.jetbrains.dataframe.AnyRow
import org.jetbrains.dataframe.ColumnPath
import org.jetbrains.dataframe.ColumnResolutionContext
import org.jetbrains.dataframe.impl.columns.RenamedColumnReference
import org.jetbrains.dataframe.impl.columns.getColumn
import org.jetbrains.dataframe.impl.columns.addPath

/**
 * Column with type and name/path
 */
interface ColumnReference<out C> : SingleColumn<C> {

    fun name(): String

    fun rename(newName: String): ColumnReference<C>

    fun path(): ColumnPath = listOf(name)

    operator fun invoke(row: AnyRow) = row[this]

    override fun resolveSingle(context: ColumnResolutionContext): ColumnWithPath<C>? {
        return context.df.getColumn<C>(name, context.unresolvedColumnsPolicy)?.addPath(listOf(name), context.df)
    }
}

internal val ColumnReference<*>.name get() = name()

internal fun <C> ColumnReference<C>.renamedReference(newName: String): ColumnReference<C> = RenamedColumnReference(this, newName)

internal fun ColumnReference<*>.shortPath() = listOf(name)