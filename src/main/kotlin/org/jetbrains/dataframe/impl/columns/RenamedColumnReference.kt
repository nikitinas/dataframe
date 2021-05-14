package org.jetbrains.dataframe.impl.columns

import org.jetbrains.dataframe.*
import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.columns.ColumnWithPath

internal class RenamedColumnReference<C>(val source: ColumnReference<C>, val name: String) : ColumnReference<C> {

    override fun resolveSingle(context: ColumnResolutionContext): ColumnWithPath<C>? {

        return source.resolveSingle(context)?.let { it.data.rename(name).addPath(it.path, context.df) }
    }

    override fun name() = name

    override fun rename(newName: String) = RenamedColumnReference(source, newName)
}