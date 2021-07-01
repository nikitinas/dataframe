package org.jetbrains.dataframe.impl.columns

import org.jetbrains.dataframe.ColumnResolutionContext
import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.columns.ColumnWithPath
import org.jetbrains.dataframe.map
import kotlin.reflect.KType

internal class ConvertedColumnDef<C, R>(val source: ColumnReference<C>, val transform: (C) -> R, val type: KType?) :
    ColumnReference<R> {

    override fun resolveSingle(context: ColumnResolutionContext): ColumnWithPath<R>? {
        return source.resolveSingle(context)?.let { it.data.map(type, transform).addPath(it.path, context.df) }
    }

    override fun name() = source.name()

    override fun rename(newName: String) = ConvertedColumnDef(source.rename(newName), transform, type)
}
