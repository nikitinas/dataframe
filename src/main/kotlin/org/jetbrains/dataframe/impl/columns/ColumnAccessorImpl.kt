package org.jetbrains.dataframe.impl.columns

import org.jetbrains.dataframe.AnyCol
import org.jetbrains.dataframe.ColumnPath
import org.jetbrains.dataframe.ColumnResolutionContext
import org.jetbrains.dataframe.columns.ColumnAccessor
import org.jetbrains.dataframe.columns.ColumnWithPath
import org.jetbrains.dataframe.isGroup

internal class ColumnAccessorImpl<T>(val path: ColumnPath) : ColumnAccessor<T> {

    override fun name() = path.last()

    override fun path() = path

    constructor(vararg path: String): this(path.toList())

    override fun resolveSingle(context: ColumnResolutionContext): ColumnWithPath<T>? {
        var df = context.df
        var col : AnyCol? = null
        for(colName in path){
            col = df.getColumn<Any?>(colName, context.unresolvedColumnsPolicy) ?: return null
            if(col.isGroup())
                df = col.asGroup().df
        }
        return col?.typed<T>()?.addPath(path, context.df)
    }

    override fun rename(newName: String) = ColumnAccessorImpl<T>(path.dropLast(1) + newName)
}