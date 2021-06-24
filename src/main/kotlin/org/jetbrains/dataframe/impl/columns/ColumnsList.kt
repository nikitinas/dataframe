package org.jetbrains.dataframe.impl.columns

import org.jetbrains.dataframe.ColumnResolutionContext
import org.jetbrains.dataframe.columns.Columns

internal class ColumnsList<C>(val columns: List<Columns<C>>) : Columns<C> {
    constructor(vararg columns: Columns<C>) : this(columns.toList())

    override fun resolve(context: ColumnResolutionContext) = columns.flatMap { it.resolve(context) }
}
