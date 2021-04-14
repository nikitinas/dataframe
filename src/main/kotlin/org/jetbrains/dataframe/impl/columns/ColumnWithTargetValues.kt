package org.jetbrains.dataframe.impl.columns

import org.jetbrains.dataframe.ColumnPath
import org.jetbrains.dataframe.ColumnResolutionContext
import org.jetbrains.dataframe.DataFrameBase
import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.columns.ColumnWithPath
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.name

internal class ColumnWithTargetValues<C>(val source: ColumnReference<C>, val targetValues: List<C>):
    ColumnReference<C> {

    override fun name() = source.name

    override fun resolveSingle(context: ColumnResolutionContext): ColumnWithPath<C>? {
        return source.resolveSingle(context)?.let { ColumnWithPathAndTargetValues(it.data, it.path, it.df, targetValues) }
    }
}

internal class ColumnWithPathAndTargetValues<T>(data: DataColumn<T>, path: ColumnPath, df: DataFrameBase<*>, val targetValues: List<T>) :
    ColumnWithPathImpl<T>(data, path, df)