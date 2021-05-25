package org.jetbrains.dataframe.impl.columns

import org.jetbrains.dataframe.ColumnResolutionContext
import org.jetbrains.dataframe.columns.ColumnWithPath
import org.jetbrains.dataframe.columns.ColumnGroup
import org.jetbrains.dataframe.columns.ValueColumn

internal class ValueColumnWithParent<T>(override val parent: ColumnGroup<*>, override val source: ValueColumn<T>) : ColumnWithParent<T>, ValueColumn<T> by source {

    override fun equals(other: Any?) = source.checkEquals(other)

    override fun hashCode() = source.hashCode()

    override fun path() = super<ColumnWithParent>.path()

    override fun resolve(context: ColumnResolutionContext) = super<ColumnWithParent>.resolve(context)

    override fun resolveSingle(context: ColumnResolutionContext) = super<ColumnWithParent>.resolveSingle(context)
}