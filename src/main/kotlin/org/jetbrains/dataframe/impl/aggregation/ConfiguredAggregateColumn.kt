package org.jetbrains.dataframe.impl.aggregation

import org.jetbrains.dataframe.ColumnPath
import org.jetbrains.dataframe.ColumnResolutionContext
import org.jetbrains.dataframe.columns.ColumnWithPath
import org.jetbrains.dataframe.columns.Columns
import org.jetbrains.dataframe.columns.shortPath

internal class ConfiguredAggregateColumn<C> private constructor(
    val columns: Columns<C>,
    private val default: C? = null,
    private val newPath: ColumnPath? = null
) : Columns<C> {

    private fun ColumnWithPath<C>.toDescriptor(keepName: Boolean) = when (val col = this) {
        is AggregateColumnDescriptor<C> -> {
            val path = if (keepName) newPath?.plus(col.newPath ?: col.column.shortPath()) ?: col.newPath
            else newPath ?: col.newPath
            AggregateColumnDescriptor(col.column, default ?: col.default, path)
        }
        else -> AggregateColumnDescriptor(col, default, if (keepName) newPath?.plus(col.name) else newPath)
    }

    override fun resolve(context: ColumnResolutionContext): List<ColumnWithPath<C>> {
        val resolved = columns.resolve(context)
        if (resolved.size == 1) return listOf(resolved[0].toDescriptor(false))
        else return resolved.map {
            it.toDescriptor(true)
        }
    }

    companion object {

        fun <C> withDefault(src: Columns<C>, default: C?): Columns<C> = when (src) {
            is ConfiguredAggregateColumn<C> -> ConfiguredAggregateColumn(src.columns, default, src.newPath)
            else -> ConfiguredAggregateColumn(src, default, null)
        }

        fun <C> withPath(src: Columns<C>, newPath: ColumnPath): Columns<C> = when (src) {
            is ConfiguredAggregateColumn<C> -> ConfiguredAggregateColumn(src.columns, src.default, newPath)
            else -> ConfiguredAggregateColumn(src, null, newPath)
        }
    }
}
