package org.jetbrains.dataframe.impl.columns

import org.jetbrains.dataframe.ColumnResolutionContext
import org.jetbrains.dataframe.DataRow
import org.jetbrains.dataframe.MapColumnReference
import org.jetbrains.dataframe.columns.AnyCol
import org.jetbrains.dataframe.columns.BaseColumn
import org.jetbrains.dataframe.columns.ColumnGroup
import org.jetbrains.dataframe.columns.ColumnReference
import kotlin.reflect.KProperty

internal class ColumnGroupWithParent<T>(override val parent: MapColumnReference?, override val source: ColumnGroup<T>) : ColumnWithParent<DataRow<T>>, DataColumnGroup<T> by (source as DataColumnGroup<T>) {

    override fun path() = super<ColumnWithParent>.path()

    override fun getValue(thisRef: Any?, property: KProperty<*>): DataColumnGroup<T> = source.getValue(thisRef, property) as DataColumnGroup<T>

    private fun <T> BaseColumn<T>.addParent(parent: ColumnGroup<*>) = (this as DataColumnInternal<T>).addParent(parent)

    override fun get(columnName: String): AnyCol {
        val col = df[columnName]
        return col.addParent(this)
    }
    override fun <R> get(column: ColumnReference<R>) = df[column].addParent(this)
    override fun <R> get(column: ColumnReference<DataRow<R>>) = df[column].addParent(this) as ColumnGroup<R>
    override fun columns() = df.columns().map { it.addParent(this) }
    override fun column(columnIndex: Int) = df.column(columnIndex).addParent(this)

    override fun equals(other: Any?) = source.checkEquals(other)

    override fun hashCode() = source.hashCode()

    override fun resolveSingle(context: ColumnResolutionContext) = super<ColumnWithParent>.resolveSingle(context)

    override fun resolve(context: ColumnResolutionContext) = super<ColumnWithParent>.resolve(context)
}
