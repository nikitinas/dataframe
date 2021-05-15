package org.jetbrains.dataframe.impl.columns

import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.DataRow
import org.jetbrains.dataframe.columns.ColumnGroup
import org.jetbrains.dataframe.columns.DataColumn
import kotlin.reflect.KProperty

internal interface DataColumnGroup<out T> : ColumnGroup<T>, DataColumn<DataRow<T>> {

    override operator fun getValue(thisRef: Any?, property: KProperty<*>): DataColumnGroup<T> = super<DataColumn>.getValue(thisRef, property) as DataColumnGroup<T>

    override fun iterator() = super<ColumnGroup>.iterator()

    override fun rename(newName: String): DataColumnGroup<T>

    override fun slice(range: IntRange): DataColumnGroup<T>

    override fun slice(indices: Iterable<Int>): DataColumnGroup<T>

    override fun slice(mask: BooleanArray): DataColumnGroup<T>

    override fun distinct(): DataColumnGroup<T>
}