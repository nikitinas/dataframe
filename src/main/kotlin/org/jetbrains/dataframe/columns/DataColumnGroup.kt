package org.jetbrains.dataframe.columns

import org.jetbrains.dataframe.DataRow
import kotlin.reflect.KProperty

interface DataColumnGroup<out T> : ColumnGroup<T>, DataColumn<DataRow<T>> {

    override operator fun getValue(thisRef: Any?, property: KProperty<*>): DataColumnGroup<T> = super<DataColumn>.getValue(thisRef, property) as DataColumnGroup<T>

    override fun rename(newName: String): DataColumnGroup<T>

    override fun slice(range: IntRange): DataColumnGroup<T>

    override fun slice(indices: Iterable<Int>): DataColumnGroup<T>

    override fun slice(mask: BooleanArray): DataColumnGroup<T>
}