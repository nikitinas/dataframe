package org.jetbrains.dataframe.columns

import org.jetbrains.dataframe.DataRow
import kotlin.reflect.KProperty

interface MapDataColumn<out T> : MapColumn<T>, DataColumn<DataRow<T>> {

    override operator fun getValue(thisRef: Any?, property: KProperty<*>): MapDataColumn<T> = super<DataColumn>.getValue(thisRef, property) as MapDataColumn<T>

    override fun rename(newName: String): MapDataColumn<T>

    override fun slice(range: IntRange): MapDataColumn<T>

    override fun slice(indices: Iterable<Int>): MapDataColumn<T>

    override fun slice(mask: BooleanArray): MapDataColumn<T>
}