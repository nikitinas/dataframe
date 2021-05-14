package org.jetbrains.dataframe.columns

import org.jetbrains.dataframe.ColumnKind
import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.DataRow
import kotlin.reflect.KProperty

interface MapColumn<out T> : Column<DataRow<T>>, DataFrame<T> {

    val df: DataFrame<T>

    override fun get(index: Int): DataRow<T>

    override fun slice(range: IntRange): MapColumn<T>

    override fun slice(indices: Iterable<Int>): MapColumn<T>

    override fun slice(mask: BooleanArray): MapColumn<T>

    override fun get(columnName: String): AnyCol

    override fun kind() = ColumnKind.Map

    override fun distinct(): MapDataColumn<T>

    override fun get(firstIndex: Int, vararg otherIndices: Int): MapColumn<T>

    override fun rename(newName: String): MapColumn<T>

    override operator fun getValue(thisRef: Any?, property: KProperty<*>) = super.getValue(thisRef, property) as MapColumn<T>
}