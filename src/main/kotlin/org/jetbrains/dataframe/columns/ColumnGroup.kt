package org.jetbrains.dataframe.columns

import org.jetbrains.dataframe.ColumnKind
import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.DataRow
import kotlin.reflect.KProperty

interface ColumnGroup<out T> : Column<DataRow<T>>, DataFrame<T> {

    val df: DataFrame<T>

    override fun get(index: Int): DataRow<T>

    override fun slice(range: IntRange): ColumnGroup<T>

    override fun slice(indices: Iterable<Int>): ColumnGroup<T>

    override fun slice(mask: BooleanArray): ColumnGroup<T>

    override fun get(columnName: String): AnyCol

    override fun kind() = ColumnKind.Group

    override fun distinct(): ColumnGroup<T>

    override fun get(firstIndex: Int, vararg otherIndices: Int): ColumnGroup<T>

    override fun rename(newName: String): ColumnGroup<T>

    override operator fun getValue(thisRef: Any?, property: KProperty<*>) = super.getValue(thisRef, property) as ColumnGroup<T>
}