package org.jetbrains.dataframe.columns

import org.jetbrains.dataframe.ColumnKind
import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.DataRow
import kotlin.reflect.KProperty

public interface ColumnGroup<out T> : BaseColumn<DataRow<T>>, DataFrame<T> {

    public val df: DataFrame<T>

    override fun get(index: Int): DataRow<T>

    override fun slice(range: IntRange): ColumnGroup<T>

    override fun slice(indices: Iterable<Int>): ColumnGroup<T>

    override fun slice(mask: BooleanArray): ColumnGroup<T>

    override fun get(columnName: String): AnyCol

    override fun kind(): ColumnKind = ColumnKind.Group

    override fun distinct(): ColumnGroup<T>

    override fun get(firstIndex: Int, vararg otherIndices: Int): ColumnGroup<T>

    override fun rename(newName: String): ColumnGroup<T>

    override operator fun getValue(thisRef: Any?, property: KProperty<*>): ColumnGroup<T> = super.getValue(thisRef, property) as ColumnGroup<T>
}
