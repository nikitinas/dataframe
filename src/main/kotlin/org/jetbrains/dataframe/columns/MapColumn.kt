package org.jetbrains.dataframe.columns

import org.jetbrains.dataframe.*

interface MapColumn<T> : DataColumn<DataRow<T>>, ColumnGroup<T> {

    val df: DataFrame<T>

    override fun get(index: Int): DataRow<T>

    override fun get(columnName: String): AnyCol

    override fun kind() = ColumnKind.Map

    override fun distinct(): MapColumn<T>

    override fun get(firstIndex: Int, vararg otherIndices: Int): MapColumn<T>
}