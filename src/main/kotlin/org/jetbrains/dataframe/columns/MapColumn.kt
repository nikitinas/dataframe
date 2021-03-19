package org.jetbrains.dataframe.columns

import org.jetbrains.dataframe.*

interface MapColumn<T> : DataColumn<DataRow<T>>, ColumnGroup<T> {

    val df: DataFrame<T>

    fun distinctColumn() = DataColumn.create(name, (this as DataFrame<T>).distinct())

    override fun get(index: Int): DataRow<T> {
        return super<ColumnGroup>.get(index)
    }

    override fun get(columnName: String): AnyCol {
        return super<ColumnGroup>.get(columnName)
    }

    override fun kind() = ColumnKind.Map
}