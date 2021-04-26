package org.jetbrains.dataframe.impl.columns

import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.MapColumn
import kotlin.reflect.KType

internal interface DataColumnInternal<T> : DataColumn<T> {

    fun changeType(type: KType): DataColumn<T>
    fun rename(newName: String): DataColumn<T>
    fun addParent(parent: MapColumn<*>): DataColumn<T>
    override fun named(name: String) = rename(name)
}