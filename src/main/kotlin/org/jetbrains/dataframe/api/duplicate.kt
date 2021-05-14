package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.MapColumn
import org.jetbrains.dataframe.columns.type
import kotlin.reflect.full.withNullability

fun <T> DataRowBase<T>.duplicate(n: Int): DataFrame<T> = (this as DataRow<T>).owner.columns().mapIndexed { colIndex, col ->
    when(col) {
        is MapColumn<*> -> DataColumn.create(col.name, col[index].duplicate(n))
        else -> {
            val value = col[index]
            if (value is AnyFrame)
                DataColumn.create(col.name, MutableList(n) { value })
            else DataColumn.create(col.name, MutableList(n) { value }, col.type.withNullability(value == null))
        }
    }
}.asDataFrame()