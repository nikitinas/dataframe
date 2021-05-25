package org.jetbrains.dataframe

import org.jetbrains.dataframe.impl.columns.toColumnSet
import org.jetbrains.dataframe.impl.columns.toColumns
import kotlin.reflect.KProperty

fun <T> DataFrame<T>.dropNulls(whereAllNull: Boolean = false, selector: ColumnsSelector<T, *>): DataFrame<T> {
    val cols = this[selector]
    return if(whereAllNull) drop { row -> cols.all { col -> col[row] == null } }
    else drop { row -> cols.any { col -> col[row] == null } }
}

fun <T> DataFrame<T>.dropNulls(whereAllNull: Boolean = false) = dropNulls(whereAllNull) { all() }
fun <T> DataFrame<T>.dropNulls(vararg cols: KProperty<*>, whereAllNull: Boolean = false) = dropNulls(whereAllNull) { cols.toColumns() }
fun <T> DataFrame<T>.dropNulls(vararg cols: String, whereAllNull: Boolean = false) = dropNulls(whereAllNull) { cols.toColumns() }
fun <T> DataFrame<T>.dropNulls(vararg cols: Column, whereAllNull: Boolean = false) = dropNulls(whereAllNull) { cols.toColumns() }
fun <T> DataFrame<T>.dropNulls(cols: Iterable<Column>, whereAllNull: Boolean = false) = dropNulls(whereAllNull) { cols.toColumnSet() }