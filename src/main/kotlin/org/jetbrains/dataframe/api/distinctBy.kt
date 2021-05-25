package org.jetbrains.dataframe

import org.jetbrains.dataframe.impl.columns.toColumnSet
import org.jetbrains.dataframe.impl.columns.toColumns
import kotlin.reflect.KProperty

fun <T> DataFrame<T>.distinctBy(vararg columns: KProperty<*>): DataFrame<T> = distinctBy { columns.toColumns() }
fun <T> DataFrame<T>.distinctBy(vararg columns: String): DataFrame<T> = distinctBy { columns.toColumns() }
fun <T> DataFrame<T>.distinctBy(vararg columns: Column): DataFrame<T> = distinctBy { columns.toColumns() }
@JvmName("distinctByT")
fun <T> DataFrame<T>.distinctBy(columns: Iterable<String>): DataFrame<T> = distinctBy { columns.toColumns() }
fun <T> DataFrame<T>.distinctBy(columns: Iterable<Column>): DataFrame<T> = distinctBy { columns.toColumnSet() }

fun <T, C> DataFrame<T>.distinctBy(columns: ColumnsSelector<T, C>): DataFrame<T> {
    val cols = get(columns)
    val distinctIndices = indices.distinctBy { i -> cols.map { it[i] } }
    return this[distinctIndices]
}

fun <T, R> DataFrame<T>.distinctByExpr(selector: RowSelector<T, R>): DataFrame<T> {
    val distinctIndices = indices.distinctBy {
        val row = get(it)
        selector(row, row)
    }
    return this[distinctIndices]
}