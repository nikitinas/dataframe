package org.jetbrains.dataframe

import org.jetbrains.dataframe.impl.asList
import org.jetbrains.dataframe.impl.columns.toColumnSet
import org.jetbrains.dataframe.impl.columns.toColumns
import kotlin.reflect.KProperty

fun <T> DataFrame<T>.select(selector: ColumnsSelector<T, *>): DataFrame<T> = new(get(selector))
fun <T> DataFrame<T>.select(vararg columns: KProperty<*>): DataFrame<T> = select(columns.map {it.name})
fun <T> DataFrame<T>.select(vararg columns: String): DataFrame<T> = select(columns.asIterable())
fun <T> DataFrame<T>.select(vararg columns: Column): DataFrame<T> = select { columns.toColumns() }
@JvmName("selectT")
fun <T> DataFrame<T>.select(columns: Iterable<String>): DataFrame<T> = new(getColumns(columns.asList()))
fun <T> DataFrame<T>.select(columns: Iterable<Column>): DataFrame<T> = select { columns.toColumnSet() }

