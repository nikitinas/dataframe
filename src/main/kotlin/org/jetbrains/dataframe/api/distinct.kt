package org.jetbrains.dataframe

import org.jetbrains.dataframe.impl.columns.toColumnSet
import org.jetbrains.dataframe.impl.columns.toColumns
import kotlin.reflect.KProperty

fun <T, C> DataFrame<T>.distinct(columns: ColumnsSelector<T, C>) = select(columns).distinct()
fun <T> DataFrame<T>.distinct(vararg columns: KProperty<*>): DataFrame<T> = distinct { columns.toColumns() }
fun <T> DataFrame<T>.distinct(vararg columns: String): DataFrame<T> = distinct { columns.toColumns() }
fun <T> DataFrame<T>.distinct(vararg columns: Column): DataFrame<T> = distinct { columns.toColumns() }
@JvmName("distinctT")
fun <T> DataFrame<T>.distinct(columns: Iterable<String>): DataFrame<T> = distinct { columns.toColumns() }
fun <T> DataFrame<T>.distinct(columns: Iterable<Column>): DataFrame<T> = distinct { columns.toColumnSet() }

fun <T> DataFrame<T>.distinct() = distinctBy { all() }