package org.jetbrains.dataframe

import org.jetbrains.dataframe.impl.columns.toColumnSet
import org.jetbrains.dataframe.impl.columns.toColumns
import kotlin.reflect.KProperty

public fun <T, C> DataFrame<T>.distinct(columns: ColumnsSelector<T, C>): DataFrame<T> = select(columns).distinct()
public fun <T> DataFrame<T>.distinct(vararg columns: KProperty<*>): DataFrame<T> = distinct { columns.toColumns() }
public fun <T> DataFrame<T>.distinct(vararg columns: String): DataFrame<T> = distinct { columns.toColumns() }
public fun <T> DataFrame<T>.distinct(vararg columns: Column): DataFrame<T> = distinct { columns.toColumns() }
@JvmName("distinctT")
public fun <T> DataFrame<T>.distinct(columns: Iterable<String>): DataFrame<T> = distinct { columns.toColumns() }
public fun <T> DataFrame<T>.distinct(columns: Iterable<Column>): DataFrame<T> = distinct { columns.toColumnSet() }

public fun <T> DataFrame<T>.distinct(): DataFrame<T> = distinctBy { all() }
