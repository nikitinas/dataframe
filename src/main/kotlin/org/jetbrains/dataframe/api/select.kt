package org.jetbrains.dataframe

import org.jetbrains.dataframe.impl.columns.toColumnSet
import org.jetbrains.dataframe.impl.columns.toColumns
import kotlin.reflect.KProperty

fun <T> DataFrame<T>.select(selector: ColumnsSelector<T, *>): DataFrame<T> = new(this[selector])
fun <T> DataFrame<T>.select(vararg columns: KProperty<*>): DataFrame<T> = select { columns.toColumns() }
fun <T> DataFrame<T>.select(vararg columns: String): DataFrame<T> = select { columns.toColumns() }
fun <T> DataFrame<T>.select(vararg columns: Column): DataFrame<T> = select { columns.toColumns() }
fun <T> DataFrame<T>.select(columns: Iterable<Column>): DataFrame<T> = select { columns.toColumnSet() }

