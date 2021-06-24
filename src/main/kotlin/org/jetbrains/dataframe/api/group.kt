package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.ColumnWithPath
import org.jetbrains.dataframe.impl.columns.toColumnSet
import org.jetbrains.dataframe.impl.columns.toColumns
import kotlin.reflect.KProperty

public data class GroupClause<T, C>(val df: DataFrame<T>, val selector: ColumnsSelector<T, C>)

public fun <T> DataFrame<T>.group(cols: Iterable<Column>): GroupClause<T, Any?> = group { cols.toColumnSet() }
public fun <T> DataFrame<T>.group(vararg cols: KProperty<*>): GroupClause<T, Any?> = group { cols.toColumns() }
public fun <T> DataFrame<T>.group(vararg cols: String): GroupClause<T, Any?> = group { cols.toColumns() }
public fun <T> DataFrame<T>.group(vararg cols: Column): GroupClause<T, Any?> = group { cols.toColumns() }
public fun <T, C> DataFrame<T>.group(cols: ColumnsSelector<T, C>): GroupClause<T, C> = GroupClause(this, cols)

public fun <T, C> GroupClause<T, C>.into(groupName: String): DataFrame<T> = into { groupName }
public fun <T, C> GroupClause<T, C>.into(groupRef: MapColumnReference): DataFrame<T> = df.move(selector).under(groupRef)
public fun <T, C> GroupClause<T, C>.into(groupName: ColumnWithPath<C>.(ColumnWithPath<C>) -> String): DataFrame<T> = df.move(selector).under { path(groupName(it, it)) }
