package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.impl.columns.toColumnSet
import org.jetbrains.dataframe.impl.columns.toColumns
import kotlin.reflect.KProperty

public fun <T, C> DataFrame<T>.fillNulls(cols: ColumnsSelector<T, C>): UpdateClause<T, C> = update(cols).where { it == null }
public fun <T> DataFrame<T>.fillNulls(vararg cols: String): UpdateClause<T, Any?> = fillNulls { cols.toColumns() }
public fun <T, C> DataFrame<T>.fillNulls(vararg cols: KProperty<C>): UpdateClause<T, C> = fillNulls { cols.toColumns() }
public fun <T, C> DataFrame<T>.fillNulls(vararg cols: ColumnReference<C>): UpdateClause<T, C> = fillNulls { cols.toColumns() }
public fun <T, C> DataFrame<T>.fillNulls(cols: Iterable<ColumnReference<C>>): UpdateClause<T, C> = fillNulls { cols.toColumnSet() }
