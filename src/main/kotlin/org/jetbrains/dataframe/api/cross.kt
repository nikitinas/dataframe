package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.columns.ColumnSet
import org.jetbrains.dataframe.impl.DataFrameReceiver
import org.jetbrains.dataframe.impl.columns.ColumnWithTargetValues
import org.jetbrains.dataframe.impl.columns.toColumnSet
import org.jetbrains.dataframe.impl.columns.toColumns
import kotlin.reflect.KProperty

interface ColumnValuesReceiver<out T> : SelectReceiver<T>{
    infix fun <C> ColumnReference<C>.into(values: List<C>): ColumnReference<C> = ColumnWithTargetValues(this, values)
}

internal open class ColumnValuesReceiverImpl<T>(df: DataFrameBase<T>, allowMissingColumns: Boolean) :
    DataFrameReceiver<T>(df, allowMissingColumns), ColumnValuesReceiver<T>

typealias ColumnValuesSelector<T, C> = ColumnValuesReceiver<T>.(ColumnValuesReceiver<T>) -> ColumnSet<C>

fun <T, C> DataFrame<T>.cross(rowColumns: ColumnValuesSelector<T,C>) = CrossClause(this, rowColumns) { nrow() }
fun <T> DataFrame<T>.cross(vararg rowColumns: String) = cross { rowColumns.toColumns() }
fun <T, C> DataFrame<T>.cross(vararg rowColumns: KProperty<C>) = cross { rowColumns.toColumns() }
fun <T, C> DataFrame<T>.cross(rowColumns: Iterable<ColumnReference<C>>) = cross { rowColumns.toColumnSet() }

data class CrossClause<T, C, R>(val df: DataFrame<T>, val rows: ColumnValuesSelector<T, C>, val reducer: Reducer<T, R>)

fun <T, C, R> CrossClause<T, C, *>.by(reducer: Reducer<T, R>) = CrossClause(df, rows, reducer)

inline fun <T, C1, C2, reified R> CrossClause<T, C1, R>.with(noinline columns: ColumnSelector<T, C2>) = df.groupBy(rows).spread(columns).with(reducer).into { it?.toString() }
inline fun <T, C1, reified R> CrossClause<T, C1, R>.with(rowColumn: String) = with { rowColumn.toColumnDef() }
inline fun <T, C1, C2, reified R> CrossClause<T, C1, R>.with(rowColumn: KProperty<C2>) = with { rowColumn.toColumnDef() }
inline fun <T, C1, C2, reified R> CrossClause<T, C1, R>.with(rowColumn: ColumnReference<C2>) = with { rowColumn }