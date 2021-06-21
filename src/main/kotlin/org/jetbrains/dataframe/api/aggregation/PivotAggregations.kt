package org.jetbrains.dataframe.api.aggregation

import org.jetbrains.dataframe.Column
import org.jetbrains.dataframe.ColumnSelector
import org.jetbrains.dataframe.ColumnsSelector
import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.impl.aggregation.DataFramePivotImpl
import org.jetbrains.dataframe.DataRow
import org.jetbrains.dataframe.GroupedPivotAggregations
import org.jetbrains.dataframe.RowFilter
import org.jetbrains.dataframe.RowSelector
import org.jetbrains.dataframe.aggregation.Aggregatable
import org.jetbrains.dataframe.aggregation.AggregateColumnsSelector
import org.jetbrains.dataframe.aggregation.PivotAggregateBody
import org.jetbrains.dataframe.impl.columns.toColumns
import org.jetbrains.dataframe.with
import kotlin.reflect.KProperty

interface PivotAggregations<T> : Aggregatable<T> {

    fun <R> aggregate(body: PivotAggregateBody<T, R>): DataRow<T> = asGrouped().aggregate(body)[0]

    fun plain(): DataRow<T> = aggregate { this }

    fun groupBy(columns: ColumnsSelector<T, *>): GroupedPivotAggregations<T>
    fun groupBy(vararg columns: String) = groupBy { columns.toColumns() }
    fun groupBy(vararg columns: Column) = groupBy { columns.toColumns() }

    fun count(predicate: RowFilter<T>? = null): DataRow<T> = asGrouped().count(predicate)[0]

    fun values(vararg columns: Column, separate: Boolean = false) = values(separate) { columns.toColumns() }
    fun values(vararg columns: String, separate: Boolean = false) = values(separate) { columns.toColumns() }
    fun values(vararg columns: KProperty<*>, separate: Boolean = false) = values(separate) { columns.toColumns() }
    fun values(separate: Boolean = false, columns: AggregateColumnsSelector<T, *>): DataRow<T> = asGrouped().values(separate, columns)[0]

    fun values(separate: Boolean = false): DataRow<T> = asGrouped().values(separate)[0]

    // region min

    fun min(separate: Boolean = false): DataRow<T> = asGrouped().min(separate)[0]

    fun <R : Comparable<R>> minFor(separate: Boolean = false, columns: ColumnsSelector<T, R?>): DataRow<T> = asGrouped().minFor(separate, columns)[0]

    fun <R : Comparable<R>> min(columns: ColumnsSelector<T, R?>): DataRow<T> = asGrouped().min(columns)[0]

    fun <R : Comparable<R>> minOf(rowExpression: RowSelector<T, R>): DataRow<T> = asGrouped().minOf(rowExpression)[0]

    fun <R : Comparable<R>> minBy(column: ColumnSelector<T, R>): DataRow<T> = asGrouped().minBy(column)[0]

    fun <R : Comparable<R>> minByExpr(rowExpression: RowSelector<T, R>): DataRow<T> = asGrouped().minByExpr(rowExpression)[0]

    // endregion

    // region max

    fun max(separate: Boolean = false): DataRow<T> = asGrouped().max(separate)[0]

    fun <R : Comparable<R>> maxFor(separate: Boolean = false, columns: ColumnsSelector<T, R?>): DataRow<T> = asGrouped().maxFor(separate, columns)[0]

    fun <R : Comparable<R>> max(columns: ColumnsSelector<T, R?>): DataRow<T> = asGrouped().max(columns)[0]

    fun <R : Comparable<R>> maxOf(rowExpression: RowSelector<T, R>): DataRow<T> = asGrouped().maxOf(rowExpression)[0]

    fun <R : Comparable<R>> maxBy(column: ColumnSelector<T, R>): DataRow<T> = asGrouped().maxBy(column)[0]

    fun <R : Comparable<R>> maxByExpr(rowExpression: RowSelector<T, R>): DataRow<T> = asGrouped().maxByExpr(rowExpression)[0]

    // endregion
}

@PublishedApi
internal fun <T> PivotAggregations<T>.asGrouped() = (this as DataFramePivotImpl<T>).groupBy { none() }

// region inlines

inline fun <T, reified V> PivotAggregations<T>.with(noinline selector: RowSelector<T, V>): DataRow<T> = asGrouped().with(selector)[0]

// endregion