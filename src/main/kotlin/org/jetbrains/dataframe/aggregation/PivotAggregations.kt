package org.jetbrains.dataframe.aggregation

import org.jetbrains.dataframe.AggregateBody
import org.jetbrains.dataframe.Column
import org.jetbrains.dataframe.ColumnPath
import org.jetbrains.dataframe.ColumnsSelector
import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.PivotAggregateBody
import org.jetbrains.dataframe.RowFilter
import org.jetbrains.dataframe.RowSelector
import org.jetbrains.dataframe.impl.aggregation.aggregators.Aggregators
import org.jetbrains.dataframe.impl.aggregation.modes.aggregateAll
import org.jetbrains.dataframe.maxBy
import org.jetbrains.dataframe.minBy
import kotlin.reflect.KProperty

interface PivotAggregations<T> : PivotOrGroupByAggregations<T> {

    fun count(predicate: RowFilter<T>? = null) = aggregateBase { count(predicate) default 0 }

    fun <R> matches(yes: R, no: R) = aggregate { yes default no }
    fun matches() = matches(true, false)

    override fun <R> aggregateBase(body: AggregateBody<T, R>) = aggregate(body as PivotAggregateBody<T, R>)

    fun <R> aggregate(body: PivotAggregateBody<T, R>): DataFrame<T>

    fun groupByValue(flag: Boolean = true): PivotAggregations<T>
    fun withDefault(value: Any?): PivotAggregations<T>
    fun withGrouping(groupPath: ColumnPath): PivotAggregations<T>

    fun into(column: Column) = value(column)
    fun into(column: String) = value(column)
    fun into(column: KProperty<*>) = value(column.name)

    fun <R : Comparable<R>> minOf(selector: RowSelector<T, R>) = aggregate { minOf(selector) }
    fun <R : Comparable<R>> maxOf(selector: RowSelector<T, R>) = aggregate { maxOf(selector) }

    fun <R : Comparable<R>> min(columns: ColumnsSelector<T, R?>): DataFrame<T> = Aggregators.min.aggregateAll(this, columns)
    fun <R : Comparable<R>> max(columns: ColumnsSelector<T, R?>): DataFrame<T> = Aggregators.max.aggregateAll(this, columns)

    fun <R : Number> std(columns: ColumnsSelector<T, R?>) = Aggregators.std.aggregateAll(this, columns)
}