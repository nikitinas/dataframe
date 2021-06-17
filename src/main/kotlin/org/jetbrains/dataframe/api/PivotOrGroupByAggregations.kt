package org.jetbrains.dataframe.api

import org.jetbrains.dataframe.Column
import org.jetbrains.dataframe.ColumnsSelector
import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.aggregation.Aggregatable
import org.jetbrains.dataframe.aggregation.AggregateColumnsSelector
import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.impl.aggregation.aggregators.Aggregators
import org.jetbrains.dataframe.impl.aggregation.getAggregateColumn
import org.jetbrains.dataframe.impl.aggregation.modes.aggregateFor
import org.jetbrains.dataframe.impl.aggregation.numberColumns
import org.jetbrains.dataframe.impl.aggregation.yieldOneOrManyBy
import org.jetbrains.dataframe.impl.columns.toColumns
import org.jetbrains.dataframe.withColumn

interface PivotOrGroupByAggregations<out T> : Aggregatable<T> {

    fun <V> value(column: ColumnReference<V>): DataFrame<T> = withColumn { getAggregateColumn { column } }
    fun value(column: String): DataFrame<T> = withColumn { getAggregateColumn { it[column] } }
    fun <V> value(column: AggregateColumnsSelector<T, V>): DataFrame<T> = withColumn { getAggregateColumn(column) }

    fun values(vararg columns: Column): DataFrame<T> = values { columns.toColumns() }
    fun values(vararg columns: String): DataFrame<T> = values { columns.toColumns() }
    fun values(columns: AggregateColumnsSelector<T, *>): DataFrame<T> = yieldOneOrManyBy(columns) { it.toList() }

    fun values(): DataFrame<T> = values(remainingColumnsSelector())

    fun <R : Number> sumBy(columns: ColumnsSelector<T, R>): DataFrame<T> = Aggregators.sum.aggregateFor(this, columns)
    fun sum(): DataFrame<T> = sumBy(numberColumns())

    fun <R : Number> stdBy(columns: AggregateColumnsSelector<T, R>): DataFrame<T> = Aggregators.std.aggregateFor(this, columns)
    fun std(): DataFrame<T> = stdBy(numberColumns())
}