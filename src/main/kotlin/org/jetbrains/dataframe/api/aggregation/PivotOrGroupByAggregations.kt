package org.jetbrains.dataframe

import org.jetbrains.dataframe.aggregation.Aggregatable
import org.jetbrains.dataframe.aggregation.AggregateColumnsSelector
import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.impl.aggregation.aggregators.Aggregators
import org.jetbrains.dataframe.impl.aggregation.getAggregateColumn
import org.jetbrains.dataframe.impl.aggregation.modes.aggregateFor
import org.jetbrains.dataframe.impl.aggregation.numberColumns
import org.jetbrains.dataframe.impl.aggregation.yieldOneOrManyBy
import org.jetbrains.dataframe.impl.columns.toColumns

interface PivotOrGroupByAggregations<out T> : Aggregatable<T> {

    fun <R : Number> sumBy(columns: ColumnsSelector<T, R>): DataFrame<T> = Aggregators.sum.aggregateFor(this, columns)
    fun sum(): DataFrame<T> = sumBy(numberColumns())

    fun <R : Number> stdBy(columns: AggregateColumnsSelector<T, R>): DataFrame<T> = Aggregators.std.aggregateFor(this, columns)
    fun std(): DataFrame<T> = stdBy(numberColumns())
}