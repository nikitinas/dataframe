package org.jetbrains.dataframe.aggregation

import org.jetbrains.dataframe.Column
import org.jetbrains.dataframe.ColumnsSelector
import org.jetbrains.dataframe.impl.aggregation.GroupAggregatorPivotImpl
import org.jetbrains.dataframe.GroupedPivotAggregations
import org.jetbrains.dataframe.impl.columns.toColumns

abstract class GroupByReceiver<out T> : AggregateReceiver<T>() {

    fun pivot(columns: ColumnsSelector<T, *>): GroupedPivotAggregations<T> = GroupAggregatorPivotImpl(this, columns)
    fun pivot(vararg columns: Column) = pivot { columns.toColumns() }
    fun pivot(vararg columns: String) = pivot { columns.toColumns() }
}

typealias GroupByAggregateBody<G> = GroupByReceiver<G>.(GroupByReceiver<G>) -> Unit