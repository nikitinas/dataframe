package org.jetbrains.dataframe.aggregation

import org.jetbrains.dataframe.Column
import org.jetbrains.dataframe.ColumnsSelector
import org.jetbrains.dataframe.GroupedPivotAggregations
import org.jetbrains.dataframe.impl.aggregation.GroupAggregatorPivotImpl
import org.jetbrains.dataframe.impl.columns.toColumns

public abstract class GroupByReceiver<out T> : AggregateReceiver<T>() {

    public fun pivot(columns: ColumnsSelector<T, *>): GroupedPivotAggregations<T> = GroupAggregatorPivotImpl(this, columns)
    public fun pivot(vararg columns: Column): GroupedPivotAggregations<T> = pivot { columns.toColumns() }
    public fun pivot(vararg columns: String): GroupedPivotAggregations<T> = pivot { columns.toColumns() }
}

public typealias GroupByAggregateBody<G> = GroupByReceiver<G>.(GroupByReceiver<G>) -> Unit
