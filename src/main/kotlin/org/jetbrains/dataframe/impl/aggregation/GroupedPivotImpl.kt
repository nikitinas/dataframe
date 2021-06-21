package org.jetbrains.dataframe.impl.aggregation

import org.jetbrains.dataframe.ColumnPath
import org.jetbrains.dataframe.ColumnsSelector
import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.GroupedDataFrame
import org.jetbrains.dataframe.GroupedPivotAggregations
import org.jetbrains.dataframe.aggregatePivot
import org.jetbrains.dataframe.aggregation.PivotAggregateBody
import org.jetbrains.dataframe.impl.aggregation.receivers.AggregateBodyInternal
import org.jetbrains.dataframe.impl.columns.toColumns
import org.jetbrains.dataframe.typed

internal data class GroupedPivotImpl<T>(
    internal val df: GroupedDataFrame<*, T>,
    internal val columns: ColumnsSelector<T, *>,
    internal val groupValues: Boolean = false,
    internal val default: Any? = null,
    internal val groupPath: ColumnPath = emptyList()
) : GroupedPivotAggregations<T>, AggregatableInternal<T> {
    override fun <R> aggregate(body: PivotAggregateBody<T, R>): DataFrame<T> {
        return df.aggregate {
            aggregatePivot(this as GroupByReceiverImpl<T>, columns, groupValues, groupPath, default, body)
        }.typed()
    }

    override fun groupByValue(flag: Boolean) = if(flag == groupValues) this else copy(groupValues = flag)

    override fun withGrouping(groupPath: ColumnPath) = copy(groupPath = groupPath)

    override fun default(value: Any?) = copy(default = value)

    override fun remainingColumnsSelector(): ColumnsSelector<*, *> = { all().except(columns.toColumns() and df.keys.columnNames().toColumns()) }

    override fun <R> aggregateInternal(body: AggregateBodyInternal<T, R>) = aggregate(body as PivotAggregateBody<T, R>)
}