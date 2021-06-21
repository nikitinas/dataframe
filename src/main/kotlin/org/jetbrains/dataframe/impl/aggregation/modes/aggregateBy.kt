package org.jetbrains.dataframe.impl.aggregation.modes

import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.DataFrameSelector
import org.jetbrains.dataframe.DataRow
import org.jetbrains.dataframe.GroupedDataFrame
import org.jetbrains.dataframe.GroupByAggregations
import org.jetbrains.dataframe.impl.aggregation.aggregateInternal
import org.jetbrains.dataframe.namedValues
import org.jetbrains.dataframe.typed

internal fun <T> GroupByAggregations<T>.aggregateBy(
    body: DataFrameSelector<T, DataRow<T>?>
): DataFrame<T> {
    require(this is GroupedDataFrame<*, T>)
    val keyColumns = keys.columnNames().toSet()
    return aggregateInternal {
        val row = body(df, df)
        row?.namedValues()?.forEach {
            if(!keyColumns.contains(it.name)) yield(it)
        }
    }.typed()
}