package org.jetbrains.dataframe.impl.aggregation.modes

import org.jetbrains.dataframe.*
import org.jetbrains.dataframe.impl.aggregation.aggregateInternal

internal fun <T> GroupByAggregations<T>.aggregateBy(
    body: DataFrameSelector<T, DataRow<T>?>
): DataFrame<T> {
    require(this is GroupedDataFrame<*, T>)
    val keyColumns = keys.columnNames().toSet()
    return aggregateInternal {
        val row = body(df, df)
        row?.namedValues()?.forEach {
            if (!keyColumns.contains(it.name)) yield(it)
        }
    }.typed()
}
