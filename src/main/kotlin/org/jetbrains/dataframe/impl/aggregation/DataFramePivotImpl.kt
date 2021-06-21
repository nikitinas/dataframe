package org.jetbrains.dataframe.impl.aggregation

import org.jetbrains.dataframe.ColumnsSelector
import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.GroupedPivotAggregations
import org.jetbrains.dataframe.api.aggregation.PivotAggregations
import org.jetbrains.dataframe.groupBy

internal data class DataFramePivotImpl<T>(
    internal val df: DataFrame<T>,
    internal val columns: ColumnsSelector<T, *>
) : PivotAggregations<T> {

    override fun groupBy(columns: ColumnsSelector<T, *>): GroupedPivotAggregations<T> =
        GroupedPivotImpl(df.groupBy(columns), this.columns)
}