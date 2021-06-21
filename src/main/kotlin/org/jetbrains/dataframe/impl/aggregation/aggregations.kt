package org.jetbrains.dataframe.impl.aggregation

import org.jetbrains.dataframe.aggregation.AggregateColumnsSelector
import org.jetbrains.dataframe.aggregation.AggregateReceiver
import org.jetbrains.dataframe.aggregation.AggregateSelectReceiver
import org.jetbrains.dataframe.ColumnPath
import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.GroupByAggregations
import org.jetbrains.dataframe.GroupedPivotAggregations
import org.jetbrains.dataframe.SelectReceiverImpl
import org.jetbrains.dataframe.columns.Columns
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.impl.getListType
import org.jetbrains.dataframe.toColumns
import org.jetbrains.dataframe.toMany
import org.jetbrains.dataframe.typed
import kotlin.reflect.KType

@PublishedApi
internal fun <T, V> AggregateReceiver<T>.yieldOneOrMany(
    path: ColumnPath,
    values: List<V>,
    type: KType,
    default: V? = null
) {
    if (values.size == 1) yield(path, values[0], type, default)
    else yield(path, values.toMany(), getListType(type), default)
}

@JvmName("toColumnSetForAggregate")
internal fun <T, C> AggregateColumnsSelector<T, C>.toColumns(): Columns<C> = toColumns {

    class AggregateSelectReceiverImpl<T>(df: DataFrame<T>) : SelectReceiverImpl<T>(df, true), AggregateSelectReceiver<T>

    AggregateSelectReceiverImpl(it.df.typed())
}

internal fun <T, C, R> AggregateReceiver<T>.columnValues(columns: AggregateColumnsSelector<T, C>,
                                                         aggregator: (DataColumn<C>) -> List<R>){
    val cols = getAggregateColumns(columns)
    val isSingle = cols.size == 1
    cols.forEach { col ->
        val path = getPath(col, isSingle)
        yieldOneOrMany(path, aggregator(col.data), col.type, col.default)
    }
}