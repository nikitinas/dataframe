package org.jetbrains.dataframe.impl.aggregation.receivers

import org.jetbrains.dataframe.ColumnPath
import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.NamedValue
import org.jetbrains.dataframe.aggregation.PivotReceiver
import org.jetbrains.dataframe.columns.AnyCol
import org.jetbrains.dataframe.impl.emptyPath
import kotlin.reflect.KType

internal class PivotReceiverImpl<T>(override val df: DataFrame<T>) : PivotReceiver<T>(), AggregateReceiverInternal<T>, DataFrame<T> by df {

    internal val values = mutableListOf<NamedValue>()

    override fun yield(value: NamedValue) = value.also { values.add(it) }

    override fun pathForSingleColumn(column: AnyCol) = emptyPath()

    override fun <R> yield(path: ColumnPath, value: R, type: KType?, default: R?) = yield(path, value, type, default, true)
}