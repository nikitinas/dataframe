package org.jetbrains.dataframe.impl.aggregation

import org.jetbrains.dataframe.AggregatedPivot
import org.jetbrains.dataframe.AnyRow
import org.jetbrains.dataframe.ColumnPath
import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.aggregation.GroupByReceiver
import org.jetbrains.dataframe.NamedValue
import org.jetbrains.dataframe.columns.AnyCol
import org.jetbrains.dataframe.columns.shortPath
import org.jetbrains.dataframe.impl.aggregation.receivers.AggregateReceiverInternal
import org.jetbrains.dataframe.toDataFrame
import kotlin.reflect.KType

internal class GroupByReceiverImpl<T>(internal val df: DataFrame<T>) : GroupByReceiver<T>(),
    AggregateReceiverInternal<T>, DataFrame<T> by df {

    private val values = mutableListOf<NamedValue>()

    internal fun child(): GroupByReceiverImpl<T> {
        val child = GroupByReceiverImpl(df)
        values.add(NamedValue.aggregator(child))
        return child
    }

    internal fun compute(): AnyRow? {

        val allValues = mutableListOf<NamedValue>()
        values.forEach {
            if (it.value is GroupByReceiverImpl<*>) {
                it.value.values.forEach {
                    allValues.add(it)
                }
            } else
                allValues.add(it)
        }
        val columns = allValues.map { it.toColumnWithPath() }
        return if (columns.isEmpty()) null
        else columns.toDataFrame<T>()[0]
    }

    override fun pathForSingleColumn(column: AnyCol) = column.shortPath()

    override fun <R> yield(path: ColumnPath, value: R, type: KType?, default: R?) =
        yield(path, value, type, default, false)

    override fun yield(value: NamedValue): NamedValue {
        when (value.value) {
            is AggregatedPivot<*> -> {
                value.value.aggregator.values.forEach {
                    yield(value.path + it.path, it.value, it.type, it.default, it.guessType)
                }
                value.value.aggregator.values.clear()
            }
            else -> values.add(value)
        }
        return value
    }
}