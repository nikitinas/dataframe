package org.jetbrains.dataframe.impl.aggregation

import org.jetbrains.dataframe.AggregatedPivot
import org.jetbrains.dataframe.AnyFrame
import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.aggregation.receivers.GroupByReceiver
import org.jetbrains.dataframe.NamedValue
import org.jetbrains.dataframe.emptyDataFrame
import org.jetbrains.dataframe.toDataFrame

internal class GroupByReceiverImpl<T>(internal val df: DataFrame<T>) : GroupByReceiver<T>(), DataFrame<T> by df {

    private val values = mutableListOf<NamedValue>()

    internal fun child(): GroupByReceiverImpl<T> {
        val child = GroupByReceiverImpl(df)
        values.add(NamedValue.aggregator(child))
        return child
    }

    internal fun compute(): AnyFrame {

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
        return if (columns.isEmpty()) emptyDataFrame(1)
        else columns.toDataFrame<T>()
    }

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