package org.jetbrains.dataframe.impl.aggregation.receivers

import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.NamedValue
import org.jetbrains.dataframe.PivotReceiver

internal class PivotReceiverImpl<T>(internal val df: DataFrame<T>) : PivotReceiver<T>(), DataFrame<T> by df {

    internal val values = mutableListOf<NamedValue>()

    override fun yield(value: NamedValue) = value.also { values.add(it) }
}