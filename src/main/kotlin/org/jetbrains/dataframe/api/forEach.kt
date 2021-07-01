package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.values

public fun <T, G> GroupedDataFrame<T, G>.forEach(body: (GroupedDataFrame.Entry<T, G>) -> Unit): Unit = forEach { key, group -> body(GroupedDataFrame.Entry(key, group)) }
public fun <T, G> GroupedDataFrame<T, G>.forEach(body: (key: DataRow<T>, group: DataFrame<G>?) -> Unit): Unit =
    keys.forEachIndexed { index, row ->
        val group = groups[index]
        body(row, group)
    }

public fun <T> DataFrame<T>.forEach(action: RowSelector<T, Unit>): Unit = rows().forEach { action(it, it) }
public fun <T> DataColumn<T>.forEach(action: (T) -> Unit): Unit = values.forEach(action)

public fun <T> DataFrame<T>.forEachIndexed(action: (Int, DataRow<T>) -> Unit): Unit = rows().forEachIndexed(action)
public fun <T> DataColumn<T>.forEachIndexed(action: (Int, T) -> Unit): Unit = values.forEachIndexed(action)
