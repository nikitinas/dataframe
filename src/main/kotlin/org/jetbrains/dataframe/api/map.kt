package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.name
import org.jetbrains.dataframe.columns.size
import org.jetbrains.dataframe.columns.values
import org.jetbrains.dataframe.impl.columns.typed
import org.jetbrains.dataframe.impl.createDataCollector
import kotlin.reflect.KType

public fun <T> DataFrame<T>.mapColumns(body: TypedColumnsFromDataRowBuilder<T>.() -> Unit): AnyFrame {
    val builder = TypedColumnsFromDataRowBuilder(this)
    body(builder)
    return dataFrameOf(builder.columns)
}

public fun <T, R> DataColumn<T>.map(transform: (T) -> R): DataColumn<R> {
    val collector = createDataCollector(size)
    values.forEach { collector.add(transform(it)) }
    return collector.toColumn(name).typed()
}

public fun <T, R> DataColumn<T>.map(type: KType?, transform: (T) -> R): DataColumn<R> {
    if (type == null) return map(transform)
    val collector = createDataCollector<R>(size, type)
    values.forEach { collector.add(transform(it)) }
    return collector.toColumn(name) as DataColumn<R>
}

public fun <T, G, R> GroupedDataFrame<T, G>.mapNotNullGroups(transform: DataFrame<G>.() -> DataFrame<R>?): GroupedDataFrame<T, R> = mapGroups { if (it == null) null else transform(it) }

public fun <T, G, R> GroupedDataFrame<T, G>.map(body: (key: DataRow<T>, group: DataFrame<G>) -> R): List<R> =
    keys.mapIndexedNotNull { index, row ->
        val group = groups[index]
        if (group == null) null
        else body(row, group)
    }
