package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.FrameColumn
import org.jetbrains.dataframe.impl.GroupedDataFrameImpl
import org.jetbrains.dataframe.impl.columns.asTable
import org.jetbrains.dataframe.impl.columns.isTable

typealias GroupKey = List<Any?>

interface GroupedDataFrame<out T, out G>: GroupByAggregations<G> {

    val groups: FrameColumn<G>

    val keys: DataFrame<T>

    fun plain(): DataFrame<T>

    fun ungroup() = groups.union().typed<G>()

    operator fun get(vararg values: Any?) = get(values.toList())
    operator fun get(key: GroupKey): DataFrame<T>

    fun <R> mapGroups(transform: Selector<DataFrame<G>?, DataFrame<R>?>): GroupedDataFrame<T, R>

    data class Entry<T, G>(val key: DataRow<T>, val group: DataFrame<G>?)
}

internal fun <T, G> DataFrame<T>.toGrouped(groupedColumnName: String): GroupedDataFrame<T, G> =
    GroupedDataFrameImpl(this, this[groupedColumnName] as FrameColumn<G>) { none() }

internal fun <T> DataFrame<T>.toGrouped(): GroupedDataFrame<T, T> {

    val groupCol = columns().single { it.isTable() }.asTable() as FrameColumn<T>
    return toGrouped { groupCol }
}



