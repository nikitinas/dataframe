package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.columns.FrameColumn
import org.jetbrains.dataframe.impl.GroupedDataFrameImpl
import org.jetbrains.dataframe.impl.columns.asTable
import org.jetbrains.dataframe.impl.columns.isTable
import org.jetbrains.dataframe.impl.columns.typed

typealias GroupKey = List<Any?>

interface GroupedDataRow<out T, out G>: DataRow<T> {

    fun group(): DataFrame<G>

    fun groupOrNull(): DataFrame<G>?
}

val <T, G> GroupedDataRow<T, G>.group get() = group()

typealias GroupedRowSelector<T, G, R> = GroupedDataRow<T, G>.(GroupedDataRow<T, G>) -> R

typealias GroupedRowFilter<T, G> = GroupedRowSelector<T, G, Boolean>

interface GroupedDataFrame<out T, out G>: GroupByAggregations<G> {

    val groups: FrameColumn<G>

    val keys: DataFrame<T>

    fun plain(): DataFrame<T>

    fun ungroup() = groups.union().typed<G>()

    operator fun get(vararg values: Any?) = get(values.toList())
    operator fun get(key: GroupKey): DataFrame<T>

    fun <R> mapGroups(transform: Selector<DataFrame<G>?, DataFrame<R>?>): GroupedDataFrame<T, R>

    fun filter(predicate: GroupedRowFilter<T, G>): GroupedDataFrame<T, G>

    data class Entry<T, G>(val key: DataRow<T>, val group: DataFrame<G>?)

    companion object
}

internal fun <T, G> DataFrame<T>.toGrouped(groupedColumnName: String): GroupedDataFrame<T, G> =
    GroupedDataFrameImpl(this, this[groupedColumnName] as FrameColumn<G>) { none() }

internal fun <T, G> DataFrame<T>.toGrouped(groupedColumn: ColumnReference<DataFrame<G>?>): GroupedDataFrame<T, G> =
    GroupedDataFrameImpl(this, frameColumn(groupedColumn.name()).typed()) { none() }

internal fun <T> DataFrame<T>.toGrouped(): GroupedDataFrame<T, T> {

    val groupCol = columns().single { it.isTable() }.asTable() as FrameColumn<T>
    return toGrouped { groupCol }
}



