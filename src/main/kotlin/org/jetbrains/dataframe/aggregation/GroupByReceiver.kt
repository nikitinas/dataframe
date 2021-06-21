package org.jetbrains.dataframe.aggregation

import org.jetbrains.dataframe.ColumnPath
import org.jetbrains.dataframe.columns.AnyCol
import org.jetbrains.dataframe.columns.shortPath
import org.jetbrains.dataframe.getType
import kotlin.reflect.KType

abstract class GroupByReceiver<out T> : AggregateReceiver<T> {

    override fun pathForSingleColumn(column: AnyCol) = column.shortPath()

    override fun <R> yield(path: ColumnPath, value: R, type: KType?, default: R?) =
        yield(path, value, type, default, false)

    inline infix fun <reified R> R.into(name: String) = yield(listOf(name), this, getType<R>())
}

typealias GroupAggregator<G> = GroupByReceiver<G>.(GroupByReceiver<G>) -> Unit