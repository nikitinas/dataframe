package org.jetbrains.dataframe.impl

import org.jetbrains.dataframe.*
import org.jetbrains.dataframe.columns.values
import org.jetbrains.dataframe.columns.FrameColumn

internal class GroupedDataFrameImpl<T, G>(val df: DataFrame<T>, override val groups: FrameColumn<G>): GroupedDataFrame<T, G> {

    override val keys by lazy { df - groups }

    override operator fun get(key: GroupKey): DataFrame<T> {

        require(key.size < df.ncol()) { "Invalid size of the key" }

        val keySize = key.size
        val filtered = df.filter { it.values.subList(0, keySize) == key }
        return filtered.frameColumn(groups.name()).values.union().typed<T>()
    }

    override fun <R> mapGroups(transform: Selector<DataFrame<G>?, DataFrame<R>?>) =
            df.update(groups) { transform(it, it) }.toGrouped { frameColumn<R>(groups.name()) }

    override fun plain() = df

    override fun toString() = df.toString()
}