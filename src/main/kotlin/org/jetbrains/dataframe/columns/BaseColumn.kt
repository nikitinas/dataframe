package org.jetbrains.dataframe.columns

import org.jetbrains.dataframe.AnyRow
import org.jetbrains.dataframe.ColumnKind
import org.jetbrains.dataframe.headPlusIterable
import org.jetbrains.dataframe.impl.asList
import org.jetbrains.dataframe.impl.columns.DataColumnInternal
import kotlin.reflect.KProperty
import kotlin.reflect.KType

/**
 * Column with type, name/path and values
 * Base interface for all three kinds of columns: [ValueColumn], [ColumnGroup] and [FrameColumn]
 */
interface BaseColumn<out T> : ColumnReference<T> {

    fun size(): Int
    fun ndistinct(): Int
    fun kind(): ColumnKind

    operator fun get(index: Int): T
    operator fun get(firstIndex: Int, vararg otherIndices: Int) = slice(
        headPlusIterable(
            firstIndex,
            otherIndices.asIterable()
        )
    )
    operator fun get(row: AnyRow) = get(row.index())

    fun values(): Iterable<T>

    fun toList() = values().asList()

    fun type(): KType

    fun defaultValue(): T?

    fun distinct(): BaseColumn<T>

    fun slice(range: IntRange): BaseColumn<T>

    fun slice(indices: Iterable<Int>): BaseColumn<T>

    fun slice(mask: BooleanArray): BaseColumn<T>

    override fun rename(newName: String): BaseColumn<T>

    operator fun get(columnName: String): AnyCol

    fun toSet(): Set<T>

    operator fun getValue(thisRef: Any?, property: KProperty<*>): BaseColumn<T> = (this as DataColumnInternal<*>).rename(property.name).forceResolve() as BaseColumn<T>
}

typealias AnyColumn = BaseColumn<*>

internal val <T> BaseColumn<T>.values get() = values()
internal val AnyColumn.ndistinct get() = ndistinct()
internal val AnyColumn.size get() = size()
