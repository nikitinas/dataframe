package org.jetbrains.dataframe.columns

import org.jetbrains.dataframe.AnyRow
import org.jetbrains.dataframe.ColumnKind
import org.jetbrains.dataframe.headPlusIterable
import org.jetbrains.dataframe.impl.asList
import kotlin.reflect.KProperty

/**
 * Column with type, name/path and values
 * Base interface for all three kinds of columns: [ValueColumn], [ColumnGroup] and [FrameColumn]
 */
interface Column<out T> : ColumnReference<T> {

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

    fun defaultValue(): T?

    fun distinct(): Column<T>

    fun slice(range: IntRange): Column<T>

    fun slice(indices: Iterable<Int>): Column<T>

    fun slice(mask: BooleanArray): Column<T>

    override fun rename(newName: String): Column<T>

    operator fun get(columnName: String): AnyCol

    fun toSet(): Set<T>

    operator fun getValue(thisRef: Any?, property: KProperty<*>) = rename(property.name)
}

typealias AnyColumn = Column<*>

internal val <T> Column<T>.values get() = values()
internal val AnyColumn.ndistinct get() = ndistinct()
internal val AnyColumn.size get() = size()
