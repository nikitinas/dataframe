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
public interface BaseColumn<out T> : ColumnReference<T> {

    public fun size(): Int
    public fun ndistinct(): Int
    public fun kind(): ColumnKind

    public operator fun get(index: Int): T
    public operator fun get(firstIndex: Int, vararg otherIndices: Int): BaseColumn<T> = slice(
        headPlusIterable(
            firstIndex,
            otherIndices.asIterable()
        )
    )
    public operator fun get(row: AnyRow): T = get(row.index())

    public fun values(): Iterable<T>

    public fun toList(): List<T> = values().asList()

    public fun type(): KType

    public fun defaultValue(): T?

    public fun distinct(): BaseColumn<T>

    public fun slice(range: IntRange): BaseColumn<T>

    public fun slice(indices: Iterable<Int>): BaseColumn<T>

    public fun slice(mask: BooleanArray): BaseColumn<T>

    override fun rename(newName: String): BaseColumn<T>

    public operator fun get(columnName: String): AnyCol

    public fun toSet(): Set<T>

    public operator fun getValue(thisRef: Any?, property: KProperty<*>): BaseColumn<T> = (this as DataColumnInternal<*>).rename(property.name).forceResolve() as BaseColumn<T>
}

public typealias AnyColumn = BaseColumn<*>

internal val <T> BaseColumn<T>.values get() = values()
internal val AnyColumn.ndistinct get() = ndistinct()
internal val AnyColumn.size get() = size()
