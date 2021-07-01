package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.BaseColumn

public interface Many<out T> : List<T>

internal class ManyImpl<out T>(val list: List<T>) : List<T> by list, Many<T> {

    override fun toString() = list.toString()

    override fun equals(other: Any?) = list.equals(other)

    override fun hashCode() = list.hashCode()
}

internal val EmptyMany: Many<Nothing> = ManyImpl(emptyList())

public fun <T> BaseColumn<T>.toMany(): Many<T> = values().toMany()

public fun <T> emptyMany(): Many<T> = EmptyMany

public fun <T> manyOf(element: T): Many<T> = ManyImpl(listOf(element))

public fun <T> manyOf(vararg values: T): Many<T> = ManyImpl(listOf(*values))

public fun <T> Sequence<T>.toMany(): Many<T> = toList().toMany()

public fun <T> Iterable<T>.toMany(): Many<T> = when (this) {
    is Many<T> -> this
    is List<T> -> ManyImpl(this)
    else -> ManyImpl(toList())
}

internal typealias AnyMany = Many<*> // :)
