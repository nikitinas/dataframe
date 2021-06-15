package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.BaseColumn
import org.jetbrains.dataframe.impl.asList

interface Many<out T> : List<T>

internal class ManyImpl<out T>(val list: List<T>) : List<T> by list, Many<T> {

    override fun toString() = list.toString()

    override fun equals(other: Any?) = list.equals(other)

    override fun hashCode() = list.hashCode()
}

internal val EmptyMany: Many<Nothing> = ManyImpl(emptyList())

fun <T> BaseColumn<T>.toMany(): Many<T> = values().toMany()

fun <T> emptyMany(): Many<T> = EmptyMany

fun <T> manyOf(element: T): Many<T> = ManyImpl(listOf(element))

fun <T> manyOf(vararg values: T): Many<T> = ManyImpl(listOf(*values))

fun <T> Iterable<T>.toMany(): Many<T> = when(this){
    is Many<T> -> this
    is List<T> -> ManyImpl(this)
    else -> ManyImpl(toList())
}

internal typealias AnyMany = Many<*> // :)