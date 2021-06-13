package org.jetbrains.dataframe

interface Many<out T> : List<T>

internal class ManyImpl<out T>(val list: List<T>) : List<T> by list, Many<T> {

    override fun toString() = list.toString()

    override fun equals(other: Any?) = list.equals(other)

    override fun hashCode() = list.hashCode()
}

internal val EmptyMany: Many<Nothing> = ManyImpl(emptyList())

fun <T> List<T>.wrapValues(): Many<T> = if(this is Many) this else ManyImpl(this)

fun <T> emptyMany(): Many<T> = EmptyMany

fun <T> manyOf(element: T): Many<T> = ManyImpl(listOf(element))

fun <T> manyOf(vararg values: T): Many<T> = ManyImpl(listOf(*values))