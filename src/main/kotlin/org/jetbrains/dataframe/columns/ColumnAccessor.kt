package org.jetbrains.dataframe.columns

import org.jetbrains.dataframe.impl.columns.ColumnAccessorImpl
import kotlin.reflect.KProperty

interface ColumnAccessor<out T> : ColumnReference<T> {

    operator fun getValue(thisRef: Any?, property: KProperty<*>) = this

    operator fun <C> get(column: ColumnReference<C>): ColumnAccessor<C>

    override fun rename(newName: String): ColumnAccessor<T>

    fun <C> changeType() = this as ColumnAccessor<C>
}

fun <T> ColumnReference<T>.toAccessor(): ColumnAccessor<T> = when(this){
    is ColumnAccessor<T> -> this
    else -> ColumnAccessorImpl(path())
}

