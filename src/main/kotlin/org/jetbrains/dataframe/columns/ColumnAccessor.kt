package org.jetbrains.dataframe.columns

import org.jetbrains.dataframe.impl.columns.ColumnAccessorImpl
import kotlin.reflect.KProperty

public interface ColumnAccessor<out T> : ColumnReference<T> {

    public operator fun getValue(thisRef: Any?, property: KProperty<*>): ColumnAccessor<T> = this

    public operator fun <C> get(column: ColumnReference<C>): ColumnAccessor<C>

    override fun rename(newName: String): ColumnAccessor<T>

    public fun <C> changeType(): ColumnAccessor<C> = this as ColumnAccessor<C>
}

public fun <T> ColumnReference<T>.toAccessor(): ColumnAccessor<T> = when (this) {
    is ColumnAccessor<T> -> this
    else -> ColumnAccessorImpl(path())
}
