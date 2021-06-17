package org.jetbrains.dataframe.aggregation

import org.jetbrains.dataframe.ColumnPath
import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.NamedValue
import org.jetbrains.dataframe.columns.AnyCol
import org.jetbrains.dataframe.impl.aggregation.ValueWithDefault
import kotlin.reflect.KType

interface AggregateReceiver<out T> : DataFrame<T> {

    fun yield(value: NamedValue): NamedValue

    fun <R> yield(path: ColumnPath, value: R, type: KType?, default: R?, guessType: Boolean) =
        yield(NamedValue.create(path, value, type, default, guessType))

    fun <R> yield(path: ColumnPath, value: R, type: KType? = null, default: R? = null): NamedValue

    fun pathForSingleColumn(column: AnyCol): ColumnPath

    infix fun <R> R.default(defaultValue: R): Any = when (this) {
        is NamedValue -> this.also { it.default = defaultValue }
        else -> ValueWithDefault(this, defaultValue)
    }
}