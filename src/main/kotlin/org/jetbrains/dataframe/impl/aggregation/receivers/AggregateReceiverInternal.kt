package org.jetbrains.dataframe.impl.aggregation.receivers

import org.jetbrains.dataframe.AggregateBody
import org.jetbrains.dataframe.ColumnPath
import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.NamedValue
import org.jetbrains.dataframe.aggregation.AggregateReceiver
import org.jetbrains.dataframe.aggregation.AggregateReceiverWithDefault
import org.jetbrains.dataframe.columns.AnyCol
import kotlin.reflect.KType

@PublishedApi
internal interface AggregateReceiverInternal<out T> {

    val df: DataFrame<T>

    fun yield(value: NamedValue): NamedValue

    fun <R> yield(path: ColumnPath, value: R, type: KType?, default: R?, guessType: Boolean) =
        yield(NamedValue.create(path, value, type, default, guessType))

    fun <R> yield(path: ColumnPath, value: R, type: KType? = null, default: R? = null): NamedValue

    fun pathForSingleColumn(column: AnyCol): ColumnPath
}

@PublishedApi
internal fun <T> AggregateReceiver<T>.internal() = this as AggregateReceiverInternal<T>

internal fun <T, R> AggregateBody<T, R>.internal() = this as AggregateBodyInternal<T, R>

internal typealias AggregateBodyInternal<T, R> = AggregateReceiverInternal<T>.(AggregateReceiverInternal<T>) -> R
