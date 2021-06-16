package org.jetbrains.dataframe.impl.aggregation.aggregators

import org.jetbrains.dataframe.columns.DataColumn
import kotlin.reflect.KClass

@PublishedApi
internal interface Aggregator<C, R> {

    val name: String

    fun aggregate(column: DataColumn<C>): R?

    fun aggregate(columns: Iterable<DataColumn<C>>): R?

    fun aggregate(values: Iterable<C>, clazz: KClass<*>): R?

    fun <T> cast() = this as Aggregator<T?, T>
}