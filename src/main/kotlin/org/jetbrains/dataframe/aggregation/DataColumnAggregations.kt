package org.jetbrains.dataframe.aggregation

import org.jetbrains.dataframe.Predicate
import org.jetbrains.dataframe.columns.BaseColumn

interface DataColumnAggregations<out T>: BaseColumn<T> {

    fun count(predicate: Predicate<T>? = null) = if(predicate == null) size() else values().count(predicate)
}