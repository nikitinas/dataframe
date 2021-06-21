package org.jetbrains.dataframe.aggregation

import org.jetbrains.dataframe.ColumnPath
import org.jetbrains.dataframe.SelectReceiver
import org.jetbrains.dataframe.columns.Columns
import org.jetbrains.dataframe.impl.aggregation.ConfiguredAggregateColumn
import org.jetbrains.dataframe.impl.pathOf

interface SelectAggregatableColumnsReceiver<out T> : SelectReceiver<T> {

    infix fun <C> Columns<C>.default(defaultValue: C): Columns<C> =
        ConfiguredAggregateColumn.withDefault(this, defaultValue)

    fun path(vararg names: String): ColumnPath = names.asList()

    infix fun <C> Columns<C>.into(name: String): Columns<C> = ConfiguredAggregateColumn.withPath(this, pathOf(name))

    infix fun <C> Columns<C>.into(path: ColumnPath): Columns<C> = ConfiguredAggregateColumn.withPath(this, path)
}

typealias AggregateColumnsSelector<T, C> = SelectAggregatableColumnsReceiver<T>.(SelectAggregatableColumnsReceiver<T>) -> Columns<C>
