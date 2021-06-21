package org.jetbrains.dataframe

import org.jetbrains.dataframe.aggregation.GroupByAggregateBody
import org.jetbrains.dataframe.columns.name
import org.jetbrains.dataframe.columns.values
import org.jetbrains.dataframe.impl.aggregation.GroupByReceiverImpl

data class AggregateClause<T, G>(val df: DataFrame<T>, val selector: ColumnSelector<T, DataFrame<G>>) {
    fun with(body: GroupByAggregateBody<G>) = aggregateGroupBy(df, selector, removeColumns = false, body)
}

fun <T, G> DataFrame<T>.aggregate(selector: ColumnSelector<T, DataFrame<G>>) = AggregateClause(this, selector)

internal fun <T, G> aggregateGroupBy(
    df: DataFrame<T>,
    selector: ColumnSelector<T, DataFrame<G>?>,
    removeColumns: Boolean,
    body: GroupByAggregateBody<G>
): DataFrame<T> {
    val column = df.column(selector)

    val (df2, removedNodes) = df.doRemove(selector)

    val groupedFrame = column.values.map {
        if (it == null) null
        else {
            val builder = GroupByReceiverImpl(it)
            body(builder, builder)
            builder.compute()
        }
    }.union()

    val removedNode = removedNodes.single()
    val insertPath = removedNode.pathFromRoot().dropLast(1)

    if (!removeColumns) removedNode.data.wasRemoved = false

    val columnsToInsert = groupedFrame.columns().map {
        ColumnToInsert(insertPath + it.name, it, removedNode)
    }
    val src = if (removeColumns) df2 else df
    return src.insert(columnsToInsert)
}

