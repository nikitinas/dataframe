package org.jetbrains.dataframe

import org.jetbrains.dataframe.aggregation.DataFrameAggregations
import org.jetbrains.dataframe.aggregation.GroupByAggregations
import org.jetbrains.dataframe.aggregation.PivotAggregations
import org.jetbrains.dataframe.aggregation.receivers.GroupAggregator
import org.jetbrains.dataframe.columns.AnyCol
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.name
import org.jetbrains.dataframe.columns.values
import org.jetbrains.dataframe.impl.aggregation.GroupByReceiverImpl
import org.jetbrains.dataframe.impl.aggregation.aggregators.Aggregators
import org.jetbrains.dataframe.impl.aggregation.modes.of
import org.jetbrains.dataframe.impl.aggregation.yieldOneOrMany
import org.jetbrains.dataframe.impl.createDataCollector
import kotlin.reflect.KType

// TODO: remove cast<Double>
inline fun <T, reified C : Number> DataFrameAggregations<T>.meanOf(crossinline expression: RowSelector<T, C>): Double? =
    Aggregators.mean(false).cast<Double>().of(this, expression)

inline fun <T, reified R : Number> PivotAggregations<T>.meanOf(
    skipNa: Boolean = true,
    crossinline expression: RowSelector<T, R>
): DataFrame<T> = Aggregators.mean(skipNa).cast<Double>().of(this, expression)

inline fun <T, reified R : Number> GroupByAggregations<T>.meanOf(
    resultName: String = "mean",
    skipNa: Boolean = true,
    crossinline expression: RowSelector<T, R>
): DataFrame<T> = Aggregators.mean(skipNa).cast<Double>().of(resultName, this, expression)


inline fun <T, reified C> PivotAggregations<T>.valueOf(crossinline expression: RowSelector<T, C>): DataFrame<T> {
    val type = getType<C>()
    return aggregate {
        yieldOneOrMany(map(expression), type)
    }
}

inline fun <T, reified C> GroupByAggregations<T>.valueOf(
    name: String,
    crossinline expression: RowSelector<T, C>
): DataFrame<T> {
    val type = getType<C>()
    val path = listOf(name)
    return aggregateBase {
        yieldOneOrMany(path, map(expression), type)
    }
}

data class AggregateClause<T, G>(val df: DataFrame<T>, val selector: ColumnSelector<T, DataFrame<G>>) {
    fun with(body: GroupAggregator<G>) = aggregateGroupBy(df, selector, removeColumns = false, body)
}

fun <T, G> DataFrame<T>.aggregate(selector: ColumnSelector<T, DataFrame<G>>) = AggregateClause(this, selector)

internal fun <T, G> aggregateGroupBy(
    df: DataFrame<T>,
    selector: ColumnSelector<T, DataFrame<G>?>,
    removeColumns: Boolean,
    body: GroupAggregator<G>
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

internal inline fun <T, reified C> DataFrame<T>.aggregateColumns(crossinline selector: (DataColumn<C>) -> Any?): DataRow<T> =
    aggregateColumns(getType<C>()) { selector(it as DataColumn<C>) }

internal fun <T> DataFrame<T>.aggregateColumns(type: KType, selector: (AnyCol) -> Any?): DataRow<T> =
    aggregateColumns({ colsOf(type) }, selector)

internal fun <T, C> DataFrame<T>.aggregateColumns(
    colSelector: ColumnsSelector<T, C>,
    valueSelector: (DataColumn<C>) -> Any?
): DataRow<T> {
    return this[colSelector].map {
        val collector = createDataCollector(1)
        collector.add(valueSelector(it))
        collector.toColumn(it.name)
    }.asDataFrame<T>()[0]
}

fun <T, G> GroupedDataFrame<T, G>.aggregate(body: GroupAggregator<G>) =
    aggregateGroupBy(plain(), { groups }, removeColumns = true, body)
