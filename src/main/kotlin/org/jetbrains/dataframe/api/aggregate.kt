package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.isSubtypeOf
import org.jetbrains.dataframe.impl.createDataCollector
import kotlin.reflect.KProperty
import kotlin.reflect.KType

class GroupAggregateBuilder<T>(internal val df: DataFrame<T>): DataFrame<T> by df {

    private data class NamedValue(val path: ColumnPath, val value: Any?, val type: KType, val defaultValue: Any?)

    private val values = mutableListOf<NamedValue>()

    internal fun toDataFrame() = values.map { it.path to DataColumn.createGuess(it.path.last(), listOf(it.value), it.type, it.defaultValue) }.toDataFrame<T>() ?: emptyDataFrame(1).typed()

    fun <R> addValue(path: ColumnPath, value: R, type: KType, default: R? = null) {
        values.add(NamedValue(path, value, type, default))
    }

    inline fun <reified R> addValue(columnName: String, value: R, default: R? = null) = addValue(listOf(columnName), value, getType<R>(), default)

    fun <C> spread(selector: ColumnSelector<T, C>) = SpreadClause.inAggregator(this, selector)
    fun <C> spread(column: ColumnReference<C>) = spread { column }
    fun <C> spread(column: KProperty<C>) = spread(column.toColumnDef())
    fun <C> spread(column: String) = spread(column.toColumnDef())

    fun <C> countBy(selector: ColumnSelector<T, C>) = spread(selector).with { nrow() }.useDefault(0)
    fun <C> countBy(column: ColumnReference<C>) = countBy { column }
    fun <C> countBy(column: KProperty<C>) = countBy(column.toColumnDef())
    fun countBy(column: String) = countBy(column.toColumnDef())

    inline infix fun <reified R> R.into(name: String)  = addValue(listOf(name), this, getType<R>())
}typealias GroupAggregator<G> = GroupAggregateBuilder<G>.(GroupAggregateBuilder<G>) -> Unit

fun <T, G> GroupedDataFrame<T, G>.aggregate(body: GroupAggregator<G>) = doAggregate(plain(), { groups }, removeColumns = true, body)

data class AggregateClause<T, G>(val df: DataFrame<T>, val selector: ColumnSelector<T, DataFrame<G>>){
    fun with(body: GroupAggregator<G>) = doAggregate(df, selector, removeColumns = false, body)
}

fun <T, G> DataFrame<T>.aggregate(selector: ColumnSelector<T, DataFrame<G>>) = AggregateClause(this, selector)

internal fun <T, G> doAggregate(df: DataFrame<T>, selector: ColumnSelector<T, DataFrame<G>?>, removeColumns: Boolean, body: GroupAggregator<G>): DataFrame<T> {

    val column = df.column(selector)

    val (df2, removedNodes) = df.doRemove(selector)

    val groupedFrame = column.values.map {
        if(it == null) null
        else {
            val builder = GroupAggregateBuilder(it)
            body(builder, builder)
            builder.toDataFrame()
        }
    }.union()

    val removedNode = removedNodes.single()
    val insertPath = removedNode.pathFromRoot().dropLast(1)

    if(!removeColumns) removedNode.data.wasRemoved = false

    val columnsToInsert = groupedFrame.columns().map {
        ColumnToInsert(insertPath + it.name(), removedNode, it)
    }
    val src = if(removeColumns) df2 else df
    return src.doInsert(columnsToInsert)
}

internal inline fun <T, reified C> DataFrame<T>.aggregateColumns(crossinline selector: (DataColumn<C>) -> Any?) = aggregateColumns(getType<C>()) { selector(it as DataColumn<C>) }

internal fun <T> DataFrame<T>.aggregateColumns(type: KType, selector: (AnyCol) -> Any?) =
    aggregateColumns({ it.isSubtypeOf(type) }, selector)

internal fun <T> DataFrame<T>.aggregateColumns(filter: Predicate<AnyCol>, selector: (AnyCol) -> Any?): DataRow<T> {
    return columns().filter(filter).map {
        val collector = createDataCollector(1)
        collector.add(selector(it))
        collector.toColumn(it.name)
    }.asDataFrame<T>()[0]
}