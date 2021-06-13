package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.AnyCol
import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.guessColumnType
import org.jetbrains.dataframe.columns.name
import org.jetbrains.dataframe.columns.values
import org.jetbrains.dataframe.impl.createDataCollector
import kotlin.reflect.KProperty
import kotlin.reflect.KType

internal class ValueWithDefault<T>(val value: T, val default: T)

internal data class NamedValue(val path: ColumnPath, val value: Any?, val type: KType?, val defaultValue: Any?, val guessType: Boolean = false)

class GroupAggregateBuilder<T>(internal val df: DataFrame<T>): DataFrame<T> by df {

    private val values = mutableListOf<NamedValue>()

    internal fun child() = GroupAggregateBuilder(df).also { values.add(NamedValue(emptyList(), it, null, null)) }

    internal fun NamedValue.toColumnWithPath() = path to guessColumnType(
        path.last(),
        listOf(value),
        type,
        guessType,
        defaultValue
    )

    internal fun compute(): AnyFrame {

        val allValues = mutableListOf<NamedValue>()
        values.forEach {
            if(it.value is GroupAggregateBuilder<*>){
                it.value.values.forEach {
                    allValues.add(it)
                }
            } else
                allValues.add(it)
        }
        val columns = allValues.map { it.toColumnWithPath() }
        return if (columns.isEmpty()) emptyDataFrame(1)
        else columns.toDataFrame<T>()
    }

    fun <R> addValue(path: ColumnPath, value: R, type: KType? = null, default: R? = null, guessType: Boolean = false) {
        when(value){
            is ValueWithDefault<*> -> values.add(NamedValue(path, value.value, type, value.default, guessType))
            is AggregatedPivot<*> -> {
                value.aggregator.values.forEach {
                    addValue(path + it.path, it.value, it.type, it.defaultValue, it.guessType)
                }
                value.aggregator.values.clear()
            }
            else -> values.add(NamedValue(path, value, type, default, guessType))
        }
    }

    inline fun <reified R> addValue(columnName: String, value: R, default: R? = null) = addValue(listOf(columnName), value, getType<R>(), default)

    inline infix fun <reified R> R.into(name: String)  = addValue(listOf(name), this, getType<R>())
}

typealias GroupAggregator<G> = GroupAggregateBuilder<G>.(GroupAggregateBuilder<G>) -> Unit

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
            val row = builder.compute()
            row
        }
    }.union()

    val removedNode = removedNodes.single()
    val insertPath = removedNode.pathFromRoot().dropLast(1)

    if(!removeColumns) removedNode.data.wasRemoved = false

    val columnsToInsert = groupedFrame.columns().map {
        ColumnToInsert(insertPath + it.name, it, removedNode)
    }
    val src = if(removeColumns) df2 else df
    return src.insert(columnsToInsert)
}

internal inline fun <T, reified C> DataFrame<T>.aggregateColumns(crossinline selector: (DataColumn<C>) -> Any?): DataRow<T> = aggregateColumns(getType<C>()) { selector(it as DataColumn<C>) }

internal fun <T> DataFrame<T>.aggregateColumns(type: KType, selector: (AnyCol) -> Any?): DataRow<T> =
    aggregateColumns({ colsOf(type) }, selector)

internal fun <T, C> DataFrame<T>.aggregateColumns(colSelector: ColumnsSelector<T, C>, valueSelector: (DataColumn<C>) -> Any?): DataRow<T> {
    return this[colSelector].map {
        val collector = createDataCollector(1)
        collector.add(valueSelector(it))
        collector.toColumn(it.name)
    }.asDataFrame<T>()[0]
}