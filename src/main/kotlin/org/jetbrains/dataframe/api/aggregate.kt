package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.AnyCol
import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.columns.ColumnWithPath
import org.jetbrains.dataframe.columns.Columns
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.guessColumnType
import org.jetbrains.dataframe.columns.name
import org.jetbrains.dataframe.columns.shortPath
import org.jetbrains.dataframe.columns.values
import org.jetbrains.dataframe.impl.DataFrameReceiver
import org.jetbrains.dataframe.impl.columns.toColumns
import org.jetbrains.dataframe.impl.createDataCollector
import org.jetbrains.dataframe.impl.getListType
import org.jetbrains.dataframe.impl.pathOf
import kotlin.reflect.KProperty
import kotlin.reflect.KType

internal class ValueWithDefault<T>(val value: T, val default: T)

internal class AggregateColumnWithOptions<C> private constructor(val columns: Columns<C>, private val default: C? = null, private val newPath: ColumnPath? = null): Columns<C> {

    override fun resolve(context: ColumnResolutionContext): List<ColumnWithPath<C>> {
        val resolved = columns.resolve(context)
        if(resolved.size == 1) return listOf(AggregateColumnDescriptor(resolved[0], default, newPath))
        else return resolved.map {
            AggregateColumnDescriptor(it, default, newPath?.plus(it.name))
        }
    }

    companion object {

        fun <C> withDefault(src: Columns<C>, default: C?): Columns<C> = when(src){
            is AggregateColumnWithOptions<C> -> AggregateColumnWithOptions(src.columns, default, src.newPath)
            else -> AggregateColumnWithOptions(src, default, null)
        }

        fun <C> withPath(src: Columns<C>, newPath: ColumnPath): Columns<C> = when(src){
            is AggregateColumnWithOptions<C> -> AggregateColumnWithOptions(src.columns, src.default, newPath)
            else -> AggregateColumnWithOptions(src, null, newPath)
        }
    }
}

interface AggregateSelectReceiver<out T> : SelectReceiver<T> {

    infix fun <C> Columns<C>.default(defaultValue: C): Columns<C> = AggregateColumnWithOptions.withDefault(this, defaultValue)

    fun path(vararg names: String): ColumnPath = names.asList()

    infix fun <C> Columns<C>.into(name: String): Columns<C> = AggregateColumnWithOptions.withPath(this, pathOf(name))

    infix fun <C> Columns<C>.into(path: ColumnPath): Columns<C> = AggregateColumnWithOptions.withPath(this, path)
}

typealias AggregateColumnsSelector<T, C> = AggregateSelectReceiver<T>.(AggregateSelectReceiver<T>) -> Columns<C>

interface Aggregatable<out T> {

    fun <R> aggregateBase(body: BaseAggregator<T, R>): DataFrame<T>

    fun <V> value(column: ColumnReference<V>) = withColumn { it[column] }
    fun value(column: String) = withColumn<T, Any?> { it.getColumn(column) }
    fun <V> value(column: ColumnSelector<T, V>) = withColumn { it[column] }

    fun values(vararg columns: Column) = values { columns.toColumns() }
    fun values(vararg columns: String) = values { columns.toColumns() }
    fun values(columns: AggregateColumnsSelector<T, *>) = yieldOneOrManyBy(columns) { it.toList() }

    fun values() = values(remainingColumnsSelector())

    // TODO: add missing overloads
    fun <R: Comparable<R>> maxBy(columns: AggregateColumnsSelector<T, R?>) = aggregateBy(columns, DataColumn<R?>::max)
    fun max() = maxBy(remainingColumns { it.isComparable() } as ColumnsSelector<T, Comparable<Any?>> )

    fun <R: Comparable<R>> minBy(columns: AggregateColumnsSelector<T, R?>) = aggregateBy(columns, DataColumn<R?>::min)
    fun min() = minBy(remainingColumns { it.isComparable() } as ColumnsSelector<T, Comparable<Any?>> )

    fun <R: Number> mean(skipNa: Boolean = true, columns: ColumnsSelector<T, R?>) = aggregateBy(columns) { it.mean(skipNa) }
    fun mean(skipNa: Boolean = true) = mean(skipNa, remainingColumns { it.isNumber() } as ColumnsSelector<T, Number> )

    fun <R: Number> sum(columns: ColumnsSelector<T, R>) = aggregateBy(columns, DataColumn<R>::sum)
    fun sum() = sum(remainingColumns { it.isNumber() } as ColumnsSelector<T, Number> )

    fun <R: Number> stdBy(columns: AggregateColumnsSelector<T, R>) = aggregateBy(columns, DataColumn<R>::std)
    fun std() = sum(remainingColumns { it.isNumber() } as ColumnsSelector<T, Number> )

    fun remainingColumnsSelector(): ColumnsSelector<*, *>
}

interface AggregatableDataFrame<T> : Aggregatable<T> {

}

interface AggregatableGroupBy<out T> : Aggregatable<T> {

    fun count(resultName: String = "count", predicate: RowFilter<T>? = null) = aggregateValue(resultName) { count(predicate) default 0}

    fun <R: Comparable<R>> minOf(resultName: String = "min", selector: RowSelector<T, R>) = aggregateValue(resultName) { minBy(selector) }
    fun <R: Comparable<R>> maxOf(resultName: String = "max", selector: RowSelector<T, R>) = aggregateValue(resultName) { maxBy(selector) }

    fun <R: Comparable<R>> min(columns: ColumnsSelector<T, R?>) = aggregateAll("min", true, columns, { it.min() }, { it.min() })
    fun <R: Comparable<R>> min(resultName: String, columns: ColumnsSelector<T, R?>) = aggregateAll(resultName, false, columns, { it.min() }, { it.min() })

    fun <R: Comparable<R>> max(columns: ColumnsSelector<T, R?>) = aggregateAll("max", true, columns, { it.max() }, { it.max() })
    fun <R: Comparable<R>> max(resultName: String, columns: ColumnsSelector<T, R?>) = aggregateAll(resultName, false, columns, { it.max() }, { it.max() })

    fun <R: Number> std(columns: ColumnsSelector<T, R?>) = aggregateAll("std", true, columns, { it.std() }, { it.std() })
    fun <R: Number> std(resultName: String, columns: ColumnsSelector<T, R?>) = aggregateAll(resultName, false, columns, { it.std() }, { it.std() })
}

interface AggregatablePivot<T>: Aggregatable<T> {

    fun count(predicate: RowFilter<T>? = null) = aggregateBase { count(predicate) default 0 }

    fun <R> matches(yes: R, no: R) = aggregate { yes default no }
    fun matches() = matches(true, false)

    override fun <R> aggregateBase(body: BaseAggregator<T, R>) = aggregate(body as PivotAggregator<T, R>)

    fun <R> aggregate(body: PivotAggregator<T, R>): DataFrame<T>

    fun groupByValue(flag: Boolean = true): AggregatablePivot<T>
    fun withGrouping(groupPath: ColumnPath): AggregatablePivot<T>

    fun into(column: Column) = value(column)
    fun into(column: String) = value(column)
    fun into(column: KProperty<*>) = value(column.name)

    fun <R: Comparable<R>> minOf(selector: RowSelector<T, R>) = aggregate { minBy(selector) }
    fun <R: Comparable<R>> maxOf(selector: RowSelector<T, R>) = aggregate { maxBy(selector) }

    fun <R: Comparable<R>> min(columns: ColumnsSelector<T, R?>) = aggregateAll(columns, { it.min() }, { it.min() })
    fun <R: Comparable<R>> max(columns: ColumnsSelector<T, R?>) = aggregateAll(columns, { it.max() }, { it.max() })

    fun <R: Number> std(columns: ColumnsSelector<T, R?>) = aggregateAll(columns, { it.std() }, { it.std() })
}

inline fun <T, reified R: Number> AggregatablePivot<T>.sumOf(crossinline selector: RowSelector<T, R>) = aggregate { sumBy(selector) }
inline fun <T, reified R: Number> AggregatableGroupBy<T>.sumOf(resultName: String = "sum", crossinline selector: RowSelector<T, R>) = aggregateValue(resultName) { sumBy(selector) }

inline fun <T, reified R: Number> AggregatablePivot<T>.meanOf(skipNa: Boolean = true, crossinline selector: RowSelector<T, R>): DataFrame<T> = aggregate { meanOf(skipNa, selector) }
inline fun <T, reified R: Number> AggregatableGroupBy<T>.meanOf(resultName: String = "mean", skipNa: Boolean = true, crossinline selector: RowSelector<T, R>): DataFrame<T> = aggregateValue(resultName) { meanOf(skipNa, selector) }

inline fun <T, reified C> AggregatablePivot<T>.valueOf(crossinline expression: RowSelector<T, C>): DataFrame<T> {
    val type = getType<C>()
    return aggregate {
        yieldOneOrMany(map(expression), type)
    }
}

inline fun <T, reified C> AggregatableGroupBy<T>.valueOf(name: String, crossinline expression: RowSelector<T, C>): DataFrame<T> {
    val type = getType<C>()
    val path = listOf(name)
    return aggregateBase {
        yieldOneOrMany(path, map(expression), type)
    }
}

@PublishedApi
internal fun <T, V> AggregateReceiver<T>.yieldOneOrMany(values: List<V>, type: KType) = yieldOneOrMany(emptyList(), values, type)

@PublishedApi
internal fun <T, V> AggregateReceiver<T>.yieldOneOrMany(path: ColumnPath, values: List<V>, type: KType, default: V? = null) {
    if (values.size == 1) yield(path, values[0], type, default)
    else yield(path, values.wrapValues(), getListType(type), default)
}

@PublishedApi
internal inline fun <T, reified R> Aggregatable<T>.aggregateValue(resultName: String, crossinline aggregator: AggregateReceiver<T>.(AggregateReceiver<T>)->R) = aggregateBase {
    val value = aggregator(this)
    yield(listOf(resultName), value)
}

internal fun <T, C, R> AggregatablePivot<T>.aggregateAll(columns: ColumnsSelector<T, C>, columnAggregator: (DataColumn<C>)->R, resultAggregator: (Iterable<R>)->R) =
    aggregateAll(emptyList(), columns, columnAggregator, resultAggregator)

internal fun <T, C, R> AggregatableGroupBy<T>.aggregateAll(name: String, useSingeColumnName: Boolean, columns: ColumnsSelector<T, C>, columnAggregator: (DataColumn<C>)->R, resultAggregator: (Iterable<R>)->R)  = aggregateBase {
    val cols = get(columns)
    if (cols.size == 1)
        yield(listOf(if (useSingeColumnName) cols[0].name() else name), columnAggregator(cols[0]))
    else
        yield(listOf(name), resultAggregator(cols.map(columnAggregator)))
}

internal fun <T, C, R> Aggregatable<T>.aggregateAll(path: ColumnPath, columns: ColumnsSelector<T, C>, columnAggregator: (DataColumn<C>)->R, resultAggregator: (Iterable<R>)->R) = aggregateBase {
    val cols = get(columns)
    if(cols.size == 1)
        yield(path, columnAggregator(cols[0]))
    else
        yield(path, resultAggregator(cols.map(columnAggregator)))
}

internal class AggregateColumnDescriptor<C>(val column: ColumnWithPath<C>, val default: C? = null, val newPath: ColumnPath? = null) : ColumnWithPath<C> by column

@JvmName("toColumnSetForAggregate")
internal fun <T, C> AggregateColumnsSelector<T, C>.toColumns(): Columns<C> = toColumns {

    class AggregateSelectReceiverImpl<T>(df: DataFrame<T>) : DataFrameReceiver<T>(df, true), AggregateSelectReceiver<T>

    AggregateSelectReceiverImpl(it.df.typed())
}

internal fun <T, C> DataFrame<T>.getAggregateColumns(selector: AggregateColumnsSelector<T, C>): List<AggregateColumnDescriptor<C>> {
    val columns = selector.toColumns().resolve(this, UnresolvedColumnsPolicy.Create)
    return columns.map {
        when (val col = it) {
            is AggregateColumnDescriptor<*> -> col as AggregateColumnDescriptor<C>
            else -> AggregateColumnDescriptor(it, null, null)
        }
    }
}

internal fun <T, C> AggregateReceiver<T>.getPath(col: AggregateColumnDescriptor<C>, isSingle: Boolean) =
    col.newPath ?: if(isSingle) pathForSingleColumn(col.data) else col.data.shortPath()

internal fun <T, C, R> Aggregatable<T>.yieldOneOrManyBy(columns: AggregateColumnsSelector<T, C>, aggregator: (DataColumn<C>)->List<R>) = aggregateBase {
    val cols = getAggregateColumns(columns)
    val isSingle = cols.size == 1
    cols.forEach { col ->
        val path = getPath(col, isSingle)
        yieldOneOrMany(path, aggregator(col.data), col.type, col.default)
    }
}

internal fun <T, C, R> Aggregatable<T>.aggregateBy(columns: AggregateColumnsSelector<T, C>, aggregator: (DataColumn<C>)->R) = aggregateBase {
    val cols = getAggregateColumns(columns)
    val isSingle = cols.size == 1
    cols.forEach { col ->
        val path = getPath(col, isSingle)
        yield(path, aggregator(col.data), col.type, col.default)
    }
}

internal inline fun <T> Aggregatable<T>.remainingColumns(crossinline predicate: (AnyCol) -> Boolean): ColumnsSelector<T, Any?> = remainingColumnsSelector().filter { predicate(it.data) }

interface AggregateReceiver<out T>: DataFrame<T> {

    fun yield(value: NamedValue): NamedValue

    fun <R> yield(path: ColumnPath, value: R, type: KType?, default: R?, guessType: Boolean) = yield(NamedValue.create(path, value, type, default, guessType))

    fun <R> yield(path: ColumnPath, value: R, type: KType? = null, default: R? = null): NamedValue

    fun pathForSingleColumn(column: AnyCol): ColumnPath

    fun <R> yield(value: R, type: KType? = null, default: R? = null): NamedValue

    infix fun <R> R.default(defaultValue: R): Any = when(this) {
        is NamedValue -> this.also { it.default = defaultValue }
        else -> ValueWithDefault(this, defaultValue)
    }
}

abstract class GroupReceiver<T> : AggregateReceiver<T> {

    override fun pathForSingleColumn(column: AnyCol) = column.shortPath()

    override fun <R> yield(path: ColumnPath, value: R, type: KType?, default: R?) = yield(path, value, type, default, false)

    override fun <R> yield(value: R, type: KType?, default: R?) = yield(listOf("value"), value, type, default)

    inline infix fun <reified R> R.into(name: String)  = yield(listOf(name), this, getType<R>())
}

internal class GroupReceiverImpl<T>(internal val df: DataFrame<T>): GroupReceiver<T>(), DataFrame<T> by df {

    private val values = mutableListOf<NamedValue>()

    internal fun child(): GroupReceiverImpl<T> {
        val child = GroupReceiverImpl(df)
        values.add(NamedValue.aggregator(child))
        return child
    }

    private fun NamedValue.toColumnWithPath() = path to guessColumnType(
        path.last(),
        listOf(value),
        type,
        guessType,
        default
    )

    internal fun compute(): AnyFrame {

        val allValues = mutableListOf<NamedValue>()
        values.forEach {
            if(it.value is GroupReceiverImpl<*>){
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

    override fun yield(value: NamedValue): NamedValue {
        when(value.value) {
            is AggregatedPivot<*> -> {
                value.value.aggregator.values.forEach {
                    yield(value.path + it.path, it.value, it.type, it.default, it.guessType)
                }
                value.value.aggregator.values.clear()
            }
            else -> values.add(value)
        }
        return value
    }
}

typealias GroupAggregator<G> = GroupReceiver<G>.(GroupReceiver<G>) -> Unit

fun <T, G> GroupedDataFrame<T, G>.aggregate(body: GroupAggregator<G>) = aggregateGroupBy(plain(), { groups }, removeColumns = true, body)

data class AggregateClause<T, G>(val df: DataFrame<T>, val selector: ColumnSelector<T, DataFrame<G>>){
    fun with(body: GroupAggregator<G>) = aggregateGroupBy(df, selector, removeColumns = false, body)
}

fun <T, G> DataFrame<T>.aggregate(selector: ColumnSelector<T, DataFrame<G>>) = AggregateClause(this, selector)

internal fun <T, G> aggregateGroupBy(df: DataFrame<T>, selector: ColumnSelector<T, DataFrame<G>?>, removeColumns: Boolean, body: GroupAggregator<G>): DataFrame<T> {

    val column = df.column(selector)

    val (df2, removedNodes) = df.doRemove(selector)

    val groupedFrame = column.values.map {
        if(it == null) null
        else {
            val builder = GroupReceiverImpl(it)
            body(builder, builder)
            builder.compute()
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