package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.AnyCol
import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.guessColumnType
import org.jetbrains.dataframe.columns.shortPath
import org.jetbrains.dataframe.impl.asList
import org.jetbrains.dataframe.impl.columns.toColumns
import org.jetbrains.dataframe.impl.getListType
import org.jetbrains.dataframe.impl.nameGenerator
import kotlin.reflect.KProperty
import kotlin.reflect.KType

fun <T> DataFrame<T>.pivot(columns: ColumnsSelector<T, *>) = DataFramePivot(this, columns)
fun <T> DataFrame<T>.pivot(vararg columns: String) = pivot { columns.toColumns() }
fun <T> DataFrame<T>.pivot(vararg columns: Column) = pivot { columns.toColumns() }

fun <T> DataFramePivot<T>.withIndex(indexColumn: ColumnsSelector<T, *>) = copy(index = indexColumn)
fun <T> DataFramePivot<T>.withIndex(indexColumn: String) = withIndex { indexColumn.toColumnDef() }
fun <T> DataFramePivot<T>.withIndex(indexColumn: Column) = withIndex { indexColumn }

fun <T> GroupedDataFrame<*, T>.pivot(columns: ColumnsSelector<T, *>) = GroupedFramePivot(this, columns)
fun <T> GroupedDataFrame<*, T>.pivot(vararg columns: Column) = pivot { columns.toColumns() }
fun <T> GroupedDataFrame<*, T>.pivot(vararg columns: String) = pivot { columns.toColumns() }

fun <T> GroupAggregateBuilder<T>.pivot(columns: ColumnsSelector<T, *>) = GroupAggregatorPivot(this, columns)
fun <T> GroupAggregateBuilder<T>.pivot(vararg columns: Column) = pivot { columns.toColumns() }
fun <T> GroupAggregateBuilder<T>.pivot(vararg columns: String) = pivot { columns.toColumns() }

inline fun <T, reified C> PivotClause<T>.valueOf(crossinline expression: RowSelector<T, C>): DataFrame<T> {
    val type = getType<C>()
    return aggregate {
        if (nrow() == 1) {
            val row = get(0)
            yield(expression(row, row), type)
        } else yield(map { expression(it) }.wrapValues(), getListType(type))
    }
}

// TODO: add  overloads
internal fun <T, V> PivotClause<T>.withColumn(column: DataFrame<T>.(DataFrame<T>) -> DataColumn<V>) = aggregate {
    val data = column(this)
    yieldOneOrMany(data.toList(), data.type())
}

fun <T, V> PivotAggregateBuilder<T>.yieldOneOrMany(values: List<V>, type: KType) = yieldOneOrMany(emptyList(), values, type)

internal fun <T, V> PivotAggregateBuilder<T>.yieldOneOrMany(path: ColumnPath, values: List<V>, type: KType) {
    if (values.size == 1) yield(path, values[0], type)
    else yield(path, values.wrapValues(), getListType(type))
}

interface PivotClause<T> {
    fun <R> aggregate(body: PivotAggregator<T, R>): DataFrame<T>

    fun groupByValue(flag: Boolean = true): PivotClause<T>
    fun withGrouping(groupPath: ColumnPath): PivotClause<T>

    fun into(column: Column) = value(column)
    fun into(column: String) = value(column)
    fun into(column: KProperty<*>) = value(column.name)

    fun matches() = matches(true, false)
    fun <R> matches(yes: R, no: R) = aggregate { yes default no }

    fun <V> value(column: ColumnReference<V>) = withColumn { it[column] }
    fun value(column: String) = withColumn<T, Any?> { it.getColumn(column) }
    fun <V> value(column: ColumnSelector<T, V>) = withColumn { it[column] }

    fun values(selector: ColumnsSelector<T, *>) = aggregate {
        get(selector).forEach { col -> yieldOneOrMany(col.shortPath(), col.toList(), col.type()) }
    }

    fun values() = values(remainingColumnsSelector())

    fun remainingColumnsSelector(): ColumnsSelector<T, *>

    fun count() = aggregate { nrow() default 0 }

    // TODO: add missing overloads
    fun <R: Comparable<R>> max(columns: ColumnsSelector<T, R?>) = aggregate(columns, DataColumn<R?>::max)
    fun max() = max(remainingColumns { it.isComparable() } as ColumnsSelector<T, Comparable<Any?>> )
    fun <R: Comparable<R>> maxOf(selector: RowSelector<T, R>) = aggregate { maxBy(selector) }

    fun <R: Comparable<R>> min(columns: ColumnsSelector<T, R?>) = aggregate(columns, DataColumn<R?>::min)
    fun min() = min(remainingColumns { it.isComparable() } as ColumnsSelector<T, Comparable<Any?>> )
    fun <R: Comparable<R>> minOf(selector: RowSelector<T, R>) = aggregate { minBy(selector) }

    fun <R: Number> sum(columns: ColumnsSelector<T, R>) = aggregate(columns, DataColumn<R>::sum)
    fun sum() = sum(remainingColumns { it.isNumber() } as ColumnsSelector<T, Number> )

    fun <R: Number> mean(skipNa: Boolean = true, columns: ColumnsSelector<T, R>) = aggregate(columns) { it.mean(skipNa) }
    fun mean(skipNa: Boolean = true) = mean(skipNa, remainingColumns { it.isNumber() } as ColumnsSelector<T, Number> )
}

internal inline fun <T> PivotClause<T>.remainingColumns(crossinline predicate: (AnyCol) -> Boolean): ColumnsSelector<T, Any?> = remainingColumnsSelector().filter { predicate(it.data) }

internal fun <T, C, R> PivotClause<T>.aggregate(columns: ColumnsSelector<T, C>, aggregator: (DataColumn<C>)->R) = aggregate {
    val cols = get(columns)
    val isSingleColumn = cols.size == 1
    get(columns).forEach { col -> yield(if(isSingleColumn) emptyList() else col.shortPath(), aggregator(col), col.type(), col.defaultValue()) }
}

fun <T, P:PivotClause<T>> P.withGrouping(group: MapColumnReference) = withGrouping(group.path()) as P
fun <T, P:PivotClause<T>> P.withGrouping(groupName: String) = withGrouping(listOf(groupName)) as P

// TODO: add overloads
inline fun <T, reified R: Number> PivotClause<T>.sumOf(crossinline selector: RowSelector<T, R>) = aggregate { sumBy(selector) }

// TODO: add overloads
inline fun <T, reified R: Number> PivotClause<T>.meanOf(skipNa: Boolean = true, crossinline selector: RowSelector<T, R>): DataFrame<T> = aggregate { meanOf(skipNa, selector) }

inline fun <T, reified V> PivotClause<T>.into(noinline selector: RowSelector<T, V>): DataFrame<T> {
    val type = getType<V>()
    return aggregate {
        val values = map {
            val value = selector(it, it)
            if (value is ColumnReference<*>) it[value]
            else value
        }
        yieldOneOrMany(values, type)
    }
}

data class GroupedFramePivot<T>(
    internal val df: GroupedDataFrame<*, T>,
    internal val columns: ColumnsSelector<T, *>,
    internal val groupValues: Boolean = false,
    internal val groupPath: ColumnPath = emptyList()
) : PivotClause<T> {
    override fun <R> aggregate(body: PivotAggregator<T, R>): DataFrame<T> {
        return df.aggregate {
            aggregatePivot(this, columns, groupValues, groupPath, body)
        }.typed()
    }

    override fun groupByValue(flag: Boolean) = copy(groupValues = flag)
    override fun withGrouping(groupPath: ColumnPath) = copy(groupPath = groupPath)

    override fun remainingColumnsSelector(): ColumnsSelector<T, *> = { all().except(columns.toColumns()) }
}

data class DataFramePivot<T>(
    internal val df: DataFrame<T>,
    internal val columns: ColumnsSelector<T, *>,
    internal val index: ColumnsSelector<T, *>? = null,
    internal val groupValues: Boolean = false,
    internal val groupPath: ColumnPath = emptyList()
) : PivotClause<T> {

    override fun groupByValue(flag: Boolean) = copy(groupValues = flag)
    override fun withGrouping(groupPath: ColumnPath) = copy(groupPath = groupPath)

    override fun <R> aggregate(body: PivotAggregator<T, R>): DataFrame<T> {
        if (index != null) {
            val grouped = df.groupBy(index)
            return GroupedFramePivot(grouped, columns, groupValues, groupPath).aggregate(body)
        } else {

            data class RowData(val name: String, val type: KType?, val defaultValue: Any?)

            val rows = mutableListOf<RowData>()

            val valueNameIndex = mutableMapOf<String, Int>()

            // compute values for every column
            val data = df.groupBy(columns).map { key, group ->
                val keyValue = key.values()
                val path = keyValue.map { it.toString() }
                val builder = PivotAggregateBuilder(group)
                val result = body(builder, builder)
                val hasResult = result != Unit
                val values = if (builder.values.isEmpty())
                    if (hasResult) listOf(NamedValue(emptyList(), result, null, null))
                    else emptyList()
                else builder.values
                val columnData = mutableListOf<Any?>()
                // TODO: support column paths
                var dataSize = 0
                values.forEach {
                    val name = if (it.path.isEmpty()) "" else it.path.last()
                    val index = valueNameIndex[name] ?: run {
                        val newIndex = rows.size
                        rows.add(RowData(name, it.type, it.defaultValue))
                        valueNameIndex[name] = newIndex
                        newIndex
                    }
                    while (dataSize < index) {
                        columnData.add(rows[dataSize++].defaultValue)
                    }
                    if (dataSize == index) {
                        columnData.add(it.value)
                        dataSize++
                    } else columnData[index] = it.value
                }
                path to columnData
            }

            val nrow = rows.size

            // use original value type if it is common for all values
            val commonType = rows.mapNotNull { it.type }.singleOrNull()

            // Align column sizes and create dataframe
            var result = data.map { (path, values) ->
                while (values.size < nrow)
                    values.add(rows[values.size].defaultValue)
                path to guessColumnType(path.last(), values.asList(), commonType, true)
            }.toDataFrame<Any>()

            if (nrow > 1) {
                val nameGenerator = result.nameGenerator()
                val indexName = nameGenerator.addUnique(defaultPivotIndexName)
                val col = rows.map { it.name }.toColumn(indexName)
                result = result.insert(col).at(0)
            }
            return result.typed()
        }
    }

    override fun remainingColumnsSelector(): ColumnsSelector<T, *> = { all().except(index?.toColumns()?.and(columns.toColumns()) ?: columns.toColumns()) }
}

class AggregatedPivot<T>(private val df: DataFrame<T>, internal var aggregator: GroupAggregateBuilder<T>) :
    DataFrame<T> by df

data class GroupAggregatorPivot<T>(
    internal val aggregator: GroupAggregateBuilder<T>,
    internal val columns: ColumnsSelector<T, *>,
    internal val groupValues: Boolean = false,
    internal val groupPath: ColumnPath = emptyList()
) : PivotClause<T> {

    override fun groupByValue(flag: Boolean) = copy(groupValues = flag)
    override fun withGrouping(groupPath: ColumnPath) = copy(groupPath = groupPath)

    override fun <R> aggregate(body: PivotAggregator<T, R>): AggregatedPivot<T> {
        val childAggregator = aggregator.child()
        aggregatePivot(childAggregator, columns, groupValues, groupPath, body)
        return AggregatedPivot(aggregator.df, childAggregator)
    }

    override fun remainingColumnsSelector(): ColumnsSelector<T, *> = { all().except(columns.toColumns()) }
}

internal fun <T, R> aggregatePivot(
    aggregator: GroupAggregateBuilder<T>,
    columns: ColumnsSelector<T, *>,
    groupValues: Boolean,
    groupPath: ColumnPath,
    body: PivotAggregator<T, R>
) {

    aggregator.groupBy(columns).forEach { key, group ->

        val keyValue = key.values()
        val path = keyValue.map { it.toString() }
        val builder = PivotAggregateBuilder(group!!)
        val result = body(builder, builder)
        val hasResult = result != null && result != Unit

        val values = builder.values
        when {
            values.size == 1 && values[0].path.isEmpty() -> aggregator.addValue(groupPath + path, values[0].value, values[0].type, guessType = true)
            values.isEmpty() -> aggregator.addValue(groupPath + path, if (hasResult) result else null, guessType = true)
            else -> {
                values.forEach {
                    val targetPath = groupPath + if (groupValues) it.path + path else path + it.path
                    aggregator.addValue(targetPath, it.value, it.type, it.defaultValue, it.guessType)
                }
            }
        }
    }
}

internal val defaultPivotIndexName = "index"

typealias PivotAggregator<T, R> = PivotAggregateBuilder<T>.(PivotAggregateBuilder<T>) -> R

class PivotAggregateBuilder<T>(internal val df: DataFrame<T>) : DataFrame<T> by df {

    internal val values = mutableListOf<NamedValue>()

    fun <R> yield(path: ColumnPath, value: R, type: KType? = null, default: R? = null) {
        values.add(NamedValue(path, value, type, default, true))
    }

    fun <R> yield(value: R, type: KType? = null, default: R? = null) {
        values.add(NamedValue(emptyList(), value, type, default, true))
    }

    infix fun <R> R.default(defaultValue: R): Any = ValueWithDefault(this, defaultValue)

    inline infix fun <reified R> R.into(name: String) = yield(listOf(name), this, getType<R>())
}