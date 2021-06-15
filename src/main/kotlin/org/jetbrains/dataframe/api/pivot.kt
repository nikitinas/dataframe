package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.AnyCol
import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.columns.guessColumnType
import org.jetbrains.dataframe.impl.asList
import org.jetbrains.dataframe.impl.columns.toColumns
import org.jetbrains.dataframe.impl.emptyPath
import org.jetbrains.dataframe.impl.nameGenerator
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

fun <T> GroupReceiver<T>.pivot(columns: ColumnsSelector<T, *>) = GroupAggregatorPivot(this, columns)
fun <T> GroupReceiver<T>.pivot(vararg columns: Column) = pivot { columns.toColumns() }
fun <T> GroupReceiver<T>.pivot(vararg columns: String) = pivot { columns.toColumns() }

// TODO: add  overloads
inline internal fun <T, V> Aggregatable<T>.withColumn(crossinline getColumn: DataFrame<T>.(DataFrame<T>) -> AggregateColumnDescriptor<V>) = aggregateBase {
    val column = getColumn(this)
    val path = getPath(column, true)
    yieldOneOrMany(path, column.data.toList(), column.type, column.default)
}

fun <T, P:AggregatablePivot<T>> P.withGrouping(group: MapColumnReference) = withGrouping(group.path()) as P
fun <T, P:AggregatablePivot<T>> P.withGrouping(groupName: String) = withGrouping(listOf(groupName)) as P

inline fun <T, reified V> AggregatablePivot<T>.into(noinline selector: RowSelector<T, V>): DataFrame<T> {
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
    internal val default: Any? = null,
    internal val groupPath: ColumnPath = emptyList()
) : AggregatablePivot<T> {
    override fun <R> aggregate(body: PivotAggregator<T, R>): DataFrame<T> {
        return df.aggregate {
            aggregatePivot(this, columns, groupValues, groupPath, default, body)
        }.typed()
    }

    override fun groupByValue(flag: Boolean) = copy(groupValues = flag)

    override fun withGrouping(groupPath: ColumnPath) = copy(groupPath = groupPath)

    override fun withDefault(value: Any?) = copy(default = value)

    override fun remainingColumnsSelector(): ColumnsSelector<*, *> = { all().except(columns.toColumns()) }
}

data class DataFramePivot<T>(
    internal val df: DataFrame<T>,
    internal val columns: ColumnsSelector<T, *>,
    internal val index: ColumnsSelector<T, *>? = null,
    internal val groupValues: Boolean = false,
    internal val default: Any? = null,
    internal val groupPath: ColumnPath = emptyList()
) : AggregatablePivot<T> {

    override fun groupByValue(flag: Boolean) = copy(groupValues = flag)

    override fun withDefault(value: Any?) = copy(default = value)

    override fun withGrouping(groupPath: ColumnPath) = copy(groupPath = groupPath)

    override fun <R> aggregate(body: PivotAggregator<T, R>): DataFrame<T> {
        if (index != null) {
            val grouped = df.groupBy(index)
            return GroupedFramePivot(grouped, columns, groupValues, default, groupPath).aggregate(body)
        } else {

            data class RowData(val name: String, val type: KType?, val defaultValue: Any?)

            val rows = mutableListOf<RowData>()

            val valueNameIndex = mutableMapOf<String, Int>()

            // compute values for every column
            val data = df.groupBy(columns).map { key, group ->
                val keyValue = key.values()
                val path = keyValue.map { it.toString() }
                val builder = PivotReceiverImpl(group)
                val result = body(builder, builder)
                val hasResult = result != Unit
                val values = if (builder.values.isEmpty())
                    if (hasResult) listOf(NamedValue.create(emptyPath(), result, null, default))
                    else emptyList()
                else builder.values
                val columnData = mutableListOf<Any?>()
                // TODO: support column paths
                var dataSize = 0
                values.forEach {
                    val name = if (it.path.isEmpty()) "" else it.path.last()
                    val index = valueNameIndex[name] ?: run {
                        val newIndex = rows.size
                        rows.add(RowData(name, it.type, it.default))
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

    override fun remainingColumnsSelector(): ColumnsSelector<*, *> = { all().except(index?.toColumns()?.and(columns.toColumns()) ?: columns.toColumns()) }
}

internal class AggregatedPivot<T>(private val df: DataFrame<T>, internal var aggregator: GroupReceiverImpl<T>) :
    DataFrame<T> by df

data class GroupAggregatorPivot<T>(
    internal val aggregator: GroupReceiver<T>,
    internal val columns: ColumnsSelector<T, *>,
    internal val groupValues: Boolean = false,
    internal val default: Any? = null,
    internal val groupPath: ColumnPath = emptyList()
) : AggregatablePivot<T> {

    override fun groupByValue(flag: Boolean) = copy(groupValues = flag)

    override fun withDefault(value: Any?) = copy(default = value)

    override fun withGrouping(groupPath: ColumnPath) = copy(groupPath = groupPath)

    override fun <R> aggregate(body: PivotAggregator<T, R>): DataFrame<T> {

        require(aggregator is GroupReceiverImpl<T>)

        val childAggregator = aggregator.child()
        aggregatePivot(childAggregator, columns, groupValues, groupPath, default, body)
        return AggregatedPivot(aggregator.df, childAggregator)
    }

    override fun remainingColumnsSelector(): ColumnsSelector<*, *> = { all().except(columns.toColumns()) }
}

internal fun <T, R> aggregatePivot(
    aggregator: GroupReceiver<T>,
    columns: ColumnsSelector<T, *>,
    groupValues: Boolean,
    groupPath: ColumnPath,
    default: Any? = null,
    body: PivotAggregator<T, R>
) {

    aggregator.groupBy(columns).forEach { key, group ->

        val keyValue = key.values()
        val path = keyValue.map { it.toString() }
        val builder = PivotReceiverImpl(group!!)
        val result = body(builder, builder)
        val hasResult = result != null && result != Unit

        val values = builder.values
        when {
            values.size == 1 && values[0].path.isEmpty() -> aggregator.yield(values[0].copy(path = groupPath + path, default = values[0].default ?: default))
            values.isEmpty() -> aggregator.yield(groupPath + path, if (hasResult) result else null, null, default, true)
            else -> {
                values.forEach {
                    val targetPath = groupPath + if (groupValues) it.path + path else path + it.path
                    aggregator.yield(targetPath, it.value, it.type, it.default ?: default, it.guessType)
                }
            }
        }
    }
}

internal val defaultPivotIndexName = "index"

typealias BaseAggregator<T, R> = AggregateReceiver<T>.(AggregateReceiver<T>) -> R

typealias PivotAggregator<T, R> = PivotReceiver<T>.(PivotReceiver<T>) -> R

data class ValueWithName(val value: Any?, val name: String)

@Suppress("DataClassPrivateConstructor")
data class NamedValue private constructor(val path: ColumnPath, val value: Any?, val type: KType?, var default: Any?, val guessType: Boolean = false) {
    companion object {
        fun create(path: ColumnPath, value: Any?, type: KType?, defaultValue: Any?, guessType: Boolean = false): NamedValue = when(value){
            is ValueWithDefault<*> -> create(path, value.value, type, value.default, guessType)
            is ValueWithName -> create(path.replaceLast(value.name), value.value, type, defaultValue, guessType)
            else -> NamedValue(path, value, type, defaultValue, guessType)
        }
        fun aggregator(builder: GroupReceiver<*>) = NamedValue(emptyPath(), builder, null, null, false)
    }
}

abstract class PivotReceiver<T>: AggregateReceiver<T> {

    override fun pathForSingleColumn(column: AnyCol) = emptyPath()

    override fun <R> yield(path: ColumnPath, value: R, type: KType?, default: R?) = yield(path, value, type, default, true)

    override fun <R> yield(value: R, type: KType?, default: R?) = yield(emptyPath(), value, type, default)

    inline infix fun <reified R> R.into(name: String) = yield(listOf(name), this, getType<R>())
}

internal class PivotReceiverImpl<T>(internal val df: DataFrame<T>) : PivotReceiver<T>(), DataFrame<T> by df {

    internal val values = mutableListOf<NamedValue>()

    override fun yield(value: NamedValue) = value.also { values.add(it) }
}