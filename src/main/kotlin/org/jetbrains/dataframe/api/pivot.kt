package org.jetbrains.dataframe

import org.jetbrains.dataframe.aggregation.Aggregatable
import org.jetbrains.dataframe.aggregation.AggregateReceiver
import org.jetbrains.dataframe.aggregation.GroupByReceiver
import org.jetbrains.dataframe.api.aggregation.PivotAggregations
import org.jetbrains.dataframe.api.aggregation.asGrouped
import org.jetbrains.dataframe.columns.AnyCol
import org.jetbrains.dataframe.impl.aggregation.AggregateColumnDescriptor
import org.jetbrains.dataframe.impl.aggregation.GroupByReceiverImpl
import org.jetbrains.dataframe.impl.aggregation.ValueWithDefault
import org.jetbrains.dataframe.impl.aggregation.getPath
import org.jetbrains.dataframe.impl.aggregation.receivers.PivotReceiverImpl
import org.jetbrains.dataframe.impl.aggregation.yieldOneOrMany
import org.jetbrains.dataframe.impl.columns.toColumns
import org.jetbrains.dataframe.impl.emptyPath
import kotlin.reflect.KType

fun <T> DataFrame<T>.pivot(columns: ColumnsSelector<T, *>) = DataFramePivot(this, columns)
fun <T> DataFrame<T>.pivot(vararg columns: String) = pivot { columns.toColumns() }
fun <T> DataFrame<T>.pivot(vararg columns: Column) = pivot { columns.toColumns() }

fun <T> DataFramePivot<T>.groupBy(columns: ColumnsSelector<T, *>): GroupedPivot<T> = GroupedPivot(df.groupBy(columns), this.columns, groupValues, default, groupPath)
fun <T> DataFramePivot<T>.groupBy(vararg columns: String) = groupBy { columns.toColumns() }
fun <T> DataFramePivot<T>.groupBy(vararg columns: Column) = groupBy { columns.toColumns() }

fun <T> GroupedDataFrame<*, T>.pivot(columns: ColumnsSelector<T, *>) = GroupedPivot(this, columns)
fun <T> GroupedDataFrame<*, T>.pivot(vararg columns: Column) = pivot { columns.toColumns() }
fun <T> GroupedDataFrame<*, T>.pivot(vararg columns: String) = pivot { columns.toColumns() }

fun <T> GroupByReceiver<T>.pivot(columns: ColumnsSelector<T, *>) = GroupAggregatorPivot(this, columns)
fun <T> GroupByReceiver<T>.pivot(vararg columns: Column) = pivot { columns.toColumns() }
fun <T> GroupByReceiver<T>.pivot(vararg columns: String) = pivot { columns.toColumns() }

fun <T, P: GroupedPivotAggregations<T>> P.withGrouping(group: MapColumnReference) = withGrouping(group.path()) as P
fun <T, P: GroupedPivotAggregations<T>> P.withGrouping(groupName: String) = withGrouping(listOf(groupName)) as P

data class GroupedPivot<T>(
    internal val df: GroupedDataFrame<*, T>,
    internal val columns: ColumnsSelector<T, *>,
    internal val groupValues: Boolean = false,
    internal val default: Any? = null,
    internal val groupPath: ColumnPath = emptyList()
) : GroupedPivotAggregations<T> {
    override fun <R> aggregate(body: PivotAggregateBody<T, R>): DataFrame<T> {
        return df.aggregate {
            aggregatePivot(this, columns, groupValues, groupPath, default, body)
        }.typed()
    }

    override fun groupByValue(flag: Boolean) = if(flag == groupValues) this else copy(groupValues = flag)

    override fun withGrouping(groupPath: ColumnPath) = copy(groupPath = groupPath)

    override fun default(value: Any?) = copy(default = value)

    override fun remainingColumnsSelector(): ColumnsSelector<*, *> = { all().except(columns.toColumns() and df.keys.columnNames().toColumns()) }
}

data class DataFramePivot<T>(
    internal val df: DataFrame<T>,
    internal val columns: ColumnsSelector<T, *>,
    internal val groupValues: Boolean = false,
    internal val default: Any? = null,
    internal val groupPath: ColumnPath = emptyList()
) : PivotAggregations<T> {

    fun <R> aggregate(body: PivotAggregateBody<T, R>): DataRow<T> = asGrouped().aggregate(body)[0]

    override fun remainingColumnsSelector(): ColumnsSelector<*, *> = { all().except(columns.toColumns()) }

    override fun <R> aggregateBase(body: AggregateBody<T, R>) = asGrouped().aggregateBase(body)
}

internal class AggregatedPivot<T>(private val df: DataFrame<T>, internal var aggregator: GroupByReceiverImpl<T>) :
    DataFrame<T> by df

data class GroupAggregatorPivot<T>(
    internal val aggregator: GroupByReceiver<T>,
    internal val columns: ColumnsSelector<T, *>,
    internal val groupValues: Boolean = false,
    internal val default: Any? = null,
    internal val groupPath: ColumnPath = emptyList()
) : GroupedPivotAggregations<T> {

    override fun groupByValue(flag: Boolean) = if(flag == groupValues) this else copy(groupValues = flag)

    override fun default(value: Any?) = copy(default = value)

    override fun withGrouping(groupPath: ColumnPath) = copy(groupPath = groupPath)

    override fun <R> aggregate(body: PivotAggregateBody<T, R>): DataFrame<T> {

        require(aggregator is GroupByReceiverImpl<T>)

        val childAggregator = aggregator.child()
        aggregatePivot(childAggregator, columns, groupValues, groupPath, default, body)
        return AggregatedPivot(aggregator.df, childAggregator)
    }

    override fun remainingColumnsSelector(): ColumnsSelector<*, *> = { all().except(columns.toColumns()) }
}

internal fun <T, R> aggregatePivot(
    aggregator: GroupByReceiver<T>,
    columns: ColumnsSelector<T, *>,
    groupValues: Boolean,
    groupPath: ColumnPath,
    default: Any? = null,
    body: PivotAggregateBody<T, R>
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

typealias AggregateBody<T, R> = AggregateReceiver<T>.(AggregateReceiver<T>) -> R

typealias PivotAggregateBody<T, R> = PivotReceiver<T>.(PivotReceiver<T>) -> R

data class ValueWithName(val value: Any?, val name: String)

@Suppress("DataClassPrivateConstructor")
data class NamedValue private constructor(val path: ColumnPath, val value: Any?, val type: KType?, var default: Any?, val guessType: Boolean = false) {
    companion object {
        fun create(path: ColumnPath, value: Any?, type: KType?, defaultValue: Any?, guessType: Boolean = false): NamedValue = when(value){
            is ValueWithDefault<*> -> create(path, value.value, type, value.default, guessType)
            is ValueWithName -> create(path.replaceLast(value.name), value.value, type, defaultValue, guessType)
            else -> NamedValue(path, value, type, defaultValue, guessType)
        }
        fun aggregator(builder: GroupByReceiver<*>) = NamedValue(emptyPath(), builder, null, null, false)
    }

    val name: String get() = path.last()
}

abstract class PivotReceiver<T>: AggregateReceiver<T> {

    override fun pathForSingleColumn(column: AnyCol) = emptyPath()

    override fun <R> yield(path: ColumnPath, value: R, type: KType?, default: R?) = yield(path, value, type, default, true)

    inline infix fun <reified R> R.into(name: String) = yield(listOf(name), this, getType<R>())
}

