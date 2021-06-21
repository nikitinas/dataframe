package org.jetbrains.dataframe

import org.jetbrains.dataframe.aggregation.AggregateReceiver
import org.jetbrains.dataframe.aggregation.GroupByReceiver
import org.jetbrains.dataframe.aggregation.PivotReceiver
import org.jetbrains.dataframe.api.aggregation.PivotAggregations
import org.jetbrains.dataframe.impl.aggregation.DataFramePivotImpl
import org.jetbrains.dataframe.impl.aggregation.GroupByReceiverImpl
import org.jetbrains.dataframe.impl.aggregation.ValueWithDefault
import org.jetbrains.dataframe.impl.aggregation.receivers.AggregateReceiverInternal
import org.jetbrains.dataframe.impl.aggregation.receivers.PivotReceiverImpl
import org.jetbrains.dataframe.impl.columns.toColumns
import org.jetbrains.dataframe.impl.emptyPath
import kotlin.reflect.KType

fun <T> DataFrame<T>.pivot(columns: ColumnsSelector<T, *>): PivotAggregations<T> = DataFramePivotImpl(this, columns)
fun <T> DataFrame<T>.pivot(vararg columns: String) = pivot { columns.toColumns() }
fun <T> DataFrame<T>.pivot(vararg columns: Column) = pivot { columns.toColumns() }

fun <T, P: GroupedPivotAggregations<T>> P.withGrouping(group: MapColumnReference) = withGrouping(group.path()) as P
fun <T, P: GroupedPivotAggregations<T>> P.withGrouping(groupName: String) = withGrouping(listOf(groupName)) as P


internal class AggregatedPivot<T>(private val df: DataFrame<T>, internal var aggregator: GroupByReceiverImpl<T>) :
    DataFrame<T> by df


internal fun <T, R> aggregatePivot(
    aggregator: AggregateReceiverInternal<T>,
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

