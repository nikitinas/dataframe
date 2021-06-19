package org.jetbrains.dataframe

import org.jetbrains.dataframe.aggregation.AggregateColumnsSelector
import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.impl.aggregation.aggregators.Aggregators
import org.jetbrains.dataframe.impl.aggregation.comparableColumns
import org.jetbrains.dataframe.impl.aggregation.modes.aggregateAll
import org.jetbrains.dataframe.impl.aggregation.modes.aggregateFor
import org.jetbrains.dataframe.impl.aggregation.modes.of
import org.jetbrains.dataframe.impl.aggregation.numberColumns
import org.jetbrains.dataframe.impl.aggregation.yieldOneOrMany
import org.jetbrains.dataframe.impl.aggregation.yieldOneOrManyBy
import org.jetbrains.dataframe.impl.columns.toColumns
import kotlin.reflect.KProperty

interface PivotAggregations<T> : PivotOrGroupByAggregations<T> {

    fun count(predicate: RowFilter<T>? = null) = aggregateBase { count(predicate) default 0 }

    fun values(vararg columns: Column, separate: Boolean = false): DataFrame<T> = values(separate) { columns.toColumns() }
    fun values(vararg columns: String, separate: Boolean = false): DataFrame<T> = values(separate) { columns.toColumns() }
    fun values(vararg columns: KProperty<*>, separate: Boolean = false): DataFrame<T> = values(separate) { columns.toColumns() }
    fun values(separate: Boolean = false, columns: AggregateColumnsSelector<T, *>): DataFrame<T> = groupByValue(separate).yieldOneOrManyBy(columns) { it.toList() }

    fun values(separate: Boolean = false): DataFrame<T> = values(separate, remainingColumnsSelector())

    fun <R> matches(yes: R, no: R) = aggregate { yes default no }
    fun matches() = matches(true, false)

    override fun <R> aggregateBase(body: AggregateBody<T, R>) = aggregate(body as PivotAggregateBody<T, R>)

    fun <R> aggregate(body: PivotAggregateBody<T, R>): DataFrame<T>

    fun groupByValue(flag: Boolean = true): PivotAggregations<T>
    fun default(value: Any?): PivotAggregations<T>
    fun withGrouping(groupPath: ColumnPath): PivotAggregations<T>

    // region min

    fun <R : Comparable<R>> min(separate: Boolean = false): DataFrame<T> = minFor(separate, comparableColumns())

    fun <R : Comparable<R>> minFor(separate: Boolean = false, columns: ColumnsSelector<T, R?>): DataFrame<T> = Aggregators.min.aggregateFor(groupByValue(separate), columns)

    fun <R : Comparable<R>> min(columns: ColumnsSelector<T, R?>): DataFrame<T> = Aggregators.min.aggregateAll(this, columns)

    fun <R : Comparable<R>> minOf(selector: RowSelector<T, R>): DataFrame<T> = aggregate { minOf(selector) }

    fun <R : Comparable<R>> minBy(column: ColumnSelector<T, R>): DataFrame<T> = aggregate { minBy(column) }

    fun <R : Comparable<R>> minByExpr(rowExpression: RowSelector<T, R>): DataFrame<T> = aggregate { minByExpr(rowExpression) }

    // endregion

    // region max

    fun <R : Comparable<R>> max(columns: ColumnsSelector<T, R?>): DataFrame<T> = Aggregators.max.aggregateAll(this, columns)

    fun <R : Comparable<R>> maxOf(selector: RowSelector<T, R>) = aggregate { maxOf(selector) }

    // endregion

    // region mean

    fun <R : Number> mean(skipNa: Boolean = true, columns: ColumnsSelector<T, R?>): DataFrame<T> = Aggregators.mean(skipNa).aggregateFor(this, columns)

    fun mean(skipNa: Boolean = true): DataFrame<T> = mean(skipNa, numberColumns())

    // endregion

    fun <R : Number> std(columns: ColumnsSelector<T, R?>) = Aggregators.std.aggregateAll(this, columns)
}

// region inlines

inline fun <T, reified R : Number> PivotAggregations<T>.meanOf(
    skipNa: Boolean = true,
    crossinline expression: RowSelector<T, R>
): DataFrame<T> = Aggregators.mean(skipNa).cast<Double>().of(this, expression)

inline fun <T, reified V> PivotAggregations<T>.with(noinline selector: RowSelector<T, V>): DataFrame<T> {
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

inline fun <T, reified R : Number> PivotAggregations<T>.sumOf(crossinline selector: RowSelector<T, R>) =
    Aggregators.sum.of(this, selector)


// endregion