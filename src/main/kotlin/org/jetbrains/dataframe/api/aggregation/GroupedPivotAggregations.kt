package org.jetbrains.dataframe

import org.jetbrains.dataframe.aggregation.Aggregatable
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

interface GroupedPivotAggregations<T> : Aggregatable<T> {

    fun count(predicate: RowFilter<T>? = null) = aggregateBase { count(predicate) default 0 }

    fun values(vararg columns: Column, separate: Boolean = false) = values(separate) { columns.toColumns() }
    fun values(vararg columns: String, separate: Boolean = false) = values(separate) { columns.toColumns() }
    fun values(vararg columns: KProperty<*>, separate: Boolean = false) = values(separate) { columns.toColumns() }
    fun values(separate: Boolean = false, columns: AggregateColumnsSelector<T, *>) = groupByValue(separate).yieldOneOrManyBy(columns) { it.toList() }

    fun values(separate: Boolean = false) = values(separate, remainingColumnsSelector())

    fun <R> matches(yes: R, no: R) = aggregate { yes default no }
    fun matches() = matches(true, false)

    // TODO: move to internal
    override fun <R> aggregateBase(body: AggregateBody<T, R>): DataFrame<T> = aggregate(body as PivotAggregateBody<T, R>)

    fun <R> aggregate(body: PivotAggregateBody<T, R>): DataFrame<T>

    fun groupByValue(flag: Boolean = true): GroupedPivotAggregations<T>
    fun default(value: Any?): GroupedPivotAggregations<T>
    fun withGrouping(groupPath: ColumnPath): GroupedPivotAggregations<T>

    // region min

    fun min(separate: Boolean = false) = minFor(separate, comparableColumns())

    fun <R : Comparable<R>> minFor(separate: Boolean = false, columns: ColumnsSelector<T, R?>) = Aggregators.min.aggregateFor(groupByValue(separate), columns)

    fun <R : Comparable<R>> min(columns: ColumnsSelector<T, R?>) = Aggregators.min.aggregateAll(this, columns)

    fun <R : Comparable<R>> minOf(rowExpression: RowSelector<T, R>) = aggregate { minOf(rowExpression) }

    fun <R : Comparable<R>> minBy(column: ColumnSelector<T, R>) = aggregate { minBy(column) }

    fun <R : Comparable<R>> minByExpr(rowExpression: RowSelector<T, R>) = aggregate { minByExpr(rowExpression) }

    // endregion

    // region max

    fun max(separate: Boolean = false) = maxFor(separate, comparableColumns())

    fun <R : Comparable<R>> maxFor(separate: Boolean = false, columns: ColumnsSelector<T, R?>) = Aggregators.max.aggregateFor(groupByValue(separate), columns)

    fun <R : Comparable<R>> max(columns: ColumnsSelector<T, R?>) = Aggregators.max.aggregateAll(this, columns)

    fun <R : Comparable<R>> maxOf(rowExpression: RowSelector<T, R>) = aggregate { maxOf(rowExpression) }

    fun <R : Comparable<R>> maxBy(column: ColumnSelector<T, R>) = aggregate { maxBy(column) }

    fun <R : Comparable<R>> maxByExpr(rowExpression: RowSelector<T, R>) = aggregate { maxByExpr(rowExpression) }

    // endregion

    // region sum

    fun sum(separate: Boolean = false) = sumFor(separate, numberColumns())

    fun <R : Number> sumFor(separate: Boolean = false, columns: ColumnsSelector<T, R>): DataFrame<T> = Aggregators.sum.aggregateFor(groupByValue(separate), columns)

    // endregion

    // region mean

    fun <R : Number> mean(skipNa: Boolean = true, columns: ColumnsSelector<T, R?>): DataFrame<T> = Aggregators.mean(skipNa).aggregateFor(this, columns)

    fun mean(skipNa: Boolean = true): DataFrame<T> = mean(skipNa, numberColumns())

    // endregion

    // region std

    fun std(separate: Boolean = false): DataFrame<T> = stdFor(separate, numberColumns())

    fun <R : Number> stdFor(separate: Boolean = false, columns: AggregateColumnsSelector<T, R>): DataFrame<T> = Aggregators.std.aggregateFor(groupByValue(separate), columns)

    fun <R : Number> std(columns: ColumnsSelector<T, R?>) = Aggregators.std.aggregateAll(this, columns)

    // ednregion
}

// region inlines

inline fun <T, reified R : Number> GroupedPivotAggregations<T>.meanOf(
    skipNa: Boolean = true,
    crossinline expression: RowSelector<T, R>
): DataFrame<T> = Aggregators.mean(skipNa).cast<Double>().of(this, expression)

inline fun <T, reified R : Number> GroupedPivotAggregations<T>.sumOf(crossinline selector: RowSelector<T, R>) =
    Aggregators.sum.of(this, selector)

inline fun <T, reified V> GroupedPivotAggregations<T>.with(noinline selector: RowSelector<T, V>): DataFrame<T> {
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

// endregion