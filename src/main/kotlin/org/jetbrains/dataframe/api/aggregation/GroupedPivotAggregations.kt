package org.jetbrains.dataframe

import org.jetbrains.dataframe.aggregation.Aggregatable
import org.jetbrains.dataframe.aggregation.AggregateColumnsSelector
import org.jetbrains.dataframe.aggregation.PivotAggregateBody
import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.impl.aggregation.*
import org.jetbrains.dataframe.impl.aggregation.aggregators.Aggregators
import org.jetbrains.dataframe.impl.aggregation.modes.aggregateAll
import org.jetbrains.dataframe.impl.aggregation.modes.aggregateFor
import org.jetbrains.dataframe.impl.aggregation.modes.of
import org.jetbrains.dataframe.impl.columns.toColumns
import kotlin.reflect.KProperty

public interface GroupedPivotAggregations<out T> : Aggregatable<T> {

    public fun <R> aggregate(body: PivotAggregateBody<T, R>): DataFrame<T>

    public fun frames(): DataFrame<T> = aggregate { this }

    public fun count(predicate: RowFilter<T>? = null): DataFrame<T> = aggregate { count(predicate) default 0 }

    public fun values(vararg columns: Column, separate: Boolean = false): DataFrame<T> = values(separate) { columns.toColumns() }
    public fun values(vararg columns: String, separate: Boolean = false): DataFrame<T> = values(separate) { columns.toColumns() }
    public fun values(vararg columns: KProperty<*>, separate: Boolean = false): DataFrame<T> = values(separate) { columns.toColumns() }
    public fun values(separate: Boolean = false, columns: AggregateColumnsSelector<T, *>): DataFrame<T> =
        groupByValue(separate).aggregateInternal { columnValues(columns) { it.toList() } }

    public fun values(separate: Boolean = false): DataFrame<T> = values(separate, remainingColumnsSelector())

    public fun <R> matches(yes: R, no: R): DataFrame<T> = aggregate { yes default no }
    public fun matches(): DataFrame<T> = matches(yes = true, no = false)

    public fun groupByValue(flag: Boolean = true): GroupedPivotAggregations<T>
    public fun default(value: Any?): GroupedPivotAggregations<T>
    public fun withGrouping(groupPath: ColumnPath): GroupedPivotAggregations<T>

    // region min

    public fun min(separate: Boolean = false): DataFrame<T> = minFor(separate, comparableColumns())

    public fun <R : Comparable<R>> minFor(separate: Boolean = false, columns: AggregateColumnsSelector<T, R?>): DataFrame<T> =
        Aggregators.min.aggregateFor(groupByValue(separate), columns)

    public fun <R : Comparable<R>> min(columns: ColumnsSelector<T, R?>): DataFrame<T> = Aggregators.min.aggregateAll(this, columns)

    public fun <R : Comparable<R>> minOf(rowExpression: RowSelector<T, R>): DataFrame<T> = aggregate { minOf(rowExpression) }

    public fun <R : Comparable<R>> minBy(column: ColumnSelector<T, R>): DataFrame<T> = aggregate { minBy(column) }

    public fun <R : Comparable<R>> minByExpr(rowExpression: RowSelector<T, R>): DataFrame<T> = aggregate { minByExpr(rowExpression) }

    // endregion

    // region max

    public fun max(separate: Boolean = false): DataFrame<T> = maxFor(separate, comparableColumns())

    public fun <R : Comparable<R>> maxFor(separate: Boolean = false, columns: AggregateColumnsSelector<T, R?>): DataFrame<T> =
        Aggregators.max.aggregateFor(groupByValue(separate), columns)

    public fun <R : Comparable<R>> max(columns: ColumnsSelector<T, R?>): DataFrame<T> = Aggregators.max.aggregateAll(this, columns)

    public fun <R : Comparable<R>> maxOf(rowExpression: RowSelector<T, R>): DataFrame<T> = aggregate { maxOf(rowExpression) }

    public fun <R : Comparable<R>> maxBy(column: ColumnSelector<T, R>): DataFrame<T> = aggregate { maxBy(column) }

    public fun <R : Comparable<R>> maxByExpr(rowExpression: RowSelector<T, R>): DataFrame<T> = aggregate { maxByExpr(rowExpression) }

    // endregion

    // region sum

    public fun sum(separate: Boolean = false): DataFrame<T> = sumFor(separate, numberColumns())

    public fun <R : Number> sumFor(separate: Boolean = false, columns: AggregateColumnsSelector<T, R>): DataFrame<T> =
        Aggregators.sum.aggregateFor(groupByValue(separate), columns)

    // endregion

    // region mean

    public fun <R : Number> mean(skipNa: Boolean = true, columns: ColumnsSelector<T, R?>): DataFrame<T> =
        Aggregators.mean(skipNa).aggregateFor(this, columns)

    public fun mean(skipNa: Boolean = true): DataFrame<T> = mean(skipNa, numberColumns())

    // endregion

    // region std

    public fun std(separate: Boolean = false): DataFrame<T> = stdFor(separate, numberColumns())

    public fun <R : Number> stdFor(separate: Boolean = false, columns: AggregateColumnsSelector<T, R>): DataFrame<T> =
        Aggregators.std.aggregateFor(groupByValue(separate), columns)

    public fun <R : Number> std(columns: ColumnsSelector<T, R?>): DataFrame<T> = Aggregators.std.aggregateAll(this, columns)

    // ednregion
}

// region inlines

public inline fun <T, reified R : Number> GroupedPivotAggregations<T>.meanOf(
    skipNa: Boolean = true,
    crossinline expression: RowSelector<T, R>
): DataFrame<T> = Aggregators.mean(skipNa).cast<Double>().of(this, expression)

public inline fun <T, reified V> GroupedPivotAggregations<T>.with(noinline selector: RowSelector<T, V>): DataFrame<T> {
    val type = getType<V>()
    return aggregateInternal {
        val values = df.map {
            val value = selector(it, it)
            if (value is ColumnReference<*>) it[value]
            else value
        }
        yieldOneOrMany(emptyList(), values, type)
    }
}

public inline fun <T, reified R : Number> GroupedPivotAggregations<T>.sumOf(crossinline selector: RowSelector<T, R>): DataFrame<T> =
    Aggregators.sum.of(this, selector)

// endregion
