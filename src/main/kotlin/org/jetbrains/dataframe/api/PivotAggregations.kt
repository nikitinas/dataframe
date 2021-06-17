package org.jetbrains.dataframe.api

import org.jetbrains.dataframe.AggregateBody
import org.jetbrains.dataframe.Column
import org.jetbrains.dataframe.ColumnPath
import org.jetbrains.dataframe.ColumnSelector
import org.jetbrains.dataframe.ColumnsSelector
import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.PivotAggregateBody
import org.jetbrains.dataframe.RowFilter
import org.jetbrains.dataframe.RowSelector
import org.jetbrains.dataframe.getType
import org.jetbrains.dataframe.impl.aggregation.aggregators.Aggregators
import org.jetbrains.dataframe.impl.aggregation.comparableColumns
import org.jetbrains.dataframe.impl.aggregation.modes.aggregateAll
import org.jetbrains.dataframe.impl.aggregation.modes.aggregateFor
import org.jetbrains.dataframe.impl.aggregation.modes.of
import org.jetbrains.dataframe.impl.aggregation.numberColumns
import org.jetbrains.dataframe.impl.aggregation.yieldOneOrMany
import org.jetbrains.dataframe.map
import kotlin.reflect.KProperty

interface PivotAggregations<T> : PivotOrGroupByAggregations<T> {

    fun count(predicate: RowFilter<T>? = null) = aggregateBase { count(predicate) default 0 }

    fun <R> matches(yes: R, no: R) = aggregate { yes default no }
    fun matches() = matches(true, false)

    override fun <R> aggregateBase(body: AggregateBody<T, R>) = aggregate(body as PivotAggregateBody<T, R>)

    fun <R> aggregate(body: PivotAggregateBody<T, R>): DataFrame<T>

    fun groupByValue(flag: Boolean = true): PivotAggregations<T>
    fun withDefault(value: Any?): PivotAggregations<T>
    fun withGrouping(groupPath: ColumnPath): PivotAggregations<T>

    fun into(column: Column) = value(column)
    fun into(column: String) = value(column)
    fun into(column: KProperty<*>) = value(column.name)

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

inline fun <T, reified C> PivotAggregations<T>.valueOf(crossinline expression: RowSelector<T, C>): DataFrame<T> {
    val type = getType<C>()
    return aggregate {
        yieldOneOrMany(map(expression), type)
    }
}

inline fun <T, reified R : Number> PivotAggregations<T>.sumOf(crossinline selector: RowSelector<T, R>) =
    Aggregators.sum.of(this, selector)


// endregion