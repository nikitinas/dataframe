package org.jetbrains.dataframe.api

import org.jetbrains.dataframe.ColumnSelector
import org.jetbrains.dataframe.ColumnsSelector
import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.DataFrameBase
import org.jetbrains.dataframe.DataRow
import org.jetbrains.dataframe.RowFilter
import org.jetbrains.dataframe.RowSelector
import org.jetbrains.dataframe.aggregation.Aggregatable
import org.jetbrains.dataframe.aggregation.AggregateColumnsSelector
import org.jetbrains.dataframe.asSequence
import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.impl.aggregation.aggregators.Aggregators
import org.jetbrains.dataframe.impl.aggregation.comparableColumns
import org.jetbrains.dataframe.impl.aggregation.modes.aggregateAll
import org.jetbrains.dataframe.impl.aggregation.modes.aggregateFor
import org.jetbrains.dataframe.impl.aggregation.modes.of
import org.jetbrains.dataframe.impl.aggregation.numberColumns
import org.jetbrains.dataframe.impl.aggregation.remainingColumns
import org.jetbrains.dataframe.impl.columns.toColumns
import org.jetbrains.dataframe.impl.columns.toColumnsOf
import org.jetbrains.dataframe.impl.columns.toComparableColumn
import org.jetbrains.dataframe.impl.columns.toComparableColumns
import org.jetbrains.dataframe.impl.columns.toNumberColumns
import org.jetbrains.dataframe.impl.mapRows
import org.jetbrains.dataframe.impl.zero
import org.jetbrains.dataframe.indexOfMax
import org.jetbrains.dataframe.indexOfMin
import org.jetbrains.dataframe.isNumber
import org.jetbrains.dataframe.sumOf
import org.jetbrains.dataframe.toColumnDef
import kotlin.reflect.KProperty

interface DataFrameAggregations<out T> : Aggregatable<T>, DataFrameBase<T> {

    fun count(predicate: RowFilter<T>? = null): Int =
        if (predicate == null) nrow() else rows().count { predicate(it, it) }

    // region min

    fun min() = minFor(comparableColumns())

    fun <C : Comparable<C>> minFor(columns: AggregateColumnsSelector<T, C?>): DataRow<T> = Aggregators.min.aggregateFor(this, columns)
    fun minFor(vararg columns: String) = minFor { columns.toComparableColumns() }

    fun <C : Comparable<C>> min(columns: ColumnsSelector<T, C?>): C = minOrNull(columns)!!
    fun min(vararg columns: String) = minOrNull(*columns)!!
    fun <C : Comparable<C>> min(vararg columns: ColumnReference<C?>) = minOrNull(*columns)!!
    fun <C : Comparable<C>> min(vararg columns: KProperty<C?>) = minOrNull(*columns)!!

    fun <C : Comparable<C>> minOrNull(columns: ColumnsSelector<T, C?>): C? = Aggregators.min.aggregateAll(this, columns)
    fun minOrNull(vararg columns: String) = minOrNull { columns.toComparableColumns() }
    fun <C : Comparable<C>> minOrNull(vararg columns: ColumnReference<C?>) = minOrNull { columns.toColumns() }
    fun <C : Comparable<C>> minOrNull(vararg columns: KProperty<C?>) = minOrNull { columns.toColumns() }

    fun <C : Comparable<C>> minOf(selector: RowSelector<T, C>): C = minOfOrNull(selector)!!

    fun <C : Comparable<C>> minOfOrNull(selector: RowSelector<T, C>): C? = rows().minOfOrNull { selector(it, it) }

    fun <C : Comparable<C>> minBy(selector: ColumnSelector<T, C?>): DataRow<T> = minByOrNull(selector)!!
    fun minBy(column: String) = minByOrNull(column)!!
    fun <C: Comparable<C>> minBy(column: ColumnReference<C?>) = minByOrNull(column)!!
    fun <C: Comparable<C>> minBy(column: KProperty<C?>) = minByOrNull(column)!!

    fun <C : Comparable<C>> minByOrNull(selector: ColumnSelector<T, C?>): DataRow<T>? = getOrNull(get(selector).asSequence().indexOfMin())
    fun minByOrNull(column: String) = minByOrNull { column.toComparableColumn() }
    fun <C: Comparable<C>> minByOrNull(column: ColumnReference<C?>) = minByOrNull { column }
    fun <C: Comparable<C>> minByOrNull(column: KProperty<C?>) = minByOrNull { column.toColumnDef() }

    fun <C : Comparable<C>> minByExpr(selector: RowSelector<T, C?>): DataRow<T> = minByExprOrNull(selector)!!

    fun <C : Comparable<C>> minByExprOrNull(selector: RowSelector<T, C?>): DataRow<T>? = getOrNull(mapRows(selector).indexOfMin())

    // endregion

    // region max

    fun max() = maxFor(comparableColumns())

    fun <R : Comparable<R>> maxFor(columns: AggregateColumnsSelector<T, R?>): DataRow<T> = Aggregators.max.aggregateFor(this, columns)

    fun <R : Comparable<R>> max(columns: ColumnsSelector<T, R?>): R = maxOrNull(columns)!!
    fun max(vararg columns: String) = maxOrNull(*columns)!!
    fun <C : Comparable<C>> max(vararg columns: ColumnReference<C?>) = maxOrNull(*columns)!!
    fun <C : Comparable<C>> max(vararg columns: KProperty<C?>) = maxOrNull(*columns)!!

    fun <R : Comparable<R>> maxOrNull(columns: ColumnsSelector<T, R?>): R? = Aggregators.max.aggregateAll(this, columns)
    fun maxOrNull(vararg columns: String) = maxOrNull { columns.toComparableColumns() }
    fun <C : Comparable<C>> maxOrNull(vararg columns: ColumnReference<C?>) = maxOrNull { columns.toColumns() }
    fun <C : Comparable<C>> maxOrNull(vararg columns: KProperty<C?>) = maxOrNull { columns.toColumns() }

    fun <R : Comparable<R>> maxOf(selector: RowSelector<T, R>): R = maxOfOrNull(selector)!!

    fun <R : Comparable<R>> maxOfOrNull(selector: RowSelector<T, R>): R? = rows().maxOfOrNull { selector(it, it) }

    fun <C : Comparable<C>> maxBy(selector: ColumnSelector<T, C?>): DataRow<T> = maxByOrNull(selector)!!
    fun maxBy(column: String) = maxByOrNull(column)!!
    fun <C: Comparable<C>> maxBy(column: ColumnReference<C?>) = maxByOrNull(column)!!
    fun <C: Comparable<C>> maxBy(column: KProperty<C?>) = maxByOrNull(column)!!

    fun <C : Comparable<C>> maxByOrNull(selector: ColumnSelector<T, C?>): DataRow<T>? = getOrNull(get(selector).asSequence().indexOfMax())
    fun maxByOrNull(column: String) = maxByOrNull { column.toComparableColumn() }
    fun <C: Comparable<C>> maxByOrNull(column: ColumnReference<C?>) = maxByOrNull { column }
    fun <C: Comparable<C>> maxByOrNull(column: KProperty<C?>) = maxByOrNull { column.toColumnDef() }

    fun <C : Comparable<C>> maxByExpr(selector: RowSelector<T, C>): DataRow<T> = maxByExprOrNull(selector)!!
    fun <C : Comparable<C>> maxByExprOrNull(selector: RowSelector<T, C>): DataRow<T>? = getOrNull(mapRows(selector).indexOfMax())

    // endregion

    // region sum

    fun sum() = sumFor(numberColumns())

    fun <R : Number> sumFor(columns: ColumnsSelector<T, R>): DataRow<T> = Aggregators.sum.aggregateFor(this, columns)
    fun sumFor(vararg columns: String) = sumFor { columns.toColumnsOf() }

    fun sum(vararg columns: String): Number = sum { columns.toColumnsOf() }

    // endregion

    // region std

    fun <R : Number> stdFor(columns: AggregateColumnsSelector<T, R>): DataRow<T> = Aggregators.std.aggregateFor(this, columns)
    fun std() = stdFor(remainingColumns { it.isNumber() } as ColumnsSelector<T, Number>)

    // endregion

    // region mean

    fun mean(skipNa: Boolean = false) = meanFor(skipNa, numberColumns())

    fun <C : Number> meanFor(skipNa: Boolean = false, columns: AggregateColumnsSelector<T, C?>): DataRow<T> = Aggregators.mean(skipNa).aggregateFor(this, columns)
    fun meanFor(vararg columns: String) = meanFor { columns.toNumberColumns() }

    fun <C : Number> mean(skipNa: Boolean = false, columns: ColumnsSelector<T, C?>): Double = Aggregators.mean(skipNa).aggregateAll(this, columns) as Double? ?: Double.NaN
    fun mean(vararg columns: String, skipNa: Boolean = false) = mean(skipNa) { columns.toNumberColumns() }
    fun <C : Number> mean(vararg columns: ColumnReference<C?>, skipNa: Boolean = false) = mean(skipNa) { columns.toColumns() }
    fun <C : Number> mean(vararg columns: KProperty<C?>, skipNa: Boolean = false) = mean(skipNa) { columns.toColumns() }

    // endregion
}

// region inlines

inline fun <T, reified D : Number> DataFrame<T>.meanOf(skipNa: Boolean = false, noinline selector: RowSelector<T, D?>): Double {
    return Aggregators.mean(skipNa).of(this, selector) ?: Double.NaN
}

inline fun <T, reified R: Number> DataFrameAggregations<T>.sum(noinline columns: ColumnsSelector<T, R>): R =
    Aggregators.sum.aggregateAll(this, columns) ?: R::class.zero()

inline fun <T, reified R : Number> DataFrameAggregations<T>.sumOf(crossinline selector: RowSelector<T, R>): R =
    rows().sumOf(R::class) { selector(it, it) }

// endregion