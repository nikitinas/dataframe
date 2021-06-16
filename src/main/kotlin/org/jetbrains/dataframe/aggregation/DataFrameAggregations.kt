package org.jetbrains.dataframe.aggregation

import org.jetbrains.dataframe.Column
import org.jetbrains.dataframe.ColumnSelector
import org.jetbrains.dataframe.ColumnsSelector
import org.jetbrains.dataframe.DataFrameBase
import org.jetbrains.dataframe.DataRow
import org.jetbrains.dataframe.RowFilter
import org.jetbrains.dataframe.RowSelector
import org.jetbrains.dataframe.aggregation.receivers.AggregateColumnsSelector
import org.jetbrains.dataframe.asSequence
import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.impl.aggregation.aggregators.Aggregators
import org.jetbrains.dataframe.impl.aggregation.comparableColumns
import org.jetbrains.dataframe.impl.aggregation.modes.aggregateAll
import org.jetbrains.dataframe.impl.aggregation.modes.aggregateFor
import org.jetbrains.dataframe.impl.aggregation.remainingColumns
import org.jetbrains.dataframe.impl.columns.toColumnsOf
import org.jetbrains.dataframe.indexOfMin
import org.jetbrains.dataframe.isNumber
import org.jetbrains.dataframe.sum
import org.jetbrains.dataframe.toColumnDef
import org.jetbrains.dataframe.toColumnOf
import kotlin.reflect.KProperty

interface DataFrameAggregations<out T> : Aggregatable<T>, DataFrameBase<T> {

    fun count(predicate: RowFilter<T>? = null): Int =
        if (predicate == null) nrow() else rows().count { predicate(it, it) }

    // region min

    fun min() = minFor(comparableColumns())

    fun <C : Comparable<C>> minFor(columns: AggregateColumnsSelector<T, C?>): DataRow<T> = Aggregators.min.aggregateFor(this, columns)
    fun minFor(vararg columns: String) = minFor { columns.toColumnsOf<Comparable<Any?>>() }

    fun <C : Comparable<C>> min(columns: ColumnsSelector<T, C?>): C = minOrNull(columns)!!
    fun min(vararg columns: String) = minOrNull(*columns)!!

    fun <C : Comparable<C>> minOrNull(columns: ColumnsSelector<T, C?>): C? = Aggregators.min.aggregateAll(this, columns)
    fun minOrNull(vararg columns: String) = minOrNull { columns.toColumnsOf<Comparable<Any?>>() }

    fun <C : Comparable<C>> minOf(selector: RowSelector<T, C>): C = minOfOrNull(selector)!!

    fun <C : Comparable<C>> minOfOrNull(selector: RowSelector<T, C>): C? = rows().minOfOrNull { selector(it, it) }

    fun <C : Comparable<C>> minByOrNull(selector: ColumnSelector<T, C?>): DataRow<T>? {
        val col = get(selector)
        val index = col.asSequence().indexOfMin()
        if(index == -1) return null
        return get(index)
    }

    fun minByOrNull(column: String) = minByOrNull { column.toColumnOf<Comparable<Any?>>() }
    fun <C: Comparable<C>> minByOrNull(column: ColumnReference<C?>) = minByOrNull { column }
    fun <C: Comparable<C>> minByOrNull(column: KProperty<C?>) = minByOrNull { column.toColumnDef() }

    fun <C : Comparable<C>> minBy(selector: ColumnSelector<T, C?>): DataRow<T> = minByOrNull(selector)!!
    fun minBy(column: String) = minByOrNull(column)!!
    fun <C: Comparable<C>> minBy(column: ColumnReference<C?>) = minByOrNull(column)!!
    fun <C: Comparable<C>> minBy(column: KProperty<C?>) = minByOrNull(column)!!

    fun <C : Comparable<C>> minByExprOrNull(selector: RowSelector<T, C>): DataRow<T>? = rows().minByOrNull { selector(it, it) }
    fun <C : Comparable<C>> minByExpr(selector: RowSelector<T, C>): DataRow<T> = minByExprOrNull(selector)!!

    // endregion

    // region max

    fun <R : Comparable<R>> maxFor(columns: AggregateColumnsSelector<T, R?>): DataRow<T> = Aggregators.max.aggregateFor(this, columns)
    fun max() = maxFor(comparableColumns())

    fun <R : Comparable<R>> max(columns: ColumnsSelector<T, R?>): R? = Aggregators.max.aggregateAll(this, columns)

    fun <R : Comparable<R>> maxOf(selector: RowSelector<T, R>): R = rows().maxOf { selector(it, it) }

    fun <C : Comparable<C>> maxByOrNull(selector: ColumnSelector<T, C>): DataRow<T>? = get(selector).let { col -> rows().maxByOrNull { col[it.index()] } }
    fun maxByOrNull(column: String) = maxByOrNull { column.toColumnOf<Comparable<Any?>>() }
    fun <C: Comparable<C>> maxByOrNull(column: ColumnReference<C>) = maxByOrNull { column }
    fun <C: Comparable<C>> maxByOrNull(column: KProperty<C>) = maxByOrNull { column.toColumnDef() }

    fun <C : Comparable<C>> maxBy(selector: ColumnSelector<T, C>): DataRow<T> = maxByOrNull(selector)!!
    fun maxBy(column: String) = maxByOrNull(column)!!
    fun <C: Comparable<C>> maxBy(column: ColumnReference<C>) = maxByOrNull(column)!!
    fun <C: Comparable<C>> maxBy(column: KProperty<C>) = maxByOrNull(column)!!

    fun <C : Comparable<C>> maxByExprOrNull(selector: RowSelector<T, C>): DataRow<T>? = rows().maxByOrNull { selector(it, it) }
    fun <C : Comparable<C>> maxByExpr(selector: RowSelector<T, C>): DataRow<T> = maxByExprOrNull(selector)!!

    // endregion

    // region sum

    fun sum() = sumFor(remainingColumns { it.isNumber() } as ColumnsSelector<T, Number>)

    fun <R : Number> sumFor(columns: ColumnsSelector<T, R>): DataRow<T> = Aggregators.sum.aggregateFor(this, columns)
    fun sumFor(vararg columns: String) = sumFor { columns.toColumnsOf() }

    fun sum(vararg columns: String): Number = sum { columns.toColumnsOf() }

    // endregion

    // region std

    fun <R : Number> stdFor(columns: AggregateColumnsSelector<T, R>): DataRow<T> = Aggregators.std.aggregateFor(this, columns)
    fun std() = stdFor(remainingColumns { it.isNumber() } as ColumnsSelector<T, Number>)

    // endregion

    // region mean

    fun <R : Number> meanBy(skipNa: Boolean = true, columns: ColumnsSelector<T, R?>): DataRow<T> =
        Aggregators.mean(skipNa).aggregateFor(this, columns)

    fun <R : Number> mean(skipNa: Boolean = true, columns: ColumnsSelector<T, R?>): Double =
        Aggregators.mean(skipNa).aggregateAll(this, columns) as? Double ?: Double.NaN

    fun mean(skipNa: Boolean = true): DataRow<T> =
        meanBy(skipNa, remainingColumns { it.isNumber() } as ColumnsSelector<T, Number>)

    // endregion
}