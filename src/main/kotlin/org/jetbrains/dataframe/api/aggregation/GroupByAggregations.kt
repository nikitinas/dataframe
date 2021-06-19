package org.jetbrains.dataframe

import org.jetbrains.dataframe.aggregation.Aggregatable
import org.jetbrains.dataframe.aggregation.AggregateColumnsSelector
import org.jetbrains.dataframe.impl.aggregation.aggregators.Aggregators
import org.jetbrains.dataframe.impl.aggregation.comparableColumns
import org.jetbrains.dataframe.impl.aggregation.modes.aggregateValue
import org.jetbrains.dataframe.impl.aggregation.modes.aggregateAll
import org.jetbrains.dataframe.impl.aggregation.modes.aggregateBy
import org.jetbrains.dataframe.impl.aggregation.modes.aggregateFor
import org.jetbrains.dataframe.impl.aggregation.modes.of
import org.jetbrains.dataframe.impl.aggregation.numberColumns
import org.jetbrains.dataframe.impl.aggregation.yieldOneOrMany
import org.jetbrains.dataframe.impl.aggregation.yieldOneOrManyBy
import org.jetbrains.dataframe.impl.columns.toColumns
import org.jetbrains.dataframe.impl.columns.toComparableColumns

interface GroupByAggregations<out T> : Aggregatable<T>, PivotOrGroupByAggregations<T> {

    fun count(resultName: String = "count", predicate: RowFilter<T>? = null) =
        aggregateValue(resultName) { count(predicate) default 0 }

    fun values(vararg columns: Column): DataFrame<T> = values { columns.toColumns() }
    fun values(vararg columns: String): DataFrame<T> = values { columns.toColumns() }
    fun values(columns: AggregateColumnsSelector<T, *>): DataFrame<T> = yieldOneOrManyBy(columns) { it.toList() }

    fun values(): DataFrame<T> = values(remainingColumnsSelector())


    // region min

    fun min(): DataFrame<T> = minFor(comparableColumns())

    fun <R : Comparable<R>> minFor(columns: AggregateColumnsSelector<T, R?>): DataFrame<T> = Aggregators.min.aggregateFor(this, columns)
    fun minFor(vararg columns: String) = minFor { columns.toComparableColumns() }

    fun <R : Comparable<R>> min(resultName: String? = null, columns: ColumnsSelector<T, R?>): DataFrame<T> =
        Aggregators.min.aggregateAll(resultName, this, columns)
    fun min(vararg columns: String, resultName: String? = null) = min(resultName) { columns.toComparableColumns() }

    fun <C : Comparable<C>> minOf(resultName: String = Aggregators.min.name, selector: RowSelector<T, C>): DataFrame<T> =
        aggregateValue(resultName) { minOfOrNull(selector) }

    fun <C : Comparable<C>> minBy(selector: ColumnSelector<T, C>): DataFrame<T> =
        aggregateBy { minByOrNull(selector) }

    fun <C : Comparable<C>> minByExpr(selector: RowSelector<T, C>): DataFrame<T> =
        aggregateBy { minByExprOrNull(selector) }

    // endregion

    // region max

    fun max(): DataFrame<T> = maxFor(comparableColumns())

    fun <R : Comparable<R>> maxFor(columns: AggregateColumnsSelector<T, R?>): DataFrame<T> = Aggregators.max.aggregateFor(this, columns)

    fun <R : Comparable<R>> max(resultName: String? = null, columns: ColumnsSelector<T, R?>) =
        Aggregators.max.aggregateAll(resultName, this, columns)

    fun <R : Comparable<R>> maxOf(resultName: String = Aggregators.max.name, selector: RowSelector<T, R>): DataFrame<T> =
        aggregateValue(resultName) { maxOfOrNull(selector) }

    fun <C : Comparable<C>> maxBy(resultName: String = Aggregators.min.name, selector: ColumnSelector<T, C>): DataFrame<T> =
        aggregateBy { maxByOrNull(selector) }

    fun <C : Comparable<C>> maxByExpr(selector: RowSelector<T, C>): DataFrame<T> =
        aggregateBy { minByExprOrNull(selector) }

    // endregion

    // region sum

    fun <R : Number> sum(resultName: String, columns: ColumnsSelector<T, R?>) =
        Aggregators.sum.aggregateAll(resultName, this, columns)

    // endregion

    // region mean

    fun mean(skipNa: Boolean = false): DataFrame<T> = meanFor(skipNa, numberColumns())

    fun <R : Number> meanFor(skipNa: Boolean = false, columns: AggregateColumnsSelector<T, R?>): DataFrame<T> = Aggregators.mean(skipNa).aggregateFor(this, columns)

    fun <R : Number> mean(resultName: String? = null, skipNa: Boolean = false, columns: ColumnsSelector<T, R?>) =
        Aggregators.mean(skipNa).aggregateAll(resultName, this, columns)

    // endregion

    // region std

    fun <R : Number> std(resultName: String, columns: ColumnsSelector<T, R?>) =
        Aggregators.std.aggregateAll(resultName, this, columns)

    // endregion
}

// region inlines

inline fun <T, reified R : Number> GroupByAggregations<T>.meanOf(resultName: String = Aggregators.mean.name, skipNa: Boolean = false, crossinline selector: RowSelector<T, R>): DataFrame<T> =
    Aggregators.mean(skipNa).of(resultName, this, selector)

inline fun <T, reified C> GroupByAggregations<T>.valueOf(
    name: String,
    crossinline expression: RowSelector<T, C>
): DataFrame<T> {
    val type = getType<C>()
    val path = listOf(name)
    return aggregateBase {
        yieldOneOrMany(path, map(expression), type)
    }
}

inline fun <T, reified R : Number> GroupByAggregations<T>.sumOf(
    resultName: String? = null,
    crossinline selector: RowSelector<T, R>
): DataFrame<T> = Aggregators.sum.of(resultName, this, selector)

// endregion