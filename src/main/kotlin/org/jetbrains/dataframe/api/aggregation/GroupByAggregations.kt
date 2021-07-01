package org.jetbrains.dataframe

import org.jetbrains.dataframe.aggregation.Aggregatable
import org.jetbrains.dataframe.aggregation.AggregateColumnsSelector
import org.jetbrains.dataframe.aggregation.GroupByAggregateBody
import org.jetbrains.dataframe.impl.aggregation.*
import org.jetbrains.dataframe.impl.aggregation.aggregators.Aggregators
import org.jetbrains.dataframe.impl.aggregation.modes.aggregateAll
import org.jetbrains.dataframe.impl.aggregation.modes.aggregateBy
import org.jetbrains.dataframe.impl.aggregation.modes.aggregateFor
import org.jetbrains.dataframe.impl.aggregation.modes.aggregateValue
import org.jetbrains.dataframe.impl.aggregation.modes.of
import org.jetbrains.dataframe.impl.columns.toColumns
import org.jetbrains.dataframe.impl.columns.toComparableColumns

public interface GroupByAggregations<out T> : Aggregatable<T> {

    public fun aggregate(body: GroupByAggregateBody<T>): DataFrame<T>

    public fun pivot(columns: ColumnsSelector<T, *>): GroupedPivotAggregations<T>
    public fun pivot(vararg columns: Column): GroupedPivotAggregations<T> = pivot { columns.toColumns() }
    public fun pivot(vararg columns: String): GroupedPivotAggregations<T> = pivot { columns.toColumns() }

    public fun count(resultName: String = "count", predicate: RowFilter<T>? = null): DataFrame<T> =
        aggregateValue(resultName) { count(predicate) default 0 }

    public fun values(vararg columns: Column): DataFrame<T> = values { columns.toColumns() }
    public fun values(vararg columns: String): DataFrame<T> = values { columns.toColumns() }
    public fun values(columns: AggregateColumnsSelector<T, *>): DataFrame<T> = aggregateInternal { columnValues(columns) { it.toList() } }

    public fun values(): DataFrame<T> = values(remainingColumnsSelector())

    // region min

    public fun min(): DataFrame<T> = minFor(comparableColumns())

    public fun <R : Comparable<R>> minFor(columns: AggregateColumnsSelector<T, R?>): DataFrame<T> = Aggregators.min.aggregateFor(this, columns)
    public fun minFor(vararg columns: String): DataFrame<T> = minFor { columns.toComparableColumns() }

    public fun <R : Comparable<R>> min(resultName: String? = null, columns: ColumnsSelector<T, R?>): DataFrame<T> =
        Aggregators.min.aggregateAll(resultName, this, columns)
    public fun min(vararg columns: String, resultName: String? = null): DataFrame<T> = min(resultName) { columns.toComparableColumns() }

    public fun <C : Comparable<C>> minOf(resultName: String = Aggregators.min.name, selector: RowSelector<T, C>): DataFrame<T> =
        aggregateValue(resultName) { minOfOrNull(selector) }

    public fun <C : Comparable<C>> minBy(selector: ColumnSelector<T, C>): DataFrame<T> =
        aggregateBy { minByOrNull(selector) }

    public fun <C : Comparable<C>> minByExpr(selector: RowSelector<T, C>): DataFrame<T> =
        aggregateBy { minByExprOrNull(selector) }

    // endregion

    // region max

    public fun max(): DataFrame<T> = maxFor(comparableColumns())

    public fun <R : Comparable<R>> maxFor(columns: AggregateColumnsSelector<T, R?>): DataFrame<T> = Aggregators.max.aggregateFor(this, columns)

    public fun <R : Comparable<R>> max(resultName: String? = null, columns: ColumnsSelector<T, R?>): DataFrame<T> =
        Aggregators.max.aggregateAll(resultName, this, columns)

    public fun <R : Comparable<R>> maxOf(resultName: String = Aggregators.max.name, selector: RowSelector<T, R>): DataFrame<T> =
        aggregateValue(resultName) { maxOfOrNull(selector) }

    public fun <C : Comparable<C>> maxBy(resultName: String = Aggregators.min.name, selector: ColumnSelector<T, C>): DataFrame<T> =
        aggregateBy { maxByOrNull(selector) }

    public fun <C : Comparable<C>> maxByExpr(selector: RowSelector<T, C>): DataFrame<T> =
        aggregateBy { minByExprOrNull(selector) }

    // endregion

    // region sum

    public fun sum(): DataFrame<T> = sumFor(numberColumns())

    public fun <R : Number> sumFor(columns: AggregateColumnsSelector<T, R>): DataFrame<T> = Aggregators.sum.aggregateFor(this, columns)

    public fun <R : Number> sum(resultName: String, columns: ColumnsSelector<T, R?>): DataFrame<T> =
        Aggregators.sum.aggregateAll(resultName, this, columns)

    // endregion

    // region mean

    public fun mean(skipNa: Boolean = false): DataFrame<T> = meanFor(skipNa, numberColumns())

    public fun <R : Number> meanFor(skipNa: Boolean = false, columns: AggregateColumnsSelector<T, R?>): DataFrame<T> = Aggregators.mean(skipNa).aggregateFor(this, columns)

    public fun <R : Number> mean(resultName: String? = null, skipNa: Boolean = false, columns: ColumnsSelector<T, R?>): DataFrame<T> =
        Aggregators.mean(skipNa).aggregateAll(resultName, this, columns)

    // endregion

    // region std

    public fun <R : Number> stdFor(columns: AggregateColumnsSelector<T, R>): DataFrame<T> = Aggregators.std.aggregateFor(this, columns)
    public fun std(): DataFrame<T> = stdFor(numberColumns())

    public fun <R : Number> std(resultName: String, columns: ColumnsSelector<T, R?>): DataFrame<T> =
        Aggregators.std.aggregateAll(resultName, this, columns)

    // endregion
}

// region inlines

public inline fun <T, reified R : Number> GroupByAggregations<T>.meanOf(
    resultName: String = Aggregators.mean.name,
    skipNa: Boolean = false,
    crossinline selector: RowSelector<T, R>
): DataFrame<T> =
    Aggregators.mean(skipNa).of(resultName, this, selector)

public inline fun <T, reified C> GroupByAggregations<T>.with(
    name: String = "value",
    crossinline expression: RowSelector<T, C>
): DataFrame<T> {
    val type = getType<C>()
    val path = listOf(name)
    return aggregateInternal {
        yieldOneOrMany(path, df.map(expression), type)
    }
}

public inline fun <T, reified R : Number> GroupByAggregations<T>.sumOf(
    resultName: String? = null,
    crossinline selector: RowSelector<T, R>
): DataFrame<T> = Aggregators.sum.of(resultName, this, selector)

// endregion
