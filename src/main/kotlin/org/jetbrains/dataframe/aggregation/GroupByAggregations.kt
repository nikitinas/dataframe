package org.jetbrains.dataframe.aggregation

import org.jetbrains.dataframe.ColumnSelector
import org.jetbrains.dataframe.ColumnsSelector
import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.DataRow
import org.jetbrains.dataframe.RowFilter
import org.jetbrains.dataframe.RowSelector
import org.jetbrains.dataframe.aggregation.receivers.AggregateColumnsSelector
import org.jetbrains.dataframe.impl.aggregation.AggregateColumnDescriptor
import org.jetbrains.dataframe.impl.aggregation.aggregators.Aggregators
import org.jetbrains.dataframe.impl.aggregation.comparableColumns
import org.jetbrains.dataframe.impl.aggregation.modes.aggregateValue
import org.jetbrains.dataframe.impl.aggregation.modes.aggregateAll
import org.jetbrains.dataframe.impl.aggregation.modes.aggregateFor
import org.jetbrains.dataframe.impl.columns.toColumnsOf
import org.jetbrains.dataframe.impl.columns.toComparableColumns

interface GroupByAggregations<out T> : Aggregatable<T>, PivotOrGroupByAggregations<T> {

    fun count(resultName: String = "count", predicate: RowFilter<T>? = null) =
        aggregateValue(resultName) { count(predicate) default 0 }

    // region min

    fun min(): DataFrame<T> = minFor(comparableColumns())

    fun <R : Comparable<R>> min(resultName: String? = null, columns: ColumnsSelector<T, R?>): DataFrame<T> =
        Aggregators.min.aggregateAll(resultName, this, columns)
    fun min(vararg columns: String, resultName: String? = null) = min(resultName) { columns.toComparableColumns() }

    fun <R : Comparable<R>> minFor(columns: AggregateColumnsSelector<T, R?>): DataFrame<T> = Aggregators.min.aggregateFor(this, columns)
    fun minFor(vararg columns: String) = minFor { columns.toComparableColumns() }

    fun <C : Comparable<C>> minBy(resultName: String = Aggregators.min.name, selector: ColumnSelector<T, C>): DataFrame<T> =
        aggregateValue(resultName) {
            val col = get(selector)
            rows().minByOrNull { col[it.index()] }
        }

    // endregion

    // region max

    fun max(): DataFrame<T> = maxFor(comparableColumns())

    fun <R : Comparable<R>> maxFor(columns: AggregateColumnsSelector<T, R?>): DataFrame<T> = Aggregators.max.aggregateFor(this, columns)

    fun <R : Comparable<R>> maxOf(resultName: String = "max", selector: RowSelector<T, R>): DataFrame<T> =
        aggregateValue(resultName) { rows().maxByOrNull { selector(it, it) }!! }

    fun <R : Comparable<R>> max(resultName: String? = null, columns: ColumnsSelector<T, R?>) =
        Aggregators.max.aggregateAll(resultName, this, columns)

    // endregion

    // region sum

    fun <R : Number> sum(resultName: String, columns: ColumnsSelector<T, R?>) =
        Aggregators.sum.aggregateAll(resultName, this, columns)

    // endregion

    // region std

    fun <R : Number> std(resultName: String, columns: ColumnsSelector<T, R?>) =
        Aggregators.std.aggregateAll(resultName, this, columns)

    // endregion
}