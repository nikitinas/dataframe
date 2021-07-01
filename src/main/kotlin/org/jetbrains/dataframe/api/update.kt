package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.type
import org.jetbrains.dataframe.impl.columns.toColumnSet
import org.jetbrains.dataframe.impl.columns.toColumns
import org.jetbrains.dataframe.impl.createDataCollector
import kotlin.reflect.KClass
import kotlin.reflect.KProperty
import kotlin.reflect.KType

public fun <T, C> DataFrame<T>.update(selector: ColumnsSelector<T, C>): UpdateClause<T, C> = UpdateClause(this, null, selector, null, false, null)
public fun <T, C> DataFrame<T>.update(cols: Iterable<ColumnReference<C>>): UpdateClause<T, C> = update { cols.toColumnSet() }
public fun <T> DataFrame<T>.update(vararg cols: String): UpdateClause<T, Any?> = update { cols.toColumns() }
public fun <T, C> DataFrame<T>.update(vararg cols: KProperty<C>): UpdateClause<T, C> = update { cols.toColumns() }
public fun <T, C> DataFrame<T>.update(vararg cols: ColumnReference<C>): UpdateClause<T, C> = update { cols.toColumns() }

public data class UpdateClause<T, C>(
    val df: DataFrame<T>,
    val filter: RowCellFilter<T, C>?,
    val selector: ColumnsSelector<T, C>,
    val targetType: KType?,
    val toNull: Boolean = false,
    val typeSuggestions: ((KClass<*>) -> KType)?
) {
    public fun <R> cast(): UpdateClause<T, R> = UpdateClause(df, filter as RowCellFilter<T, R>?, selector as ColumnsSelector<T, R>, targetType, toNull, typeSuggestions)

    public inline fun <reified R> toType(): UpdateClause<T, C> = UpdateClause(df, filter, selector, getType<R>(), toNull, typeSuggestions)
}

public fun <T, C> UpdateClause<T, C>.where(predicate: RowCellFilter<T, C>): UpdateClause<T, C> = copy(filter = predicate)

public fun <T, C> UpdateClause<T, C>.at(rowIndices: Collection<Int>): UpdateClause<T, C> = where { index in rowIndices }
public fun <T, C> UpdateClause<T, C>.at(vararg rowIndices: Int): UpdateClause<T, C> = at(rowIndices.toList())
public fun <T, C> UpdateClause<T, C>.at(rowRange: IntRange): UpdateClause<T, C> = where { index in rowRange }

public fun <T, C> UpdateClause<T, C>.guessTypes(): UpdateClause<T, C> = copy(targetType = null) { it.createStarProjectedType(false) }

public fun <T, C> UpdateClause<T, C>.toType(type: KType): UpdateClause<T, C> = copy(targetType = type)

public fun <T, C> UpdateClause<T, C>.suggestTypes(vararg suggestions: Pair<KClass<*>, KType>): UpdateClause<T, C> {
    val map = suggestions.toMap()
    return copy(targetType = null) { map[it] ?: it.createStarProjectedType(false) }
}

public fun <T, C> doUpdate(clause: UpdateClause<T, C>, expression: (DataRow<T>, DataColumn<C>) -> Any?): DataFrame<T> {
    val removeResult = clause.df.doRemove(clause.selector)

    val nrow = clause.df.nrow()
    val toInsert = removeResult.removedColumns.map {
        val srcColumn = it.data.column as DataColumn<C>
        val collector = when {
            clause.toNull -> createDataCollector(nrow, srcColumn.type)
            clause.typeSuggestions != null -> createDataCollector(nrow, clause.typeSuggestions)
            clause.targetType != null -> createDataCollector(nrow, clause.targetType)
            else -> createDataCollector(nrow, srcColumn.type())
        }
        if (clause.filter == null) {
            clause.df.forEach { row ->
                collector.add(expression(row, srcColumn))
            }
        } else {
            clause.df.forEach { row ->
                val currentValue = srcColumn[row.index]
                val newValue = if (clause.filter.invoke(row, currentValue)) expression(row, srcColumn) else currentValue
                collector.add(newValue)
            }
        }

        val newColumn = collector.toColumn(srcColumn.name())

        ColumnToInsert(it.pathFromRoot(), newColumn, it)
    }
    return removeResult.df.insert(toInsert)
}

// TODO: rename
public inline infix fun <T, C, reified R> UpdateClause<T, C>.with2(noinline expression: RowColumnSelector<T, C, R>): DataFrame<T> = doUpdate(copy(targetType = targetType ?: getType<R>()), expression)

public fun <T, C, R> UpdateClause<T, C>.with(targetType: KType?, expression: RowCellSelector<T, C, R>): DataFrame<T> = doUpdate(copy(filter = null, targetType = targetType)) { row, column ->
    val currentValue = column[row.index]
    if (filter?.invoke(row, currentValue) == false) {
        currentValue as R
    } else expression(row, currentValue)
}

public inline infix fun <T, C, reified R> UpdateClause<T, C>.with(noinline expression: RowCellSelector<T, C, R>): DataFrame<T> = copy(targetType = targetType ?: getType<R>()).withExpression(expression)

public fun <T, C, R> UpdateClause<T, C>.withExpression(expression: RowCellSelector<T, C, R>): DataFrame<T> = doUpdate(copy(filter = null)) { row, column ->
    val currentValue = column[row.index]
    if (filter?.invoke(row, currentValue) == false) {
        currentValue
    } else expression(row, currentValue)
}

internal infix fun <T, C> RowCellFilter<T, C>?.and(other: RowCellFilter<T, C>): RowCellFilter<T, C> {
    if (this == null) return other
    val thisExp = this
    return { thisExp(this, it) && other(this, it) }
}

public fun <T, C> UpdateClause<T, C?>.notNull(): UpdateClause<T, C> = copy(filter = filter and { it != null }) as UpdateClause<T, C>

public inline fun <T, C, reified R> UpdateClause<T, C?>.notNull(noinline expression: RowCellSelector<T, C, R>): DataFrame<T> = doUpdate(copy(filter = null, targetType = targetType ?: getType<R>())) { row, column ->
    val currentValue = column[row.index]
    if (currentValue == null) {
        null
    } else expression(row, currentValue)
}

public inline fun <T, C, reified R> DataFrame<T>.update(
    firstCol: ColumnReference<C>,
    vararg cols: ColumnReference<C>,
    noinline expression: RowCellSelector<T, C, R>
): DataFrame<T> =
    update(*headPlusArray(firstCol, cols)).with(expression)

public inline fun <T, C, reified R> DataFrame<T>.update(
    firstCol: KProperty<C>,
    vararg cols: KProperty<C>,
    noinline expression: RowCellSelector<T, C, R>
): DataFrame<T> =
    update(*headPlusArray(firstCol, cols)).with(expression)

public inline fun <T, reified R> DataFrame<T>.update(
    firstCol: String,
    vararg cols: String,
    noinline expression: RowCellSelector<T, Any?, R>
): DataFrame<T> =
    update(*headPlusArray(firstCol, cols)).with(expression)

public fun <T, C> UpdateClause<T, C>.withNull(): DataFrame<T> = doUpdate(copy(filter = null, targetType = null, typeSuggestions = null, toNull = true)) { row, column ->
    if (filter != null) {
        val currentValue = column[row.index]
        if (!filter.invoke(row, currentValue)) {
            currentValue
        } else null
    } else null
}
public inline infix fun <T, C, reified R> UpdateClause<T, C>.with(value: R): DataFrame<T> = with { value }
