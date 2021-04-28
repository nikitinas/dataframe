package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.impl.columns.toColumnSet
import org.jetbrains.dataframe.impl.columns.toColumns
import org.jetbrains.dataframe.impl.createDataCollector
import kotlin.reflect.KClass
import kotlin.reflect.KProperty
import kotlin.reflect.KType

fun <T, C> DataFrame<T>.update(selector: ColumnsSelector<T, C>) = UpdateClause(this, null, selector, null, false, null)
fun <T, C> DataFrame<T>.update(cols: Iterable<ColumnReference<C>>) = update { cols.toColumnSet() }
fun <T> DataFrame<T>.update(vararg cols: String) = update { cols.toColumns() }
fun <T, C> DataFrame<T>.update(vararg cols: KProperty<C>) = update { cols.toColumns() }
fun <T, C> DataFrame<T>.update(vararg cols: ColumnReference<C>) = update { cols.toColumns() }

data class UpdateClause<T, C>(val df: DataFrame<T>, val filter: RowCellFilter<T, C>?, val selector: ColumnsSelector<T, C>, val targetType: KType?, val toNull: Boolean = false, val typeSuggestions: ((KClass<*>) -> KType)?){
    fun <R> cast() = UpdateClause(df, filter as RowCellFilter<T, R>?, selector as ColumnsSelector<T, R>, targetType, toNull, typeSuggestions)

    inline fun <reified R> toType() = UpdateClause(df, filter, selector, getType<R>(), toNull, typeSuggestions)
}

fun <T, C> UpdateClause<T, C>.where(predicate: RowCellFilter<T, C>) = copy(filter = predicate)

fun <T, C> UpdateClause<T, C>.at(rowIndices: Collection<Int>) = where { index in rowIndices }
fun <T, C> UpdateClause<T, C>.at(vararg rowIndices: Int) = at(rowIndices.toList())
fun <T, C> UpdateClause<T, C>.at(rowRange: IntRange) = where { index in rowRange }

fun <T, C> UpdateClause<T, C>.guessTypes() = copy(targetType = null) { it.createStarProjectedType(false) }

fun <T, C> UpdateClause<T, C>.toType(type: KType) = copy(targetType = type)

fun <T, C> UpdateClause<T, C>.suggestTypes(vararg suggestions: Pair<KClass<*>, KType>): UpdateClause<T, C> {
    val map = suggestions.toMap()
    return copy(targetType = null) { map[it] ?: it.createStarProjectedType(false) }
}

fun <T, C> doUpdate(clause: UpdateClause<T, C>, expression: (DataRow<T>, DataColumn<C>) -> Any?): DataFrame<T> {

    val removeResult = clause.df.doRemove(clause.selector)

    val nrow = clause.df.nrow()
    val toInsert = removeResult.removedColumns.map {
        val srcColumn = it.data.column as DataColumn<C>
        val collector = when {
            clause.toNull -> createDataCollector(nrow, srcColumn.type)
            clause.typeSuggestions != null -> createDataCollector(nrow, clause.typeSuggestions)
            clause.targetType != null -> createDataCollector(nrow, clause.targetType)
            else -> error("Invalid state of UpdateClause: ${clause}")
        }
        if(clause.filter == null)
            clause.df.forEach { row ->
                collector.add(expression(row, srcColumn))
            }
        else
            clause.df.forEach { row ->
                val currentValue = srcColumn[row.index]
                val newValue = if (clause.filter.invoke(row, currentValue)) expression(row, srcColumn) else currentValue
                collector.add(newValue)
            }

        val newColumn = collector.toColumn(srcColumn.name())

        ColumnToInsert(it.pathFromRoot(), newColumn, it)
    }
    return removeResult.df.insert(toInsert)
}

// TODO: rename
inline infix fun <T, C, reified R> UpdateClause<T, C>.with2(noinline expression: RowColumnSelector<T, C, R>) = doUpdate(copy(targetType = targetType ?: getType<R>()), expression)

fun <T, C, R> UpdateClause<T, C>.with(targetType: KType?, expression: RowCellSelector<T, C, R>) = doUpdate(copy(filter = null, targetType = targetType)) { row, column ->
    val currentValue = column[row.index]
    if (filter?.invoke(row, currentValue) == false)
        currentValue as R
    else expression(row, currentValue)
}

inline infix fun <T, C, reified R> UpdateClause<T, C>.with(noinline expression: RowCellSelector<T, C, R>) = doUpdate(copy(filter = null, targetType = targetType ?: getType<R>())) { row, column ->
    val currentValue = column[row.index]
    if (filter?.invoke(row, currentValue) == false)
        currentValue
    else expression(row, currentValue)
}

internal infix fun <T,C> RowCellFilter<T, C>?.and(other: RowCellFilter<T, C>): RowCellFilter<T,C> {
    if(this == null) return other
    val thisExp = this
    return { thisExp(this, it) && other(this, it) }
}

fun <T, C> UpdateClause<T, C?>.notNull(): UpdateClause<T, C> = copy(filter = filter and { it != null } ) as UpdateClause<T, C>

inline fun <T, C, reified R> UpdateClause<T, C?>.notNull(noinline expression: RowCellSelector<T, C, R>) = doUpdate(copy(filter = null, targetType =  targetType ?: getType<R>())) { row, column ->
    val currentValue = column[row.index]
    if (currentValue == null)
        null
    else expression(row, currentValue)
}

inline fun <T, C, reified R> DataFrame<T>.update(firstCol: ColumnReference<C>, vararg cols: ColumnReference<C>, noinline expression: RowCellSelector<T, C, R>) =
        update(*headPlusArray(firstCol, cols)).with(expression)

inline fun <T, C, reified R> DataFrame<T>.update(firstCol: KProperty<C>, vararg cols: KProperty<C>, noinline expression: RowCellSelector<T, C, R>) =
        update(*headPlusArray(firstCol, cols)).with(expression)

inline fun <T, reified R> DataFrame<T>.update(firstCol: String, vararg cols: String, noinline expression: RowCellSelector<T, Any?, R>) =
        update(*headPlusArray(firstCol, cols)).with(expression)

fun <T, C> UpdateClause<T, C>.withNull() = doUpdate(copy(filter = null, targetType = null, typeSuggestions = null, toNull = true)) { row, column ->
    if(filter != null){
        val currentValue = column[row.index]
        if (!filter.invoke(row, currentValue))
            currentValue
        else null
    }
    else null
}
inline infix fun <T, C, reified R> UpdateClause<T, C>.with(value: R) = with { value }
