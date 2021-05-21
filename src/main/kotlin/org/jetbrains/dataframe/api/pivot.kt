package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.ColumnReference

fun <T> DataFrame<T>.pivot(column: ColumnSelector<T, *>) = PivotClause(this, column)
fun <T> DataFrame<T>.pivot(column: String) = pivot { column.toColumnDef() }
fun <T, C> DataFrame<T>.pivot(column: ColumnReference<C>) = pivot { column }

data class PivotClause<T>(
    val df: DataFrame<T>,
    val column: ColumnSelector<T, *>,
    val index: ColumnSelector<T, *>? = null
)

fun <T> PivotClause<T>.withIndex(indexColumn: ColumnSelector<T, *>) = copy(index = indexColumn)
fun <T> PivotClause<T>.withIndex(indexColumn: String) = withIndex { indexColumn.toColumnDef() }
fun <T> PivotClause<T>.withIndex(indexColumn: Column) = withIndex { indexColumn }

fun <T> PivotClause<T>.into(valueColumn: Column) = into { valueColumn }
fun <T> PivotClause<T>.into(valueColumn: String) = into { valueColumn.toColumnDef() }
fun <T> PivotClause<T>.into(valueColumn: ColumnSelector<T, *>): AnyFrame {
    require(index != null) { "Pivot index has to be defined" }
    val cols = listOf(df[index], df[column], df[valueColumn])
    return cols.toDataFrame().spread(cols[1].name()).by(cols[2].name()).into { it.toString() }
}