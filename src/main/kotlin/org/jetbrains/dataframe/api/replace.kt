package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.impl.columns.toColumnSet
import org.jetbrains.dataframe.impl.columns.toColumns
import kotlin.reflect.KProperty

fun <T, C> DataFrame<T>.replace(selector: ColumnsSelector<T, C>) = ReplaceCause(this, selector)
fun <T, C> DataFrame<T>.replace(vararg cols: ColumnReference<C>) = replace { cols.toColumns() }
fun <T, C> DataFrame<T>.replace(vararg cols: KProperty<C>) = replace { cols.toColumns() }
fun <T> DataFrame<T>.replace(vararg cols: String) = replace { cols.toColumns() }
fun <T, C> DataFrame<T>.replace(cols: Iterable<ColumnReference<C>>) = replace { cols.toColumnSet() }

data class ReplaceCause<T, C>(val df: DataFrame<T>, val selector: ColumnsSelector<T, C>)

fun <T, C> ReplaceCause<T, C>.with(vararg columns: AnyCol) = with(columns.toList())

fun <T, C> ReplaceCause<T, C>.with(newColumns: List<AnyCol>): DataFrame<T> {
    var index = 0
    return with {
        require(index < newColumns.size) { "Insufficient number of new columns in 'replace': ${newColumns.size} instead of ${df[selector].size}" }
        newColumns[index++]
    }
}

fun <T, C> ReplaceCause<T, C>.with(transform: DataFrameBase<T>.(DataColumn<C>)->AnyCol): DataFrame<T> {

    val removeResult = df.doRemove(selector)
    val toInsert = removeResult.removedColumns.map {
        val newCol = transform(df, it.data.column as DataColumn<C>)
        ColumnToInsert(it.pathFromRoot().dropLast(1) + newCol.name, newCol, it)
    }
    return removeResult.df.insert(toInsert)
}