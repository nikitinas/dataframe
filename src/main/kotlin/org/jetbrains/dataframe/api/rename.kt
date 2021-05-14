package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.ColumnWithPath
import org.jetbrains.dataframe.impl.columns.DataColumnInternal
import org.jetbrains.dataframe.impl.columns.RenamedColumnReference
import org.jetbrains.dataframe.impl.columns.toColumnSet
import org.jetbrains.dataframe.impl.columns.toColumns
import kotlin.reflect.KProperty

fun <T> DataFrame<T>.rename(vararg mappings: Pair<String, String>) = rename { mappings.map { it.first.toColumnDef() }.toColumnSet() }
        .into(*mappings.map { it.second }.toTypedArray())

fun <T, C> DataFrame<T>.rename(selector: ColumnsSelector<T, C>) = RenameClause(this, selector)
fun <T, C> DataFrame<T>.rename(vararg cols: ColumnReference<C>) = rename { cols.toColumns() }
fun <T, C> DataFrame<T>.rename(vararg cols: KProperty<C>) = rename { cols.toColumns() }
fun <T> DataFrame<T>.rename(vararg cols: String) = rename { cols.toColumns() }
fun <T, C> DataFrame<T>.rename(cols: Iterable<ColumnReference<C>>) = rename { cols.toColumnSet() }

data class RenameClause<T, C>(val df: DataFrame<T>, val selector: ColumnsSelector<T, C>)

fun <T, C> RenameClause<T, C>.into(vararg newColumns: ColumnReference<*>) =
    into(*newColumns.map { it.name() }.toTypedArray())
fun <T, C> RenameClause<T, C>.into(vararg newNames: String) = df.move(selector).intoIndexed { col, index ->
    col.path.drop(1) + newNames[index]
}

fun <T, C> RenameClause<T, C>.into(transform: (ColumnWithPath<C>) -> String) = df.move(selector).into {
    it.path.dropLast(1) + transform(it)
}