package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.impl.columns.toColumnSet
import org.jetbrains.dataframe.impl.columns.toColumns
import kotlin.reflect.KProperty

fun <T> DataFrame<T>.filter(predicate: RowFilter<T>): DataFrame<T> =
        (0 until nrow()).filter {
            val row = get(it)
            predicate(row, row)
        }.let { get(it) }

fun <T> DataFrame<T>.filterNotNull() = filterNotNull { all() }

fun <T> DataFrame<T>.filterNotNull(selector: ColumnsSelector<T, *>): DataFrame<T> {
    val cols = this[selector]
    return filter { row -> cols.all { col -> col[row] != null } }
}

fun <T> DataFrame<T>.filterNotNull(vararg cols: KProperty<*>) = filterNotNull { cols.toColumns() }
fun <T> DataFrame<T>.filterNotNull(vararg cols: String) = filterNotNull { cols.toColumns() }
fun <T> DataFrame<T>.filterNotNull(vararg cols: Column) = filterNotNull { cols.toColumns() }
fun <T> DataFrame<T>.filterNotNull(cols: Iterable<Column>) = filterNotNull { cols.toColumnSet() }

fun <T> DataFrame<T>.filterNotNullAny(selector: ColumnsSelector<T, *>): DataFrame<T> {
    val cols = this[selector]
    return filter { row -> cols.any { col -> col[row] != null } }
}

fun <T> DataFrame<T>.filterNotNullAny(vararg cols: KProperty<*>) = filterNotNullAny { cols.toColumns() }
fun <T> DataFrame<T>.filterNotNullAny(vararg cols: String) = filterNotNullAny { cols.toColumns() }
fun <T> DataFrame<T>.filterNotNullAny(vararg cols: Column) = filterNotNullAny { cols.toColumns() }
fun <T> DataFrame<T>.filterNotNullAny(cols: Iterable<Column>) = filterNotNullAny { cols.toColumnSet() }

fun <T> DataColumn<T>.filter(predicate: Predicate<T>) = slice(isMatching(predicate))

fun <T> DataFrame<T>.filterFast(predicate: VectorizedRowFilter<T>) = this.get(predicate(this))