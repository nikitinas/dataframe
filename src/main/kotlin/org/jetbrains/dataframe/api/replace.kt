package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.AnyCol
import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.FrameColumn
import org.jetbrains.dataframe.columns.ColumnGroup
import org.jetbrains.dataframe.columns.name
import org.jetbrains.dataframe.columns.type
import org.jetbrains.dataframe.impl.columns.toColumnSet
import org.jetbrains.dataframe.impl.columns.toColumns
import kotlin.reflect.KProperty
import kotlin.reflect.full.isSubclassOf
import kotlin.reflect.full.withNullability
import kotlin.reflect.jvm.jvmErasure

fun <T, C> DataFrame<T>.replace(selector: ColumnsSelector<T, C>) = ReplaceCause(this, selector)
fun <T, C> DataFrame<T>.replace(vararg cols: ColumnReference<C>) = replace { cols.toColumns() }
fun <T, C> DataFrame<T>.replace(vararg cols: KProperty<C>) = replace { cols.toColumns() }
fun <T> DataFrame<T>.replace(vararg cols: String) = replace { cols.toColumns() }
fun <T, C> DataFrame<T>.replace(cols: Iterable<ColumnReference<C>>) = replace { cols.toColumnSet() }

fun <T> DataFrame<T>.replaceAll(vararg valuePairs: Pair<Any?, Any?>, selector: ColumnsSelector<T, *> = { dfs() }): DataFrame<T> {
    val map = valuePairs.toMap()
    return update(selector).withExpression { map[it] ?: it }
}

data class ReplaceCause<T, C>(val df: DataFrame<T>, val selector: ColumnsSelector<T, C>)

fun <T, C> ReplaceCause<T, C>.with(vararg columns: AnyCol) = with(columns.toList())

fun <T, C> ReplaceCause<T, C>.with(newColumns: List<AnyCol>): DataFrame<T> {
    var index = 0
    return with {
        require(index < newColumns.size) { "Insufficient number of new columns in 'replace': ${newColumns.size} instead of ${df[selector].size}" }
        newColumns[index++]
    }
}

fun <T, C> ReplaceCause<T, C>.with(transform: DataFrameBase<T>.(DataColumn<C>)-> AnyCol): DataFrame<T> {

    val removeResult = df.doRemove(selector)
    val toInsert = removeResult.removedColumns.map {
        val newCol = transform(df, it.data.column as DataColumn<C>)
        ColumnToInsert(it.pathFromRoot().dropLast(1) + newCol.name, newCol, it)
    }
    return removeResult.df.insert(toInsert)
}

/**
 * Replaces all values in column asserting that new values are compatible with current column kind
 */
fun <T> DataColumn<T>.replaceAll(values: List<T>): DataColumn<T> = when (this) {
    is FrameColumn<*> -> {
        var nulls = false
        values.forEach {
            if (it == null) nulls = true
            else require(it is AnyFrame) { "Can not add value '$it' to FrameColumn" }
        }
        val groups = (values as List<AnyFrame?>)
        DataColumn.create(name, groups, nulls) as DataColumn<T>
    }
    is ColumnGroup<*> -> {
        this.columns().mapIndexed { colIndex, col ->
            val newValues = values.map {
                when (it) {
                    null -> null
                    is List<*> -> it[colIndex]
                    is AnyRow -> it.tryGet(col.name)
                    else -> require(false) { "Can not add value '$it' to MapColumn" }
                }
            }
            col.replaceAll(newValues)
        }.asDataFrame<Unit>().let { DataColumn.create(name, it) } as DataColumn<T>
    }
    else -> {
        var nulls = false
        val kclass = type.jvmErasure
        values.forEach {
            when (it) {
                null -> nulls = true
                else -> {
                    require(it.javaClass.kotlin.isSubclassOf(kclass)) { "Can not append value '${it}' to column '${name}' of type ${type}" }
                }
            }
        }
        DataColumn.create(name, values, type.withNullability(nulls))
    }
}