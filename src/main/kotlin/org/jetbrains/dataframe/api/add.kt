package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.AnyCol
import org.jetbrains.dataframe.columns.ColumnAccessor
import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.impl.DataRowImpl
import kotlin.reflect.KProperty

operator fun <T> DataFrame<T>.plus(col: AnyCol) = dataFrameOf(columns() + col).typed<T>()

operator fun <T> DataFrame<T>.plus(col: Iterable<AnyCol>) = dataFrameOf(columns() + col).typed<T>()

fun <T> DataFrame<T>.add(cols: Iterable<AnyCol>) = this + cols

fun <T> DataFrame<T>.add(other: AnyFrame) = add(other.columns())

fun <T> DataFrame<T>.add(column: AnyCol) = this + column

fun <T> DataFrame<T>.add(name: String, data: AnyCol) = dataFrameOf(columns() + data.rename(name)).typed<T>()

interface AddDataRow<out T>: DataRow<T> {
    fun <C> AnyRow.added(): C
}

internal class AddDataRowImpl<T>(index: Int, owner: DataFrame<T>, private val container: List<*>): DataRowImpl<T>(index, owner), AddDataRow<T> {

    override fun <C> AnyRow.added() = container[index] as C
}

typealias AddExpression<T, C> = AddDataRow<T>.(AddDataRow<T>) -> C

inline fun <reified R, T> DataFrame<T>.add(name: String, noinline expression: AddExpression<T, R>) =
        (this + newColumn(name, expression))

inline fun <reified R, T> DataFrame<T>.add(property: KProperty<R>, noinline expression: RowSelector<T, R>) =
    (this + newColumn(property.name, expression))

inline fun <reified R, T, G> GroupedDataFrame<T, G>.add(name: String, noinline expression: RowSelector<G, R>) =
        mapNotNullGroups { add(name, expression) }

inline fun <reified R, T> DataFrame<T>.add(column: ColumnAccessor<R>, noinline expression: AddExpression<T, R>): DataFrame<T> {
    val col = newColumn(column.name(), expression)
    val path = column.path()
    if(path.size == 1) return this + col
    return insert(path, col)
}

fun <T> DataFrame<T>.add(body: TypedColumnsFromDataRowBuilder<T>.() -> Unit): DataFrame<T> {
    val builder = TypedColumnsFromDataRowBuilder(this)
    body(builder)
    return dataFrameOf(this@add.columns() + builder.columns).typed<T>()
}

operator fun <T> DataFrame<T>.plus(body: TypedColumnsFromDataRowBuilder<T>.() -> Unit) = add(body)

class TypedColumnsFromDataRowBuilder<T>(val df: DataFrame<T>): DataFrameBase<T> by df {
    internal val columns = mutableListOf<AnyCol>()

    fun add(column: AnyCol) = columns.add(column)

    inline fun <reified R> add(name: String, noinline expression: RowSelector<T, R>) = add(df.newColumn(name, expression))

    inline fun <reified R> add(`colum nReference`: ColumnReference<R>, noinline expression: RowSelector<T, R>) = add(df.newColumn(`colum nReference`.name(), expression))

    inline infix fun <reified R> ColumnReference<R>.by(noinline expression: RowSelector<T, R>) = add(df.newColumn(name(), expression))

    inline operator fun <reified R> ColumnReference<R>.invoke(noinline expression: RowSelector<T, R>) = by(expression)

    inline infix fun <reified R> String.by(noinline expression: RowSelector<T, R>) = add(this, expression)

    inline operator fun <reified R> String.invoke(noinline expression: RowSelector<T, R>) = by(expression)

    operator fun String.invoke(column: AnyCol) = add(column.rename(this))

    inline operator fun <reified R> ColumnReference<R>.invoke(column: DataColumn<R>) = name()(column)

    infix fun AnyCol.into(name: String) = add(rename(name))
}