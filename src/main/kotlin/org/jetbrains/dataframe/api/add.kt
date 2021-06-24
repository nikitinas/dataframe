package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.AnyCol
import org.jetbrains.dataframe.columns.ColumnAccessor
import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.impl.DataRowImpl
import kotlin.reflect.KProperty

public operator fun <T> DataFrame<T>.plus(col: AnyCol): DataFrame<T> = dataFrameOf(columns() + col).typed()

public operator fun <T> DataFrame<T>.plus(col: Iterable<AnyCol>): DataFrame<T> = dataFrameOf(columns() + col).typed()

public fun <T> DataFrame<T>.add(cols: Iterable<AnyCol>): DataFrame<T> = this + cols

public fun <T> DataFrame<T>.add(other: AnyFrame): DataFrame<T> = add(other.columns())

public fun <T> DataFrame<T>.add(column: AnyCol): DataFrame<T> = this + column

public fun <T> DataFrame<T>.add(name: String, data: AnyCol): DataFrame<T> = dataFrameOf(columns() + data.rename(name)).typed<T>()

public interface AddDataRow<out T> : DataRow<T> {
    public fun <C> AnyRow.added(): C
}

internal class AddDataRowImpl<T>(index: Int, owner: DataFrame<T>, private val container: List<*>) : DataRowImpl<T>(index, owner), AddDataRow<T> {

    override fun <C> AnyRow.added() = container[index] as C
}

public typealias AddExpression<T, C> = AddDataRow<T>.(AddDataRow<T>) -> C

public inline fun <reified R, T> DataFrame<T>.add(name: String, noinline expression: AddExpression<T, R>): DataFrame<T> =
    (this + newColumn(name, expression))

public inline fun <reified R, T> DataFrame<T>.add(property: KProperty<R>, noinline expression: RowSelector<T, R>): DataFrame<T> =
    (this + newColumn(property.name, expression))

public inline fun <reified R, T, G> GroupedDataFrame<T, G>.add(name: String, noinline expression: RowSelector<G, R>): GroupedDataFrame<T, G> =
    mapNotNullGroups { add(name, expression) }

public inline fun <reified R, T> DataFrame<T>.add(column: ColumnAccessor<R>, noinline expression: AddExpression<T, R>): DataFrame<T> {
    val col = newColumn(column.name(), expression)
    val path = column.path()
    if (path.size == 1) return this + col
    return insert(path, col)
}

public fun <T> DataFrame<T>.add(body: TypedColumnsFromDataRowBuilder<T>.() -> Unit): DataFrame<T> {
    val builder = TypedColumnsFromDataRowBuilder(this)
    body(builder)
    return dataFrameOf(this@add.columns() + builder.columns).typed()
}

public operator fun <T> DataFrame<T>.plus(body: TypedColumnsFromDataRowBuilder<T>.() -> Unit): DataFrame<T> = add(body)

public class TypedColumnsFromDataRowBuilder<T>(public val df: DataFrame<T>) : DataFrameBase<T> by df {
    internal val columns = mutableListOf<AnyCol>()

    public fun add(column: AnyCol): Boolean = columns.add(column)

    public inline fun <reified R> add(name: String, noinline expression: RowSelector<T, R>): Boolean = add(df.newColumn(name, expression))

    public inline fun <reified R> add(`colum nReference`: ColumnReference<R>, noinline expression: RowSelector<T, R>): Boolean = add(df.newColumn(`colum nReference`.name(), expression))

    public inline infix fun <reified R> ColumnReference<R>.by(noinline expression: RowSelector<T, R>): Boolean = add(df.newColumn(name(), expression))

    public inline operator fun <reified R> ColumnReference<R>.invoke(noinline expression: RowSelector<T, R>): Boolean = by(expression)

    public inline infix fun <reified R> String.by(noinline expression: RowSelector<T, R>): Boolean = add(this, expression)

    public inline operator fun <reified R> String.invoke(noinline expression: RowSelector<T, R>): Boolean = by(expression)

    public operator fun String.invoke(column: AnyCol): Boolean = add(column.rename(this))

    public inline operator fun <reified R> ColumnReference<R>.invoke(column: DataColumn<R>): Boolean = name()(column)

    public infix fun AnyCol.into(name: String): Boolean = add(rename(name))
}
