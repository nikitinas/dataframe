package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.impl.toIterable
import kotlin.reflect.KProperty
import kotlin.reflect.KProperty1

interface DataRowBase<out T> {
    operator fun get(name: String): Any?
    fun tryGet(name: String): Any?
    fun index(): Int
    fun prev(): DataRow<T>?
    fun next(): DataRow<T>?
}

interface DataRow<out T>: DataRowBase<T> {
    fun df(): DataFrame<T>

    fun getRow(index: Int): DataRow<T>?
    override operator fun get(name: String): Any?
    override fun tryGet(name: String): Any?
    operator fun get(columnIndex: Int): Any?
    operator fun <R> get(selector: RowSelector<T, R>) = selector(this, this)
    operator fun <R> get(column: ColumnReference<R>): R
    operator fun <R> get(property: KProperty<R>) = get(property.name) as R
    operator fun <R> ColumnReference<R>.invoke() = get(this)
    operator fun <R> String.invoke() = get(this) as R

    operator fun get(first: Column, vararg other: Column) = owner.select(listOf(first) + other)[index]
    operator fun get(first: String, vararg other: String) = owner.select(listOf(first) + other)[index]

    fun neighbours(relativeIndices: Iterable<Int>): Sequence<DataRow<T>> = relativeIndices.asSequence().mapNotNull { getRow(index + it) }

    fun <T> read(name: String) = get(name) as T
    fun size() = owner.ncol()
    fun values(): List<Any?>

    fun int(name: String) = read<Int>(name)
    fun nint(name: String) = read<Int?>(name)
    fun string(name: String) = read<String>(name)
    fun nstring(name: String) = read<String?>(name)
    fun double(name: String) = read<Double>(name)
    fun ndouble(name: String) = read<Double?>(name)

    fun forwardIterable() = this.toIterable { it.next }
    fun backwardIterable() = this.toIterable { it.prev }

    operator fun <R : Comparable<R>> ColumnReference<R>.compareTo(other: R) = get(this).compareTo(other)
    operator fun ColumnReference<Int>.plus(a: Int) = get(this) + a
    operator fun ColumnReference<Long>.plus(a: Long) = get(this) + a
    operator fun ColumnReference<Double>.plus(a: Double) = get(this) + a
    operator fun ColumnReference<String>.plus(a: String) = get(this) + a
    operator fun Int.plus(col: ColumnReference<Int>) = this + get(col)
    operator fun Long.plus(col: ColumnReference<Long>) = this + get(col)
    operator fun Double.plus(col: ColumnReference<Double>) = this + get(col)
    operator fun String.plus(col: ColumnReference<String>) = this + get(col)

    operator fun ColumnReference<Int>.minus(a: Int) = get(this) - a
    operator fun ColumnReference<Long>.minus(a: Long) = get(this) - a
    operator fun ColumnReference<Double>.minus(a: Double) = get(this) - a
    operator fun Int.minus(col: ColumnReference<Int>) = this - get(col)
    operator fun Long.minus(col: ColumnReference<Long>) = this - get(col)
    operator fun Double.minus(col: ColumnReference<Double>) = this - get(col)

    operator fun ColumnReference<Int>.times(a: Int) = get(this) * a
    operator fun ColumnReference<Long>.times(a: Long) = get(this) * a
    operator fun ColumnReference<Double>.times(a: Double) = get(this) * a
    operator fun ColumnReference<Double>.times(a: Int) = get(this) * a
    operator fun ColumnReference<Long>.times(a: Int) = get(this) * a
    operator fun ColumnReference<Double>.times(a: Long) = get(this) * a

    operator fun ColumnReference<Int>.div(a: Int) = get(this) / a
    operator fun ColumnReference<Long>.div(a: Long) = get(this) / a
    operator fun ColumnReference<Double>.div(a: Double) = get(this) / a
    operator fun ColumnReference<Double>.div(a: Int) = get(this) / a
    operator fun ColumnReference<Long>.div(a: Int) = get(this) / a
    operator fun ColumnReference<Double>.div(a: Long) = get(this) / a

    infix fun <R> ColumnReference<R>.eq(a: R?) = get(this) == a
    infix fun <R> KProperty1<*, R>.eq(a: R?) = get(this) == a
    infix fun <R> ColumnReference<R>.neq(a: R?) = get(this) != a
    infix fun <R> KProperty1<*, R>.neq(a: R?) = get(this) != a
}

internal val AnyRow.values get() = values()
internal val <T> DataRow<T>.owner: DataFrame<T> get() = df()

val AnyRow.index @JvmName("getRowIndex") get() = index()
val <T> DataRow<T>.prev: DataRow<T>? get() = prev()
val <T> DataRow<T>.next: DataRow<T>? get() = next()

typealias Selector<T, R> = T.(T) -> R
typealias RowSelector<T, R> = DataRow<T>.(DataRow<T>) -> R
typealias RowFilter<T> = RowSelector<T, Boolean>
typealias VectorizedRowFilter<T> = Selector<DataFrameBase<T>, BooleanArray>
typealias RowCellSelector<T, C, R> = DataRow<T>.(C) -> R
typealias RowCellFilter<T, C> = RowCellSelector<T, C, Boolean>
typealias RowColumnSelector<T, C, R> = (DataRow<T>, DataColumn<C>) -> R


typealias AnyRow = DataRow<*>