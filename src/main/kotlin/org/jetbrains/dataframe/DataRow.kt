package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.AnyCol
import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.shortPath
import org.jetbrains.dataframe.impl.toIterable
import kotlin.reflect.KProperty
import kotlin.reflect.KProperty1

public interface DataRowBase<out T> {
    public operator fun get(name: String): Any?
    public fun tryGet(name: String): Any?
    public fun index(): Int
    public fun prev(): DataRow<T>?
    public fun next(): DataRow<T>?
}

public interface DataRow<out T> : DataRowBase<T> {
    public fun df(): DataFrame<T>

    public fun getRow(index: Int): DataRow<T>?
    override operator fun get(name: String): Any?

    public fun ncol(): Int = df().ncol()
    public fun columnNames(): List<String> = df().columnNames()

    override fun tryGet(name: String): Any?
    public operator fun get(columnIndex: Int): Any?
    public operator fun <R> get(selector: RowSelector<T, R>): R = selector(this, this)
    public operator fun <R> get(column: ColumnReference<R>): R
    public operator fun <R> get(columns: List<ColumnReference<R>>): List<R> = columns.map { get(it) }
    public operator fun <R> get(property: KProperty<R>): R = get(property.name) as R
    public operator fun <R> ColumnReference<R>.invoke(): R = get(this)
    public operator fun <R> String.invoke(): R = this@DataRow[this] as R

    public operator fun get(first: Column, vararg other: Column): DataRow<T> = owner.select(listOf(first) + other)[index]
    public operator fun get(first: String, vararg other: String): DataRow<T> = owner.select(listOf(first) + other)[index]

    public fun neighbours(relativeIndices: Iterable<Int>): Sequence<DataRow<T>> = relativeIndices.asSequence().mapNotNull { getRow(index + it) }

    public fun <T> read(name: String): T = get(name) as T
    public fun size(): Int = owner.ncol()
    public fun values(): List<Any?>

    public fun int(name: String): Int = read(name)
    public fun nint(name: String): Int? = read(name)
    public fun string(name: String): String = read(name)
    public fun nstring(name: String): String? = read(name)
    public fun double(name: String): Double = read(name)
    public fun ndouble(name: String): Double? = read(name)

    public fun forwardIterable(): Iterable<DataRow<T>> = this.toIterable { it.next }
    public fun backwardIterable(): Iterable<DataRow<T>> = this.toIterable { it.prev }

    public operator fun <R : Comparable<R>> ColumnReference<R>.compareTo(other: R): Int = get(this).compareTo(other)
    public operator fun ColumnReference<Int>.plus(a: Int): Int = get(this) + a
    public operator fun ColumnReference<Long>.plus(a: Long): Long = get(this) + a
    public operator fun ColumnReference<Double>.plus(a: Double): Double = get(this) + a
    public operator fun ColumnReference<String>.plus(a: String): String = get(this) + a
    public operator fun Int.plus(col: ColumnReference<Int>): Int = this + get(col)
    public operator fun Long.plus(col: ColumnReference<Long>): Long = this + get(col)
    public operator fun Double.plus(col: ColumnReference<Double>): Double = this + get(col)
    public operator fun String.plus(col: ColumnReference<String>): String = this + get(col)

    public operator fun ColumnReference<Int>.minus(a: Int): Int = get(this) - a
    public operator fun ColumnReference<Long>.minus(a: Long): Long = get(this) - a
    public operator fun ColumnReference<Double>.minus(a: Double): Double = get(this) - a
    public operator fun Int.minus(col: ColumnReference<Int>): Int = this - get(col)
    public operator fun Long.minus(col: ColumnReference<Long>): Long = this - get(col)
    public operator fun Double.minus(col: ColumnReference<Double>): Double = this - get(col)

    public operator fun ColumnReference<Int>.times(a: Int): Int = get(this) * a
    public operator fun ColumnReference<Long>.times(a: Long): Long = get(this) * a
    public operator fun ColumnReference<Double>.times(a: Double): Double = get(this) * a
    public operator fun ColumnReference<Double>.times(a: Int): Double = get(this) * a
    public operator fun ColumnReference<Long>.times(a: Int): Long = get(this) * a
    public operator fun ColumnReference<Double>.times(a: Long): Double = get(this) * a

    public operator fun ColumnReference<Int>.div(a: Int): Int = get(this) / a
    public operator fun ColumnReference<Long>.div(a: Long): Long = get(this) / a
    public operator fun ColumnReference<Double>.div(a: Double): Double = get(this) / a
    public operator fun ColumnReference<Double>.div(a: Int): Double = get(this) / a
    public operator fun ColumnReference<Long>.div(a: Int): Long = get(this) / a
    public operator fun ColumnReference<Double>.div(a: Long): Double = get(this) / a

    public infix fun <R> ColumnReference<R>.eq(a: R?): Boolean = get(this) == a
    public infix fun <R> KProperty1<*, R>.eq(a: R?): Boolean = get(this) == a
    public infix fun <R> ColumnReference<R>.neq(a: R?): Boolean = get(this) != a
    public infix fun <R> KProperty1<*, R>.neq(a: R?): Boolean = get(this) != a
}

internal val AnyRow.values get() = values()
internal val <T> DataRow<T>.owner: DataFrame<T> get() = df()

public val AnyRow.index: Int
    @JvmName("getRowIndex")
    get() = index()
public val <T> DataRow<T>.prev: DataRow<T>? get() = prev()
public val <T> DataRow<T>.next: DataRow<T>? get() = next()

public typealias Selector<T, R> = T.(T) -> R
public typealias RowSelector<T, R> = DataRow<T>.(DataRow<T>) -> R
public typealias RowFilter<T> = RowSelector<T, Boolean>
public typealias VectorizedRowFilter<T> = Selector<DataFrameBase<T>, BooleanArray>
public typealias RowCellSelector<T, C, R> = DataRow<T>.(C) -> R
public typealias RowCellFilter<T, C> = RowCellSelector<T, C, Boolean>
public typealias RowColumnSelector<T, C, R> = (DataRow<T>, DataColumn<C>) -> R

public typealias AnyRow = DataRow<*>

internal fun AnyRow.namedValues(): Sequence<NamedValue> = owner.columns().asSequence().map {
    NamedValue.create(it.shortPath(), it[index], it.type(), it.defaultValue(), guessType = false)
}

public operator fun Any?.get(column: String): Any? = when (this) {
    is AnyRow -> get(column)
    is AnyCol -> get(column)
    is AnyFrame -> get(column)
    else -> error("Unable to get value by string index from type ${this?.javaClass}")
}
