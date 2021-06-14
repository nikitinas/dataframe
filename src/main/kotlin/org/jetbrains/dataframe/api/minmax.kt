package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.typeClass
import org.jetbrains.dataframe.impl.columns.assertIsComparable
import java.math.BigDecimal
import kotlin.reflect.KClass
import kotlin.reflect.KProperty

fun <T, D : Comparable<D>> DataFrame<T>.max(col: KProperty<D?>): D? = get(col).max()
fun <T, D : Comparable<D>> DataFrame<T>.max(col: ColumnReference<D?>): D? = get(col).max()
fun <T, D : Comparable<D>> DataFrame<T>.max(selector: RowSelector<T, D?>): D? = rows().asSequence().map { selector(it, it) }.filterNotNull().maxOrNull()
fun <T> DataFrame<T>.max(col: String): Any? = get(col).maxUnsafe()

fun <T, D : Comparable<D>> DataFrame<T>.min(col: KProperty<D?>): D? = get(col).min()
fun <T, D : Comparable<D>> DataFrame<T>.min(col: ColumnReference<D?>): D? = get(col).min()
fun <T, D : Comparable<D>> DataFrame<T>.min(selector: RowSelector<T, D?>): D? = rows().asSequence().map { selector(it, it) }.filterNotNull().minOrNull()
fun <T> DataFrame<T>.min(col: String): Any? = get(col).minUnsafe()

internal fun <T> DataColumn<T?>.minUnsafe(): T? {
    val casted = assertIsComparable() as DataColumn<Comparable<Any>?>
    return casted.min() as T?
}

internal fun <T> DataColumn<T?>.maxUnsafe(): T? {
    val casted = assertIsComparable() as DataColumn<Comparable<Any>?>
    return casted.max() as T?
}

internal fun <T : Number> Iterable<T>.min(clazz: KClass<*>) = when (clazz) {
    Double::class -> (this as Iterable<Double>).minOrNull()
    Float::class -> (this as Iterable<Float>).minOrNull()
    Int::class, Short::class, Byte::class -> (this as Iterable<Int>).minOrNull()
    Long::class -> (this as Iterable<Long>).minOrNull()
    BigDecimal::class -> (this as Iterable<BigDecimal>).minOrNull()
    else -> throw IllegalArgumentException()
}

internal fun <T : Number> Iterable<T>.max(clazz: KClass<*>) = when (clazz) {
    Double::class -> (this as Iterable<Double>).maxOrNull()
    Float::class -> (this as Iterable<Float>).maxOrNull()
    Int::class, Short::class, Byte::class -> (this as Iterable<Int>).maxOrNull()
    Long::class -> (this as Iterable<Long>).maxOrNull()
    BigDecimal::class -> (this as Iterable<BigDecimal>).maxOrNull()
    else -> throw IllegalArgumentException()
}

fun <T: Comparable<T>> DataColumn<T?>.min() = asSequence().filterNotNull().minOrNull()
fun <T: Comparable<T>> DataColumn<T?>.max() = asSequence().filterNotNull().maxOrNull()

internal fun DataColumn<Number?>.minNumber() = asIterable().filterNotNull().min(typeClass)
internal fun DataColumn<Number?>.maxNumber() = asIterable().filterNotNull().max(typeClass)

internal fun <T:Comparable<T>> Iterable<T?>.min(): T? = filterNotNull().minOrNull()
internal fun <T:Comparable<T>> Iterable<T?>.max(): T? = filterNotNull().maxOrNull()