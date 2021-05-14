package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.values
import java.math.BigDecimal
import kotlin.reflect.KClass
import kotlin.reflect.jvm.jvmErasure

fun <T : Number> Iterable<T>.sum(clazz: KClass<T>) = when (clazz) {
    Double::class -> (this as Iterable<Double>).sum() as T
    Float::class -> (this as Iterable<Float>).sum() as T
    Int::class, Short::class, Byte::class -> (this as Iterable<Int>).sum() as T
    Long::class -> (this as Iterable<Long>).sum() as T
    BigDecimal::class -> (this as Iterable<BigDecimal>).sum() as T
    else -> throw IllegalArgumentException()
}

fun Iterable<BigDecimal>.sum(): BigDecimal {
    var sum: BigDecimal = BigDecimal.ZERO
    for (element in this) {
        sum += element
    }
    return sum
}

fun <T : Number> DataColumn<T>.sum() = values.sum(type().jvmErasure as KClass<T>)

inline fun <T, reified R : Number> DataFrame<T>.sumBy(crossinline selector: RowSelector<T, R>) = asSequence().map { selector(it, it)}.asIterable().sum(R::class)

inline fun <T, reified R : Number> DataColumn<T>.sumBy(noinline selector: (T) -> R) = asSequence().map(selector).asIterable().sum(R::class)