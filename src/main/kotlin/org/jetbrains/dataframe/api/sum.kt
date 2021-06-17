package org.jetbrains.dataframe

import java.math.BigDecimal
import kotlin.reflect.KClass

@PublishedApi
internal fun <T, R: Number> Iterable<T>.sumOf(clazz:KClass<*>, selector: (T)->R): R = when (clazz) {
    Double::class -> sumOf(selector as ((T)->Double)) as R
    Int::class -> sumOf(selector as ((T)->Int)) as R
    Long::class -> sumOf(selector as ((T)->Long)) as R
    BigDecimal::class -> sumOf(selector as ((T)->BigDecimal)) as R
    else -> TODO()
}

@PublishedApi
internal fun <T : Number> Iterable<T>.sum(clazz: KClass<*>) = when (clazz) {
    Double::class -> (this as Iterable<Double>).sum() as T
    Float::class -> (this as Iterable<Float>).sum() as T
    Int::class, Short::class, Byte::class -> (this as Iterable<Int>).sum() as T
    Long::class -> (this as Iterable<Long>).sum() as T
    BigDecimal::class -> (this as Iterable<BigDecimal>).sum() as T
    else -> TODO()
}

@PublishedApi
internal fun Iterable<BigDecimal>.sum(): BigDecimal {
    var sum: BigDecimal = BigDecimal.ZERO
    for (element in this) {
        sum += element
    }
    return sum
}