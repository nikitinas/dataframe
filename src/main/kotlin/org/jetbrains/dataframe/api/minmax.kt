package org.jetbrains.dataframe

import java.math.BigDecimal
import kotlin.reflect.KClass

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
