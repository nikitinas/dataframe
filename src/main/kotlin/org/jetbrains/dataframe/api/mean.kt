package org.jetbrains.dataframe

import java.math.BigDecimal
import kotlin.reflect.KClass

@JvmName("meanT?")
fun <T : Number> Sequence<T?>.mean(clazz: KClass<*>, skipNa: Boolean = true): Double = filterNotNull().mean(clazz, skipNa)

fun <T: Number> Iterable<T>.mean(clazz: KClass<*>, skipNaN: Boolean) = asSequence().mean(clazz, skipNaN)

fun <T: Number> Sequence<T>.mean(clazz: KClass<*>, skipNaN: Boolean): Double {
    return when (clazz) {
        Double::class -> (this as Sequence<Double>).mean(skipNaN)
        Float::class -> (this as Sequence<Float>).mean(skipNaN)
        Int::class -> (this as Sequence<Int>).map { it.toDouble() }.mean(false)
        Short::class -> (this as Sequence<Short>).map { it.toDouble() }.mean(false)
        Byte::class -> (this as Sequence<Byte>).map { it.toDouble() }.mean(false)
        Long::class -> (this as Sequence<Long>).map { it.toDouble() }.mean(false)
        BigDecimal::class -> (this as Sequence<BigDecimal>).map { it.toDouble() }.mean(false)
        Number::class -> (this as Sequence<Number>).map { it.toDouble() }.mean(skipNaN)
        else -> throw IllegalArgumentException()
    }
}

fun Sequence<Double>.mean(skipNaN: Boolean): Double {
    var count = 0
    var sum: Double = 0.toDouble()
    for (element in this) {
        if(element.isNaN()) {
            if(skipNaN) continue
            else return Double.NaN
        }
        sum += element
        count++
    }
    return if(count > 0) sum / count else Double.NaN
}

@JvmName("meanFloat")
fun Sequence<Float>.mean(skipNaN: Boolean): Double {
    var count = 0
    var sum: Double = 0.toDouble()
    for (element in this) {
        if(element.isNaN()) {
            if(skipNaN) continue
            else return Double.NaN
        }
        sum += element
        count++
    }
    return if(count > 0) sum / count else Double.NaN
}

@JvmName("doubleMean")
fun Iterable<Double>.mean(skipNaN: Boolean): Double = asSequence().mean(skipNaN)

@JvmName("floatMean")
fun Iterable<Float>.mean(skipNaN: Boolean): Double = asSequence().mean(skipNaN)

@JvmName("intMean")
fun Iterable<Int>.mean(): Double =
    if (this is Collection) {
        if(size > 0) sumOf { it.toDouble() } / size else Double.NaN
    } else {
        var count = 0
        val sum = sumOf { count++;it.toDouble() }
        if(count > 0) sum / count else Double.NaN
    }

@JvmName("shortMean")
fun Iterable<Short>.mean(): Double =
    if (this is Collection) {
        if(size > 0) sumOf { it.toDouble() } / size else Double.NaN
    } else {
        var count = 0
        val sum = sumOf { count++;it.toDouble() }
        if(count > 0) sum / count else Double.NaN
    }

@JvmName("byteMean")
fun Iterable<Byte>.mean(): Double =
    if (this is Collection) {
        if(size > 0) sumOf { it.toDouble() } / size else Double.NaN
    } else {
        var count = 0
        val sum = sumOf { count++;it.toDouble() }
        if(count > 0) sum / count else Double.NaN
    }

@JvmName("longMean")
fun Iterable<Long>.mean(): Double =
    if (this is Collection) {
        if(size > 0) sumOf { it.toDouble() } / size else Double.NaN
    } else {
        var count = 0
        val sum = sumOf { count++;it.toDouble() }
        if(count > 0) sum / count else Double.NaN
    }

@JvmName("bigDecimalMean")
fun Iterable<BigDecimal>.mean(): Double =
    if (this is Collection) {
        if(size > 0) sum().toDouble() / size else Double.NaN
    } else {
        var count = 0
        val sum = sumOf { count++;it.toDouble() }
        if(count > 0) sum / count else Double.NaN
    }