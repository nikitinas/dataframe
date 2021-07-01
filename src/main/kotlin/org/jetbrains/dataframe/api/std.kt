package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.*
import java.math.BigDecimal
import kotlin.math.sqrt
import kotlin.reflect.KClass
import kotlin.reflect.KProperty
import kotlin.reflect.jvm.jvmErasure

public inline fun <reified T : Number> Iterable<T>.std(): Double = std(T::class)

public inline fun <T, reified D : Number> DataFrame<T>.std(crossinline selector: RowSelector<T, D?>): Double = rows().asSequence().map { selector(it, it) }.filterNotNull().asIterable().std()
public inline fun <T, reified D : Number> DataFrame<T>.std(col: ColumnReference<D>): Double = get(col).std()
public inline fun <T, reified D : Number> DataFrame<T>.std(col: KProperty<D>): Double = get(col).std()

public fun <T> DataFrame<T>.std(): DataRow<T> {
    return columns().map {
        it.takeIf { it.isNumber() }?.let {
            column(it.name, listOf((it as DataColumn<Number>).std()))
        } ?: column(it.name(), listOf(""))
    }.asDataFrame<T>()[0]
}

public fun <T : Number> Iterable<T>.std(clazz: KClass<*>): Double = when (clazz) {
    Double::class -> (this as Iterable<Double>).std()
    Float::class -> (this as Iterable<Float>).std()
    Int::class, Short::class, Byte::class -> (this as Iterable<Int>).std()
    Long::class -> (this as Iterable<Long>).std()
    BigDecimal::class -> (this as Iterable<BigDecimal>).std()
    else -> throw IllegalArgumentException()
}

public fun <T : Number> DataColumn<T?>.std(): Double = (if (hasNulls) values.filterNotNull() else (values as Iterable<T>)).std(type.jvmErasure as KClass<T>)

@JvmName("doubleStd")
public fun Iterable<Double>.std(): Double = stdMean().first

@JvmName("floatStd")
public fun Iterable<Float>.std(): Double = stdMean().first

@JvmName("intStd")
public fun Iterable<Int>.std(): Double = stdMean().first

@JvmName("longStd")
public fun Iterable<Long>.std(): Double = stdMean().first

@JvmName("bigDecimalStd")
public fun Iterable<BigDecimal>.std(): Double = stdMean().first

@JvmName("doubleStdMean")
public fun Iterable<Double>.stdMean(): Pair<Double, Double> {
    val m = mean(false)
    return sqrt(
        fold(0.0) { acc, el ->
            val diff = el - m
            acc + diff * diff
        }
    ) to m
}

@JvmName("floatStdMean")
public fun Iterable<Float>.stdMean(): Pair<Double, Double> {
    val m = mean(false)
    return sqrt(
        fold(0.0) { acc, el ->
            val diff = el - m
            acc + diff * diff
        }
    ) to m
}

@JvmName("intStdMean")
public fun Iterable<Int>.stdMean(): Pair<Double, Double> {
    val m = mean()
    return sqrt(
        fold(0.0) { acc, el ->
            val diff = el - m
            acc + diff * diff
        }
    ) to m
}

@JvmName("longStdMean")
public fun Iterable<Long>.stdMean(): Pair<Double, Double> {
    val m = mean()
    return sqrt(
        fold(0.0) { acc, el ->
            val diff = el - m
            acc + diff * diff
        }
    ) to m
}

@JvmName("bigDecimalStdMean")
public fun Iterable<BigDecimal>.stdMean(): Pair<Double, Double> {
    val m = mean()
    return sqrt(
        fold(0.0) { acc, el ->
            val diff = el.toDouble() - m
            acc + diff * diff
        }
    ) to m
}
