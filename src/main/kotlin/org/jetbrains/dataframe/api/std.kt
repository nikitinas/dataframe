package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.hasNulls
import org.jetbrains.dataframe.columns.name
import org.jetbrains.dataframe.columns.type
import org.jetbrains.dataframe.columns.values
import java.math.BigDecimal
import kotlin.math.sqrt
import kotlin.reflect.KClass
import kotlin.reflect.KProperty
import kotlin.reflect.full.isSubclassOf
import kotlin.reflect.jvm.jvmErasure


inline fun <reified T : Number> Iterable<T>.std(): Double = std(T::class)

inline fun <T, reified D : Number> DataFrame<T>.std(crossinline selector: RowSelector<T, D?>): Double = rows().asSequence().map { selector(it, it) }.filterNotNull().asIterable().std()
inline fun <T, reified D : Number> DataFrame<T>.std(col: ColumnReference<D>): Double = get(col).std()
inline fun <T, reified D : Number> DataFrame<T>.std(col: KProperty<D>): Double = get(col).std()

fun <T> DataFrame<T>.std(): DataRow<T> {
    return columns().map {
        it.takeIf { it.isNumber() }?.let {
            column(it.name, listOf((it as DataColumn<Number>).std()))
        } ?: column(it.name(), listOf(""))
    }.asDataFrame<T>()[0]
}

fun <T, G> GroupedDataFrame<T, G>.std(): DataFrame<T> {

    val keyColumnNames = keys.columnNames().toSet()
    return aggregate {
        columns().filter { (it.type.classifier!! as KClass<*>).isSubclassOf(Number::class) && !keyColumnNames.contains(it.name()) }
            .forEach { col ->
                (col as DataColumn<Number?>).std() into col.name
            }
    }
}

fun <T : Number> Iterable<T>.std(clazz: KClass<T>) = when (clazz) {
    Double::class -> (this as Iterable<Double>).std()
    Float::class -> (this as Iterable<Float>).std()
    Int::class, Short::class, Byte::class -> (this as Iterable<Int>).std()
    Long::class -> (this as Iterable<Long>).std()
    BigDecimal::class -> (this as Iterable<BigDecimal>).std()
    else -> throw IllegalArgumentException()
}

fun <T: Number> DataColumn<T?>.std(): Double = (if(hasNulls) values.filterNotNull() else (values as Iterable<T>)).std(type.jvmErasure as KClass<T>)

@JvmName("doubleStd")
fun Iterable<Double>.std() = stdMean().first

@JvmName("floatStd")
fun Iterable<Float>.std() = stdMean().first

@JvmName("intStd")
fun Iterable<Int>.std() = stdMean().first

@JvmName("longStd")
fun Iterable<Long>.std() = stdMean().first

@JvmName("bigDecimalStd")
fun Iterable<BigDecimal>.std() = stdMean().first

@JvmName("doubleStdMean")
fun Iterable<Double>.stdMean(): Pair<Double, Double> {
    val m = mean()
    return sqrt(fold(0.0) { acc, el ->
        val diff = el - m
        acc + diff * diff
    }) to m
}

@JvmName("floatStdMean")
fun Iterable<Float>.stdMean(): Pair<Double, Double> {
    val m = mean()
    return sqrt(fold(0.0){ acc, el ->
        val diff = el - m
        acc + diff * diff
    }) to m
}

@JvmName("intStdMean")
fun Iterable<Int>.stdMean(): Pair<Double, Double> {
    val m = mean()
    return sqrt(fold(0.0) { acc, el ->
        val diff = el - m
        acc + diff * diff
    }) to m
}

@JvmName("longStdMean")
fun Iterable<Long>.stdMean(): Pair<Double, Double> {
    val m = mean()
    return sqrt(fold(0.0) { acc, el ->
        val diff = el - m
        acc + diff * diff
    }) to m
}

@JvmName("bigDecimalStdMean")
fun Iterable<BigDecimal>.stdMean(): Pair<Double, Double> {
    val m = mean()
    return sqrt(fold(0.0) { acc, el ->
        val diff = el.toDouble() - m
        acc + diff * diff
    }) to m
}
