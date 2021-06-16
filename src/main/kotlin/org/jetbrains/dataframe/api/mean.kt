package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.hasNulls
import org.jetbrains.dataframe.columns.typeClass
import java.math.BigDecimal
import kotlin.reflect.KClass
import kotlin.reflect.KProperty


inline fun <T, reified D : Number> DataFrame<T>.meanOf(skipNa: Boolean = true, crossinline selector: RowSelector<T, D?>): Double {
    val values = asSequence().map { selector(it, it) }
    return values.mean(D::class, skipNa)
}

fun <T> DataFrame<T>.mean(skipNa: Boolean = true): DataRow<T> = aggregateColumns<T, Number?> { it.mean(skipNa) }

fun <T> DataFrame<T>.mean(column: String, skipNa: Boolean = true): Double = getColumn<Number?>(column).mean(skipNa)
fun <T, C : Number> DataFrame<T>.mean(col: ColumnReference<C?>, skipNa: Boolean = true): Double = get(col).mean(skipNa)
fun <T, C : Number> DataFrame<T>.mean(col: KProperty<C?>, skipNa: Boolean = true): Double = get(col).mean(skipNa)
fun <T, C: Number> DataFrame<T>.mean(skipNa: Boolean = true, selector: ColumnSelector<T, C?>): Double = this[selector].mean(skipNa)

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
        else -> throw IllegalArgumentException()
    }
}

// TODO: remove
fun <T : Number> DataColumn<T?>.mean(skipNa: Boolean = true): Double {
    if(size() == 0) return Double.NaN
    if(hasNulls) {
        return if(skipNa) {
            when(typeClass) {
                Double::class -> (asSequence() as Sequence<Double?>).filterNotNull().mean(skipNa)
                Float::class -> (asSequence() as Sequence<Float?>).filterNotNull().mean(skipNa)
                Int::class -> (asSequence() as Sequence<Int?>).filterNotNull().asIterable().mean()
                Short::class -> (asSequence() as Sequence<Short?>).filterNotNull().asIterable().mean()
                Byte::class -> (asSequence() as Sequence<Byte?>).filterNotNull().asIterable().mean()
                Long::class -> (asSequence() as Sequence<Long?>).filterNotNull().asIterable().mean()
                BigDecimal::class -> (asSequence() as Sequence<BigDecimal?>).filterNotNull().asIterable().mean()
                else -> throw IllegalArgumentException()
            }
        } else Double.NaN
    } else if(skipNa){
        when(typeClass) {
            Double::class -> return (asSequence() as Sequence<Double>).mean(skipNa)
            Float::class -> return (asSequence() as Sequence<Float>).mean(skipNa)
        }
    }
    return (asIterable() as Iterable<T>).mean(typeClass, skipNa)
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

