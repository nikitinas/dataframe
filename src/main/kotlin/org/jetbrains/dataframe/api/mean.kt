package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.hasNulls
import org.jetbrains.dataframe.columns.name
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

inline fun <T, G, reified R : Number> GroupedDataFrame<T, G>.meanOf(
    columnName: String = "mean", skipNa: Boolean = true,
    noinline selector: RowSelector<G, R?>
) = aggregate { meanOf(skipNa, selector) into columnName }

inline fun <T, G, reified R : Number> GroupedDataFrame<T, G>.mean(
    skipNa: Boolean = true,
    noinline selector: ColumnSelector<G, R?>
) = aggregate {
    val col = this[selector]
    col.mean(skipNa) into col.name()
}

fun <T, G> GroupedDataFrame<T, G>.mean(): DataFrame<T> {

    val keyColumnNames = keys.columnNames().toSet()
    return aggregate {
        columns().filter { it.isSubtypeOf<Number?>() && !keyColumnNames.contains(it.name) }
            .forEach { col ->
                if(!keyColumnNames.contains(col.name) && col.isNumber())
                (col as DataColumn<Number?>).mean() into col.name()
            }
    }
}

fun <T : Number> Sequence<T?>.mean(clazz: KClass<T>, skipNa: Boolean = true): Double {

    return if (skipNa) {
        when (clazz) {
            Double::class -> ((this as Sequence<Double?>).filter { it != null && !it.isNaN() } as Sequence<Double>).mean()
            Float::class -> ((this as Sequence<Float?>).filter { it != null && !it.isNaN() } as Sequence<Float>).mean()
            Int::class -> (this as Sequence<Int?>).filterNotNull().asIterable().mean()
            Long::class -> (this as Sequence<Long?>).filterNotNull().asIterable().mean()
            Byte::class -> (this as Sequence<Byte?>).filterNotNull().asIterable().mean()
            Short::class -> (this as Sequence<Short?>).filterNotNull().asIterable().mean()
            BigDecimal::class -> (this as Sequence<BigDecimal?>).filterNotNull().asIterable().mean()
            else -> throw IllegalArgumentException()
        }
    } else when (clazz) {
        Double::class -> (this as Sequence<Double?>).map { it ?: Double.NaN }.mean()
        Float::class -> (this as Sequence<Float?>).map { it?.toDouble() ?: Double.NaN }.mean()
        Int::class -> (this as Sequence<Int?>).map { it?.toDouble() ?: Double.NaN }.mean()
        Long::class -> (this as Sequence<Long?>).map { it?.toDouble() ?: Double.NaN }.mean()
        Byte::class -> (this as Sequence<Byte?>).map { it?.toDouble() ?: Double.NaN }.mean()
        Short::class -> (this as Sequence<Short?>).map { it?.toDouble() ?: Double.NaN }.mean()
        BigDecimal::class -> (this as Sequence<BigDecimal?>).map { it?.toDouble() ?: Double.NaN }.mean()
        else -> throw IllegalArgumentException()
    }
}

fun <T : Number> DataColumn<T?>.mean(skipNa: Boolean = true): Double {
    if(size() == 0) return Double.NaN
    if(hasNulls) {
        return if(skipNa) {
            when(typeClass) {
                Double::class -> ((asSequence() as Sequence<Double?>).filter { it != null && !it.isNaN() } as Sequence<Double>).mean()
                Float::class -> ((asSequence() as Sequence<Float?>).filter { it != null && !it.isNaN() } as Sequence<Float>).mean()
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
            Double::class -> return (asSequence() as Sequence<Double>).filter { !it.isNaN() }.asIterable().mean()
            Float::class -> return (asSequence() as Sequence<Float>).filter { !it.isNaN() }.asIterable().mean()
        }
    }
    return when(typeClass) {
        Double::class -> (asIterable() as Iterable<Double>).mean()
        Float::class -> (asIterable() as Iterable<Float>).mean()
        Int::class -> (asIterable() as Iterable<Int>).mean()
        Short::class -> (asIterable() as Iterable<Short>).mean()
        Byte::class -> (asIterable() as Iterable<Byte>).mean()
        Long::class -> (asIterable() as Iterable<Long>).mean()
        BigDecimal::class -> (asIterable() as Iterable<BigDecimal>).mean()
        else -> throw IllegalArgumentException()
    }
}

fun Sequence<Double>.mean(): Double {
    var count = 0
    val sum = sumOf { count++; it }
    return if(count > 0) sum / count else Double.NaN
}

@JvmName("meanFloat")
fun Sequence<Float>.mean(): Double {
    var count = 0
    val sum = sumOf { count++; it.toDouble() }
    return if(count > 0) sum / count else Double.NaN
}

@JvmName("doubleMean")
fun Iterable<Double>.mean(): Double =
    if (this is Collection) {
        if(size > 0) sum() / size else Double.NaN
    } else asSequence().mean()

@JvmName("floatMean")
fun Iterable<Float>.mean(): Double =
    if (this is Collection) {
        if(size > 0) sumOf { it.toDouble() } / size else Double.NaN
    } else asSequence().mean()

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

