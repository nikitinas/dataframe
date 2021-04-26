package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.isSubtypeOf
import org.jetbrains.dataframe.impl.createDataCollector
import java.math.BigDecimal
import kotlin.reflect.KClass
import kotlin.reflect.KProperty
import kotlin.reflect.KType
import kotlin.reflect.jvm.jvmErasure


inline fun <reified T : Number> Iterable<T>.mean(): Double = mean(T::class)

inline fun <T, reified D : Number> DataFrame<T>.mean(crossinline selector: RowSelector<T, D?>): Double =
    rows().asSequence().map { selector(it, it) }.filterNotNull().asIterable().mean()

inline fun <T, reified D : Number> DataFrame<T>.mean(col: ColumnReference<D>): Double = get(col).mean()
inline fun <T, reified D : Number> DataFrame<T>.mean(col: KProperty<D>): Double = get(col).mean()
fun <T> DataFrame<T>.mean(column: String): Double = (get(column) as DataColumn<Number?>).mean()

inline fun <T, G, reified R : Number> GroupedDataFrame<T, G>.mean(
    columnName: String = "mean",
    noinline selector: RowSelector<G, R?>
) = aggregate { mean(selector) into columnName }

fun <T> DataFrame<T>.mean(): DataRow<T> = aggregateColumns<T, Number?> { it.mean() }

fun <T, G> GroupedDataFrame<T, G>.mean(): DataFrame<T> {

    val keyColumnNames = keys.columnNames().toSet()
    return aggregate {
        columns().filter { it.isSubtypeOf<Number?>() && !keyColumnNames.contains(it.name) }
            .forEach { col ->
                (col as DataColumn<Number?>).mean() into col.name()
            }
    }
}

fun <T : Number> Iterable<T>.mean(clazz: KClass<T>) = when (clazz) {
    Double::class -> (this as Iterable<Double>).mean()
    Int::class -> (this as Iterable<Int>).mean()
    Long::class -> (this as Iterable<Long>).mean()
    BigDecimal::class -> (this as Iterable<BigDecimal>).mean()
    else -> throw IllegalArgumentException()
}

fun <T : Number> DataColumn<T?>.mean(): Double =
    (if (hasNulls) values.filterNotNull() else (values as Iterable<T>)).mean(type.jvmErasure as KClass<T>)

@JvmName("doubleMean")
fun Iterable<Double>.mean(): Double {
    val counter = Counter()
    return computeSize(counter).sum() / counter.value.zeroToOne()
}

@JvmName("intMean")
fun Iterable<Int>.mean(): Double {
    val counter = Counter()
    return computeSize(counter).map { it.toDouble() }.sum() / counter.value.zeroToOne()
}

@JvmName("longMean")
fun Iterable<Long>.mean(): Double {
    val counter = Counter()
    return computeSize(counter).sum().toDouble() / counter.value.zeroToOne()
}

@JvmName("bigDecimalMean")
fun Iterable<BigDecimal>.mean(): Double {
    val counter = Counter()
    return computeSize(counter).sum().toDouble() / counter.value.zeroToOne()
}

internal fun Int.zeroToOne() = if (this == 0) 1 else this

internal class Counter(var value: Int = 0) {
    operator fun inc(): Counter {
        value++
        return this
    }
}

internal fun <T> Iterable<T>.computeSize(counter: Counter) = map {
    counter.inc()
    it
}

