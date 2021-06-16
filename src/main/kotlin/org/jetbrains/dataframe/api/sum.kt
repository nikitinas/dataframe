package org.jetbrains.dataframe

import org.jetbrains.dataframe.aggregation.DataFrameAggregations
import org.jetbrains.dataframe.aggregation.GroupByAggregations
import org.jetbrains.dataframe.aggregation.PivotAggregations
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.typeClass
import org.jetbrains.dataframe.columns.values
import org.jetbrains.dataframe.impl.aggregation.aggregators.Aggregators
import org.jetbrains.dataframe.impl.aggregation.modes.aggregateAll
import org.jetbrains.dataframe.impl.aggregation.modes.of
import org.jetbrains.dataframe.impl.zero
import java.math.BigDecimal
import kotlin.reflect.KClass

fun <T : Number> DataColumn<T>.sum() = values.sum(typeClass)

inline fun <T, reified R: Number> DataFrameAggregations<T>.sum(noinline columns: ColumnsSelector<T, R>): R =
    Aggregators.sum.aggregateAll(this, columns) ?: R::class.zero()

inline fun <T, reified R : Number> DataFrameAggregations<T>.sumOf(crossinline selector: RowSelector<T, R>): R =
    rows().sumOf(R::class) { selector(it, it) }

inline fun <T, reified R : Number> PivotAggregations<T>.sumOf(crossinline selector: RowSelector<T, R>) =
    Aggregators.sum.of(this, selector)

inline fun <T, reified R : Number> DataColumn<T>.sumOf(crossinline expression: (T) -> R): R? =
    Aggregators.sum.cast<R>().of(this, expression)

inline fun <T, reified R : Number> GroupByAggregations<T>.sumOf(
    resultName: String? = null,
    crossinline selector: RowSelector<T, R>
): DataFrame<T> = Aggregators.sum.of(resultName, this, selector)

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