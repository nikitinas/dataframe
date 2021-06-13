package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.AnyCol
import org.jetbrains.dataframe.columns.DataColumn
import java.math.BigDecimal

operator fun DataColumn<Int>.plus(value: Int) = map { it + value }
operator fun DataColumn<Int>.minus(value: Int) = map { it - value }
operator fun Int.plus(column: DataColumn<Int>) = column.map { this + it }
operator fun Int.minus(column: DataColumn<Int>) = column.map { this - it }
operator fun DataColumn<Int>.unaryMinus() = map { -it }
operator fun DataColumn<Int>.times(value: Int) = map {it * value }
operator fun DataColumn<Int>.div(value: Int) = map { it / value }
operator fun Int.div(column: DataColumn<Int>) = column.map { this / it }

operator fun String.plus(column: AnyCol) = column.map { this + it.toString() }
operator fun AnyCol.plus(str: String) = map { it.toString() + str }

@JvmName("plusInt?")
operator fun DataColumn<Int?>.plus(value: Int) = map { it?.plus(value) }
@JvmName("minusInt?")
operator fun DataColumn<Int?>.minus(value: Int) = map { it?.minus(value) }
@JvmName("plus?")
operator fun Int.plus(column: DataColumn<Int?>) = column.map { it?.plus(this) }
@JvmName("minus?")
operator fun Int.minus(column: DataColumn<Int?>) = column.map { it?.let { this - it } }
@JvmName("unaryMinusInt?")
operator fun DataColumn<Int?>.unaryMinus() = map { it?.unaryMinus() }
@JvmName("timesInt?")
operator fun DataColumn<Int?>.times(value: Int) = map { it?.times(value) }
@JvmName("divInt?")
operator fun DataColumn<Int?>.div(value: Int) = map { it?.div(value) }
@JvmName("div?")
operator fun Int.div(column: DataColumn<Int?>) = column.map { it?.let { this / it }}

@JvmName("plusInt")
operator fun DataColumn<Int>.plus(value: Double) = map { it + value }
@JvmName("minusInt")
operator fun DataColumn<Int>.minus(value: Double) = map { it - value }
@JvmName("doublePlus")
operator fun Double.plus(column: DataColumn<Int>) = column.map { this + it }
@JvmName("doubleMinus")
operator fun Double.minus(column: DataColumn<Int>) = column.map { this - it }
@JvmName("timesInt")
operator fun DataColumn<Int>.times(value: Double) = map { it * value }
@JvmName("divInt")
operator fun DataColumn<Int>.div(value: Double) = map { it / value }
@JvmName("doubleDiv")
operator fun Double.div(column: DataColumn<Int>) = column.map { this / it }

@JvmName("plusDouble")
operator fun DataColumn<Double>.plus(value: Int) = map { it + value }
@JvmName("minusDouble")
operator fun DataColumn<Double>.minus(value: Int) = map { it - value }
@JvmName("intPlus")
operator fun Int.plus(column: DataColumn<Double>) = column.map { this + it }
@JvmName("intMinus")
operator fun Int.minus(column: DataColumn<Double>) = column.map { this - it }
@JvmName("timesDouble")
operator fun DataColumn<Double>.times(value: Int) = map {it * value }
@JvmName("divDouble")
operator fun DataColumn<Double>.div(value: Int) = map { it / value }
@JvmName("intDiv")
operator fun Int.div(column: DataColumn<Double>) = column.map { this / it }

operator fun DataColumn<Double>.plus(value: Double) = map { it + value }
operator fun DataColumn<Double>.minus(value: Double) = map { it - value }
operator fun Double.plus(column: DataColumn<Double>) = column.map { this + it }
operator fun Double.minus(column: DataColumn<Double>) = column.map { this - it }
@JvmName("unaryMinusDouble")
operator fun DataColumn<Double>.unaryMinus() = map { -it }
operator fun DataColumn<Double>.times(value: Double) = map {it * value }
operator fun DataColumn<Double>.div(value: Double) = map { it / value }
operator fun Double.div(column: DataColumn<Double>) = column.map { this / it }

operator fun DataColumn<Long>.plus(value: Long) = map { it + value }
operator fun DataColumn<Long>.minus(value: Long) = map { it - value }
operator fun Long.plus(column: DataColumn<Long>) = column.map { this + it }
operator fun Long.minus(column: DataColumn<Long>) = column.map { this - it }
@JvmName("unaryMinusLong")
operator fun DataColumn<Long>.unaryMinus() = map { -it }
operator fun DataColumn<Long>.times(value: Long) = map {it * value }
operator fun DataColumn<Long>.div(value: Long) = map { it / value }
operator fun Long.div(column: DataColumn<Long>) = column.map { this / it }

operator fun DataColumn<BigDecimal>.plus(value: BigDecimal) = map { it + value }
operator fun DataColumn<BigDecimal>.minus(value: BigDecimal) = map { it - value }
operator fun BigDecimal.plus(column: DataColumn<BigDecimal>) = column.map { this + it }
operator fun BigDecimal.minus(column: DataColumn<BigDecimal>) = column.map { this - it }
@JvmName("unaryMinusBigDecimal")
operator fun DataColumn<BigDecimal>.unaryMinus() = map { -it }
operator fun DataColumn<BigDecimal>.times(value: BigDecimal) = map {it * value }
operator fun DataColumn<BigDecimal>.div(value: BigDecimal) = map { it / value }
operator fun BigDecimal.div(column: DataColumn<BigDecimal>) = column.map { this / it }