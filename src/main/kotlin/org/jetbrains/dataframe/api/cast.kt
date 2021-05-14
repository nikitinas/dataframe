package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.size
import org.jetbrains.dataframe.columns.hasNulls
import org.jetbrains.dataframe.columns.name
import org.jetbrains.dataframe.columns.type
import org.jetbrains.dataframe.columns.values
import org.jetbrains.dataframe.impl.columns.DataColumnInternal
import org.jetbrains.dataframe.impl.columns.toColumns
import java.math.BigDecimal
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatterBuilder
import kotlin.reflect.KClass
import kotlin.reflect.KProperty
import kotlin.reflect.KType
import kotlin.reflect.full.isSubtypeOf
import kotlin.reflect.full.withNullability

fun <T> DataFrame<T>.cast(selector: ColumnsSelector<T, *>) = CastClause(this, selector)
fun <T> DataFrame<T>.cast(vararg columns: KProperty<*>) = cast { columns.toColumns() }
fun <T> DataFrame<T>.cast(vararg columns: String) = cast { columns.toColumns() }
fun <T> DataFrame<T>.cast(vararg columns: Column) = cast { columns.toColumns() }

data class CastClause<T>(val df: DataFrame<T>, val selector: ColumnsSelector<T, *>) {
    inline fun <reified C> to() = to(getType<C>())
}

fun <T> CastClause<T>.to(type: KType) = df.replace(selector).with { it.castTo(type) }

inline fun <reified C> AnyCol.cast(): DataColumn<C> = castTo(getType<C>()) as DataColumn<C>

internal val convertersCache = mutableMapOf<Pair<KType, KType>, TypeConverter?>()

fun AnyCol.castTo(newType: KType): AnyCol {
    val from = type
    if (from == newType) return this
    if (!from.isSubtypeOf(newType)) {
        val converter = convertersCache.getOrPut(from to newType) { createConverter(from, newType) }
        if(converter != null) {
            var hasNulls = hasNulls
            val values = values.map { if (it == null) null else converter(it).also { if (it == null) hasNulls = true } }
            return DataColumn.create(name, values, newType.withNullability(hasNulls))
        }
    }
    return (this as DataColumnInternal<*>).changeType(newType.withNullability(hasNulls))
}

internal typealias TypeConverter = (Any) -> Any?

internal inline fun <T> convert(crossinline converter: (T)->Any?): TypeConverter = { converter(it as T) }

internal fun createConverter(from: KType, to: KType): TypeConverter? {
    if (from.arguments.isNotEmpty() || to.arguments.isNotEmpty()) return null
    if(from.isMarkedNullable) {
        val res = createConverter(from.withNullability(false), to) ?: return null
        return { if (it == null) null else res(it) }
    }
    val fromClass = from.classifier as KClass<*>
    val toClass = to.classifier as KClass<*>

    if(fromClass == toClass) return { it }

    return when {
        fromClass == String::class -> Parsers[to]?.toConverter()
        toClass == String::class -> convert<Any> { it.toString() }
        fromClass == Number::class -> when (toClass) {
            Double::class -> convert<Number> { it.toDouble() }
            Int::class -> convert<Number> { it.toInt() }
            Float::class -> convert<Number> { it.toFloat() }
            Byte::class -> convert<Number> { it.toByte() }
            Short::class -> convert<Number> { it.toShort() }
            Long::class -> convert<Number> { it.toLong() }
            else -> null
        }
        fromClass == Int::class -> when (toClass) {
            Double::class -> convert<Int> { it.toDouble() }
            Float::class -> convert<Int> { it.toFloat() }
            Byte::class -> convert<Int> { it.toByte() }
            Short::class -> convert<Int> { it.toShort() }
            Long::class -> convert<Int> { it.toLong() }
            BigDecimal::class -> convert<Int> { it.toBigDecimal() }
            else -> null
        }
        fromClass == Double::class -> when(toClass){
            Int::class -> convert<Double> { it.toInt() }
            Float::class -> convert<Double> { it.toFloat() }
            Long::class -> convert<Double> { it.toLong() }
            BigDecimal::class -> convert<Double> { it.toBigDecimal() }
            else -> null
        }
        fromClass == Long::class -> when (toClass) {
            Double::class -> convert<Long> { it.toDouble() }
            Float::class -> convert<Long> { it.toFloat() }
            Byte::class -> convert<Long> { it.toByte() }
            Short::class -> convert<Long> { it.toShort() }
            Int::class -> convert<Long> { it.toInt() }
            BigDecimal::class -> convert<Long> { it.toBigDecimal() }
            else -> null
        }
        fromClass == Float::class -> when (toClass) {
            Double::class -> convert<Float> { it.toDouble() }
            Long::class -> convert<Float> { it.toLong() }
            Int::class -> convert<Float> { it.toInt() }
            BigDecimal::class -> convert<Float> { it.toBigDecimal() }
            else -> null
        }
        else -> null
    }
}

fun <T> CastClause<T>.toInt() = to<Int>()
fun <T> CastClause<T>.toDouble() = to<Double>()
fun <T> CastClause<T>.toFloat() = to<Float>()
fun <T> CastClause<T>.toStr() = to<String>()
fun <T> CastClause<T>.toLong() = to<Long>()
fun <T> CastClause<T>.toBigDecimal() = to<BigDecimal>()
fun <T> CastClause<T>.toDate() = to<LocalDate>()
fun <T> CastClause<T>.toTime() = to<LocalTime>()
fun <T> CastClause<T>.toDateTime() = to<LocalDateTime>()

internal class StringParser<T : Any>(val type: KType, val parse: (String) -> T?) {
    fun toConverter(): TypeConverter = { parse(it as String) }
}

internal object Parsers {

    private val formatterDateTime = DateTimeFormatterBuilder()
        .parseCaseInsensitive()
        .append(DateTimeFormatter.ISO_LOCAL_DATE)
        .appendLiteral(' ')
        .append(DateTimeFormatter.ISO_LOCAL_TIME).toFormatter()

    private val formatters = listOf(
        DateTimeFormatter.ISO_LOCAL_DATE_TIME,
        formatterDateTime
    )

    private fun String.toLocalDateTimeOrNull(): LocalDateTime? {
        var ret: LocalDateTime? = null
        for (format in formatters) {
            try {
                ret = LocalDateTime.parse(this, format)
                return ret
            } catch (_: Throwable) {}
        }
        return ret
    }

    private fun String.toBooleanOrNull() =
        when (toUpperCase()) {
            "T" -> true
            "TRUE" -> true
            "YES" -> true
            "F" -> false
            "FALSE" -> false
            "NO" -> false
            else -> null
        }

    private fun String.toLocalDateOrNull(): LocalDate? =
        try { LocalDate.parse(this) } catch (_: Throwable) { null }

    private fun String.toLocalTimeOrNull(): LocalTime? =
        try { LocalTime.parse(this) } catch (_: Throwable) { null }

    private fun String.parseDouble() =
        when (toUpperCase()) {
            "NAN" -> Double.NaN
            "INF" -> Double.POSITIVE_INFINITY
            "-INF" -> Double.NEGATIVE_INFINITY
            "INFINITY" -> Double.POSITIVE_INFINITY
            "-INFINITY" -> Double.NEGATIVE_INFINITY
            else -> toDoubleOrNull()
        }

    inline fun <reified T : Any> stringParser(noinline body: (String) -> T?) = StringParser(getType<T>(), body)

    val All = listOf(
        stringParser { it.toIntOrNull() },
        stringParser { it.toLongOrNull() },
        stringParser { it.parseDouble() },
        stringParser { it.toBooleanOrNull() },
        stringParser { it.toBigDecimalOrNull() },
        stringParser { it.toLocalDateOrNull() },
        stringParser { it.toLocalTimeOrNull() },
        stringParser { it.toLocalDateTimeOrNull() }
    )

    private val parsersMap = All.associateBy { it.type }

    val size: Int = All.size

    operator fun get(index: Int): StringParser<*> = All[index]

    operator fun get(type: KType): StringParser<*>? = parsersMap.get(type)

    operator fun <T: Any> get(type: KClass<T>): StringParser<*>? = parsersMap.get(type.createStarProjectedType(false))

    inline fun <reified T : Any> get(): StringParser<T>? = get(getType<T>()) as? StringParser<T>
}

internal fun <T : Any> DataColumn<String?>.parse(parser: StringParser<T>): DataColumn<T?> {
    val parsedValues = values.map {
        it?.let {
            parser.parse(it) ?: throw Exception("Couldn't parse '${it}' to type ${parser.type}")
        }
    }
    return DataColumn.create(name(), parsedValues, parser.type.withNullability(hasNulls)) as DataColumn<T?>
}

internal fun DataColumn<String?>.tryParseAny(): DataColumn<*> {

    if(allNulls()) return this

    var parserId = 0
    val parsedValues = mutableListOf<Any?>()

    do {
        val parser = Parsers[parserId]
        parsedValues.clear()
        for (str in values) {
            if (str == null) parsedValues.add(null)
            else {
                val res = parser.parse(str)
                if (res == null) {
                    parserId++
                    break
                }
                parsedValues.add(res)
            }
        }
    } while (parserId < Parsers.size && parsedValues.size != size)
    if (parserId == Parsers.size) return this
    return DataColumn.create(name(), parsedValues, Parsers[parserId].type.withNullability(hasNulls))
}