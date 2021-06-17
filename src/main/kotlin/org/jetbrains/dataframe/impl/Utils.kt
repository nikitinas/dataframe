package org.jetbrains.dataframe.impl

import org.jetbrains.dataframe.ColumnPath
import org.jetbrains.dataframe.Predicate
import org.jetbrains.dataframe.Selector
import java.math.BigDecimal
import java.math.BigInteger
import kotlin.reflect.KClass

internal infix fun <T> (Predicate<T>).and(other: Predicate<T>): Predicate<T> = { this(it) && other(it) }

internal fun <T> T.toIterable(getNext: (T) -> T?) = Iterable<T> {

    object : Iterator<T> {

        var current: T? = null
        var beforeStart = true
        var next: T? = null

        override fun hasNext(): Boolean {
            if (beforeStart) return true
            if (next == null) next = getNext(current!!)
            return next != null
        }

        override fun next(): T {
            if (beforeStart) {
                current = this@toIterable
                beforeStart = false
                return current!!
            }
            current = next ?: getNext(current!!)
            next = null
            return current!!
        }
    }
}

internal fun <T> List<T>.removeAt(index: Int) = subList(0, index) + subList(index + 1, size)

internal inline fun <reified T: Any> Int.cast() = convert(this, T::class)

internal fun <T: Any> convert(src:Int, targetType: KClass<T>): T = when(targetType) {
    Double::class -> src.toDouble() as T
    Long::class -> src.toLong() as T
    Float::class -> src.toFloat() as T
    BigDecimal::class -> src.toBigDecimal() as T
    else -> throw NotImplementedError("Casting int to ${targetType} is not supported")
}

internal fun BooleanArray.toIndices(): List<Int> {
    val res = ArrayList<Int>(size)
    for(i in 0 until size)
        if(this[i]) res.add(i)
    return res
}

internal fun <T> Iterable<T>.equalsByElement(other: Iterable<T>): Boolean {
    val iterator1 = iterator()
    val iterator2 = other.iterator()
    while (iterator1.hasNext() && iterator2.hasNext()) {
        if (iterator1.next() != iterator2.next()) return false
    }
    if (iterator1.hasNext() || iterator2.hasNext()) return false
    return true
}

internal fun <T> Iterable<T>.rollingHash(): Int {
    val i = iterator()
    var hash = 0
    while (i.hasNext())
        hash = 31 * hash + (i.next()?.hashCode() ?: 5)
    return hash
}

fun <T> Iterable<T>.asList() = when (this) {
    is List<T> -> this
    else -> this.toList()
}

internal fun <T> Iterable<T>.anyNull() = any { it == null }

@PublishedApi
internal fun emptyPath(): ColumnPath = emptyList()

@PublishedApi
internal fun pathOf(name: String): ColumnPath = listOf(name)

@PublishedApi
internal fun <T:Number> KClass<T>.zero(): T = when(this){
    Int::class -> 0 as T
    Byte::class -> 0.toByte() as T
    Short::class -> 0.toShort() as T
    Long::class -> 0.toLong() as T
    Double::class -> 0.toDouble() as T
    Float::class -> 0.toFloat() as T
    BigDecimal::class -> BigDecimal.ZERO as T
    BigInteger::class -> BigInteger.ZERO as T
    Number::class -> 0 as T
    else -> TODO()
}