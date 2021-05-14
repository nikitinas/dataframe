package org.jetbrains.dataframe.impl.columns

import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.dataFrameOf
import kotlin.reflect.KType

internal abstract class DataColumnImpl<T>(protected val values: List<T>, val name: String, val type: KType, distinct: Lazy<Set<T>>? = null) : DataColumn<T>, DataColumnInternal<T> {

    protected val distinct = distinct ?: lazy { values.toSet() }

    override fun name() = name

    override fun values() = values

    override fun type() = type

    override fun toSet() = distinct.value

    fun contains(value: T) = toSet().contains(value)

    override fun toString() = dataFrameOf(this).toString() // "${name()}: $type"

    override fun ndistinct() = toSet().size

    override fun get(index: Int) = values[index]

    override fun get(columnName: String) = throw UnsupportedOperationException()

    override fun size() = values.size

    override fun equals(other: Any?) = checkEquals(other)

    override fun hashCode() = getHashCode()

    override fun slice(indices: Iterable<Int>): DataColumn<T> {
        var nullable = false
        val newValues = indices.map {
            val value = values[it]
            if(value == null) nullable = true
            value
        }
        return createWithValues(newValues, nullable)
    }

    override fun slice(mask: BooleanArray): DataColumn<T> {
        val res = ArrayList<T?>(values.size)
        var hasNulls = false
        for(index in 0 until values.size) {
            if(mask[index]) {
                val value = this[index]
                if(!hasNulls && value == null) hasNulls = true
                res.add(value)
            }
        }
        return createWithValues(res as List<T>, hasNulls)
    }

    override fun slice(range: IntRange) = createWithValues(values.subList(range.start, range.endInclusive + 1))

    protected abstract fun createWithValues(values: List<T>, hasNulls: Boolean? = null): DataColumn<T>
}