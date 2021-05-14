package org.jetbrains.dataframe.impl.columns

import org.jetbrains.dataframe.*
import org.jetbrains.dataframe.columns.MapColumn
import org.jetbrains.dataframe.createType
import org.jetbrains.dataframe.impl.renderSchema
import java.lang.UnsupportedOperationException
import kotlin.reflect.KType

internal val mapColumnType = createType<AnyRow>()

internal class MapColumnImpl<T>(override val df: DataFrame<T>, val name: String) : MapColumn<T>, DataColumnInternal<DataRow<T>>, DataFrame<T> by df {

    override fun values() = df.rows()

    override fun ndistinct() = distinct.nrow()

    override fun type() = mapColumnType

    private val distinct by lazy { df.distinct() }

    private val set by lazy { distinct.rows().toSet() }

    override fun toSet() = set

    override fun size() = df.nrow()

    override fun get(index: Int) = df[index]

    override fun slice(range: IntRange) = MapColumnImpl(df[range], name)

    override fun rename(newName: String) = MapColumnImpl(df, newName)

    override fun defaultValue() = null

    override fun slice(indices: Iterable<Int>) = withDf(df[indices])

    override fun slice(mask: BooleanArray) = withDf(df.getRows(mask))

    override fun addParent(parent: MapColumn<*>) = MapColumnWithParent(parent, this)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        val g = other as? MapColumn<*> ?: return false
        return name == g.name() && df == other.df
    }

    override fun hashCode(): Int {
        return name.hashCode() * 31 + df.hashCode()
    }

    override fun tryGetColumn(columnName: String) = df.tryGetColumn(columnName)

    override fun toString() = "$name: {${renderSchema(df)}}"

    override fun changeType(type: KType) = throw UnsupportedOperationException()

    override fun name() = name
}