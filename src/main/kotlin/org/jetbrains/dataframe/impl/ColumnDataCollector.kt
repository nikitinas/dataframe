package org.jetbrains.dataframe.impl

import org.jetbrains.dataframe.*
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.guessColumnType
import kotlin.reflect.KClass
import kotlin.reflect.KType
import kotlin.reflect.full.isSubclassOf
import kotlin.reflect.full.withNullability

public interface DataCollector<T> {

    public fun add(value: T)
    public val data: List<T?>
    public val hasNulls: Boolean
    public fun toColumn(name: String): DataColumn<T>
}

internal abstract class DataCollectorBase<T>(initCapacity: Int) : DataCollector<T> {

    override var hasNulls = false

    override val data = ArrayList<T?>(initCapacity)

    val values: List<T?>
        get() = data

    override fun add(value: T) {
        if (value == null) hasNulls = true
        data.add(value)
    }

    protected fun createColumn(name: String, type: KType): DataColumn<T> {
        val classifier = type.classifier as KClass<*>
        if (classifier.isSubclassOf(DataFrame::class)) {
            return DataColumn.create(name, data as List<AnyFrame>) as DataColumn<T>
        }
        if (classifier.isSubclassOf(DataRow::class)) {
            val mergedDf = (data as List<AnyRow>).map { it.toDataFrame() }.union()
            return DataColumn.create(name, mergedDf) as DataColumn<T>
        }
        return DataColumn.create(name, data, type.withNullability(hasNulls)) as DataColumn<T>
    }
}

internal open class ColumnDataCollector(initCapacity: Int = 0, val getType: (KClass<*>) -> KType) : DataCollectorBase<Any?>(initCapacity) {

    override fun toColumn(name: String) = guessColumnType(name, values)
}

internal class TypedColumnDataCollector<T>(initCapacity: Int = 0, val type: KType) : DataCollectorBase<T?>(initCapacity) {

    override fun toColumn(name: String) = createColumn(name, type)
}

internal fun createDataCollector(initCapacity: Int = 0) = createDataCollector(initCapacity) { it.createStarProjectedType(false) }

internal fun createDataCollector(initCapacity: Int = 0, getType: (KClass<*>) -> KType) = ColumnDataCollector(initCapacity, getType)

internal fun <T> createDataCollector(initCapacity: Int = 0, type: KType) = TypedColumnDataCollector<T>(initCapacity, type)
