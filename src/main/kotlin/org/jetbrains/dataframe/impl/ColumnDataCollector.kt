package org.jetbrains.dataframe.impl

import org.jetbrains.dataframe.*
import org.jetbrains.dataframe.columns.AnyCol
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.commonParent
import java.lang.UnsupportedOperationException
import kotlin.reflect.KClass
import kotlin.reflect.KType
import kotlin.reflect.full.isSubclassOf
import kotlin.reflect.full.withNullability

interface DataCollector<T> {

    fun add(value: T)
    val hasNulls: Boolean
    fun toColumn(name: String): DataColumn<T>
}

internal abstract class DataCollectorBase<T>(initCapacity: Int): DataCollector<T> {

    override var hasNulls = false

    private val data = ArrayList<T?>(initCapacity)

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
        if(classifier.isSubclassOf(DataRow::class)) {
            val mergedDf = (data as List<AnyRow>).map { it.toDataFrame() }.union()
            return DataColumn.create(name, mergedDf) as DataColumn<T>
        }
        return DataColumn.create(name, data, type.withNullability(hasNulls)) as DataColumn<T>
    }
}

internal open class ColumnDataCollector(initCapacity: Int = 0, val getType: (KClass<*>) -> KType): DataCollectorBase<Any?>(initCapacity) {

    private val classes = mutableSetOf<KClass<*>>()

    private var hasRows : Boolean = false
    private var hasTables: Boolean = false

    protected fun commonClass() = classes.commonParent()

    override fun add(value: Any?) {
        super.add(value)
        if (value != null) {
            if(!hasRows && value is AnyRow) hasRows = true
            else if(!hasTables && value is AnyFrame) hasTables = true
            else classes.add(value.javaClass.kotlin)
        }
    }

    override fun toColumn(name: String): AnyCol {
        if(classes.isEmpty()){
            if(hasTables){
                val groups = values.map {
                    when(it){
                        null -> emptyDataFrame(1)
                        is AnyRow -> it.toDataFrame()
                        is AnyFrame -> it
                        else -> throw UnsupportedOperationException()
                    }
                }
                return DataColumn.create(name, groups)
            }else if(hasRows){
                val frames = values.map {
                    (it as AnyRow?)?.toDataFrame() ?: emptyDataFrame(1)
                }
                val merged = frames.union()
                return DataColumn.create(name, merged) as AnyCol
            }
        }
        return createColumn(name, getType(commonClass()).withNullability(hasNulls))
    }
}

internal class TypedColumnDataCollector<T>(initCapacity: Int = 0, val type: KType): DataCollectorBase<T?>(initCapacity) {

    override fun toColumn(name: String) = createColumn(name, type)
}

internal fun createDataCollector(initCapacity: Int = 0) = createDataCollector(initCapacity) { it.createStarProjectedType(false)}

internal fun createDataCollector(initCapacity: Int = 0, getType: (KClass<*>) -> KType) = ColumnDataCollector(initCapacity, getType)

internal fun <T> createDataCollector(initCapacity: Int = 0, type: KType) = TypedColumnDataCollector<T>(initCapacity, type)