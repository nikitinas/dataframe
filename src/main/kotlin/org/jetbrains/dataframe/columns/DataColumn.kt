package org.jetbrains.dataframe.columns

import org.jetbrains.dataframe.*
import org.jetbrains.dataframe.impl.asList
import org.jetbrains.dataframe.internal.schema.DataFrameSchema
import org.jetbrains.dataframe.impl.columns.MapColumnImpl
import org.jetbrains.dataframe.impl.columns.FrameColumnImpl
import org.jetbrains.dataframe.impl.columns.ValueImplColumn
import org.jetbrains.dataframe.impl.columns.addPath
import org.jetbrains.dataframe.impl.toIterable
import kotlin.reflect.KClass
import kotlin.reflect.KProperty
import kotlin.reflect.KType
import kotlin.reflect.full.isSubclassOf

interface DataColumn<out T> : ColumnReference<T>, ColumnProvider<T> {

    companion object {

        fun <T> create(name: String, values: List<T>, type: KType, defaultValue: T? = null): ValueColumn<T> = ValueImplColumn(values, name, type, defaultValue)

        fun <T> create(name: String, df: DataFrame<T>): MapColumn<T> = MapColumnImpl(df, name)

        fun <T> create(name: String, df: DataFrame<T>, startIndices: Sequence<Int>, emptyToNull: Boolean): FrameColumn<T> = FrameColumnImpl(name, df, startIndices, emptyToNull)

        fun <T> create(name: String, df: DataFrame<T>, startIndices: Iterable<Int>, emptyToNull: Boolean): FrameColumn<T> =
            create(name, df, startIndices.asSequence(), emptyToNull)

        fun <T> frames(name: String, groups: List<DataFrame<T>?>) = create(name, groups, null)

        internal fun <T> create(name: String, groups: List<DataFrame<T>?>, hasNulls: Boolean? = null, schema: Lazy<DataFrameSchema>? = null): FrameColumn<T> = FrameColumnImpl(name, groups, hasNulls, schema)

        internal fun <T> createGuess(name: String, values: List<T>, type:KType, defaultValue: T? = null): DataColumn<T> {
            val kClass = type.classifier!! as KClass<*>
            if(kClass.isSubclassOf(DataRow::class)){
                val df = values.map { (it as AnyRow).toDataFrame() }.union()
                return create(name, df) as DataColumn<T>
            }
            if(kClass.isSubclassOf(DataFrame::class)){
                return create(name, values as List<DataFrame<T>?>, null) as DataColumn<T>
            }
            return create(name, values, type, defaultValue)
        }

        fun empty() = create("", emptyList<Unit>(), getType<Unit>()) as AnyCol
    }

    fun type(): KType
    fun size(): Int
    fun hasNulls(): Boolean = type().isMarkedNullable
    fun ndistinct(): Int

    fun kind(): ColumnKind

    operator fun get(index: Int): T

    operator fun get(firstIndex: Int, vararg otherIndices: Int) = slice(headPlusIterable(firstIndex, otherIndices.asIterable()))

    operator fun get(row: AnyRow) = get(row.getIndex())

    fun values(): Iterable<T>

    fun toList() = values().asList()

    fun defaultValue(): T?

    fun slice(range: IntRange): DataColumn<T>

    operator fun get(columnName: String): AnyCol

    fun slice(indices: Iterable<Int>): DataColumn<T>

    fun slice(mask: BooleanArray): DataColumn<T>

    fun toSet(): Set<T>

    fun distinct(): DataColumn<T>

    operator fun set(predicate: Predicate<T>, value: Any) = {}

    override fun resolveSingle(context: ColumnResolutionContext): ColumnWithPath<T>? = this.addPath(context.df)

    override operator fun getValue(thisRef: Any?, property: KProperty<*>) = rename(property.name)
}

internal val <T> DataColumn<T>.values get() = values()
internal val AnyCol.ndistinct get() = ndistinct()
internal val AnyCol.type get() = type()
internal val AnyCol.size get() = size()
internal val AnyCol.hasNulls get() = hasNulls()
internal val AnyCol.valueClass get() = type.classifier as KClass<*>


