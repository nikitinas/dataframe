package org.jetbrains.dataframe.columns

import org.jetbrains.dataframe.AnyCol
import org.jetbrains.dataframe.AnyRow
import org.jetbrains.dataframe.ColumnResolutionContext
import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.DataRow
import org.jetbrains.dataframe.getType
import org.jetbrains.dataframe.union
import org.jetbrains.dataframe.impl.columns.FrameColumnImpl
import org.jetbrains.dataframe.impl.columns.MapColumnImpl
import org.jetbrains.dataframe.impl.columns.ValueImplColumn
import org.jetbrains.dataframe.impl.columns.addPath
import org.jetbrains.dataframe.internal.schema.DataFrameSchema
import org.jetbrains.dataframe.toDataFrame
import kotlin.reflect.KClass
import kotlin.reflect.KProperty
import kotlin.reflect.KType
import kotlin.reflect.full.isSubclassOf

/**
 * Column with type, name/path and values
 * Base interface only for [ValueColumn] and [FrameColumn]
 *
 * All column extension functions that clash with [DataFrame] API (such as filter, forEach, map etc.) are defined for this interface,
 * because [MapColumn] doesn't inherit from it
 */
interface DataColumn<out T> : Column<T> {

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

    fun hasNulls(): Boolean = type().isMarkedNullable

    override fun distinct(): DataColumn<T>

    override fun slice(range: IntRange): DataColumn<T>

    override fun slice(indices: Iterable<Int>): DataColumn<T>

    override fun slice(mask: BooleanArray): DataColumn<T>

    override fun rename(newName: String): DataColumn<T>

    override fun resolveSingle(context: ColumnResolutionContext): ColumnWithPath<T>? = this.addPath(context.df)

    override operator fun getValue(thisRef: Any?, property: KProperty<*>): DataColumn<T> = super.getValue(thisRef, property) as DataColumn<T>
}

internal val AnyCol.type get() = type()
internal val AnyCol.hasNulls get() = hasNulls()
internal val AnyCol.valueClass get() = type.classifier as KClass<*>

infix fun <T, C: Column<T>> C.named(name: String) = rename(name) as C


