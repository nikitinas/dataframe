package org.jetbrains.dataframe.columns

import org.jetbrains.dataframe.*
import org.jetbrains.dataframe.impl.asList
import org.jetbrains.dataframe.internal.schema.DataFrameSchema
import org.jetbrains.dataframe.impl.columns.MapColumnImpl
import org.jetbrains.dataframe.impl.columns.FrameColumnImpl
import org.jetbrains.dataframe.impl.columns.ValueImplColumn
import org.jetbrains.dataframe.impl.columns.addPath
import kotlin.reflect.KClass
import kotlin.reflect.KProperty
import kotlin.reflect.KType
import kotlin.reflect.full.isSubclassOf
import kotlin.reflect.full.isSubtypeOf
import kotlin.reflect.full.withNullability

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

    val values: Iterable<T>
    val ndistinct: Int
    val type: KType
    val hasNulls: Boolean get() = type.isMarkedNullable
    val size: Int

    fun kind(): ColumnKind

    operator fun get(index: Int): T

    operator fun get(row: AnyRow) = get(row.getIndex())

    fun values() = values

    fun toList() = values.asList()

    fun defaultValue(): T?

    fun slice(range: IntRange): DataColumn<T>

    operator fun get(columnName: String): AnyCol

    fun slice(indices: Iterable<Int>): DataColumn<T>

    fun slice(mask: BooleanArray): DataColumn<T>

    fun toSet(): Set<T>

    override fun resolveSingle(context: ColumnResolutionContext): ColumnWithPath<T>? = this.addPath(context.df)

    override operator fun getValue(thisRef: Any?, property: KProperty<*>) = rename(property.name)
}

fun <T> DataColumn<T>.first() = get(0)
fun <T> DataColumn<T>.firstOrNull() = if(size > 0) first() else null
fun <T> DataColumn<T>.first(predicate: (T)->Boolean) = values.first(predicate)
fun <T> DataColumn<T>.firstOrNull(predicate: (T)->Boolean) = values.firstOrNull(predicate)
fun <T> DataColumn<T>.last() = get(size-1)
fun <T> DataColumn<T>.lastOrNull() = if(size > 0) last() else null

val AnyCol.valueClass get() = type.classifier as KClass<*>

fun <C> DataColumn<C>.allNulls() = size == 0 || (hasNulls && ndistinct == 1)

fun <T> DataColumn<T>.isSubtypeOf(type: KType) = this.type.isSubtypeOf(type) && (!this.type.isMarkedNullable || type.isMarkedNullable)

inline fun <reified T> AnyCol.isSubtypeOf() = isSubtypeOf(getType<T>())

inline fun <reified T> AnyCol.isType() = type == getType<T>()
fun AnyCol.isNumber() = type.withNullability(false).isSubtypeOf(getType<Number>())