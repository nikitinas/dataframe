package org.jetbrains.dataframe.columns

import org.jetbrains.dataframe.AnyFrame
import org.jetbrains.dataframe.AnyRow
import org.jetbrains.dataframe.api.DataColumnAggregations
import org.jetbrains.dataframe.ColumnResolutionContext
import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.DataRow
import org.jetbrains.dataframe.Many
import org.jetbrains.dataframe.commonType
import org.jetbrains.dataframe.createStarProjectedType
import org.jetbrains.dataframe.createTypeWithArgument
import org.jetbrains.dataframe.emptyMany
import org.jetbrains.dataframe.getType
import org.jetbrains.dataframe.impl.anyNull
import org.jetbrains.dataframe.union
import org.jetbrains.dataframe.impl.columns.FrameColumnImpl
import org.jetbrains.dataframe.impl.columns.ColumnGroupImpl
import org.jetbrains.dataframe.impl.columns.ValueColumnImpl
import org.jetbrains.dataframe.impl.columns.addPath
import org.jetbrains.dataframe.internal.schema.DataFrameSchema
import org.jetbrains.dataframe.manyOf
import org.jetbrains.dataframe.toDataFrame
import kotlin.reflect.KClass
import kotlin.reflect.KProperty
import kotlin.reflect.KType
import kotlin.reflect.full.isSubclassOf
import kotlin.reflect.full.withNullability

/**
 * Column with type, name/path and values
 * Base interface only for [ValueColumn] and [FrameColumn]
 *
 * All column extension functions that clash with [DataFrame] API (such as filter, forEach, map etc.) are defined for this interface,
 * because [ColumnGroup] doesn't inherit from it
 */
interface DataColumn<out T> : DataColumnAggregations<T> {

    companion object {

        fun <T> create(name: String, values: List<T>, type: KType, defaultValue: T? = null): ValueColumn<T> = ValueColumnImpl(values, name, type, defaultValue)

        fun <T> create(name: String, df: DataFrame<T>): ColumnGroup<T> = ColumnGroupImpl(df, name)

        fun <T> create(name: String, df: DataFrame<T>, startIndices: Sequence<Int>, emptyToNull: Boolean): FrameColumn<T> = FrameColumnImpl(name, df, startIndices, emptyToNull)

        fun <T> create(name: String, df: DataFrame<T>, startIndices: Iterable<Int>, emptyToNull: Boolean): FrameColumn<T> =
            create(name, df, startIndices.asSequence(), emptyToNull)

        fun <T> frames(name: String, groups: List<DataFrame<T>?>): FrameColumn<T> = create(name, groups, null)

        internal fun <T> create(name: String, groups: List<DataFrame<T>?>, hasNulls: Boolean? = null, schema: Lazy<DataFrameSchema>? = null): FrameColumn<T> = FrameColumnImpl(name, groups, hasNulls, schema)

        fun create(name: String, values: List<Any?>) = guessColumnType(name, values)

        fun <T> createWithNullCheck(name: String, values: List<T>, type: KType): ValueColumn<T> = create(name, values, type.withNullability(values.anyNull()))

        inline fun <reified T> createWithNullCheck(name: String, values: List<T>): ValueColumn<T> = createWithNullCheck(name, values, getType<T>())

        fun empty() = create("", emptyList<Unit>(), getType<Unit>()) as AnyCol
    }

    fun hasNulls(): Boolean = type().isMarkedNullable

    override fun distinct(): DataColumn<T>

    override fun slice(range: IntRange): DataColumn<T>

    override fun slice(indices: Iterable<Int>): DataColumn<T>

    override fun slice(mask: BooleanArray): DataColumn<T>

    override fun rename(newName: String): DataColumn<T>

    override fun resolveSingle(context: ColumnResolutionContext): ColumnWithPath<T>? = this.addPath(context.df)

    override operator fun getValue(thisRef: Any?, property: KProperty<*>) = super.getValue(thisRef, property) as DataColumn<T>

    operator fun iterator() = values().iterator()
}

typealias DoubleCol = DataColumn<Double?>
typealias BooleanCol = DataColumn<Boolean?>
typealias IntCol = DataColumn<Int?>
typealias NumberCol = DataColumn<Number?>
typealias StringCol = DataColumn<String?>
typealias AnyCol = DataColumn<*>

internal val AnyCol.type get() = type()
internal val AnyCol.hasNulls get() = hasNulls()
internal val AnyCol.typeClass get() = type.classifier as KClass<*>

internal fun guessValueType(values: Sequence<Any?>, upperBound: KType? = null): KType {
    val classes = mutableSetOf<KClass<*>>()
    var hasNulls = false
    var hasFrames = false
    var hasRows = false
    var hasMany = false
    val classesInMany = mutableSetOf<KClass<*>>()
    var nullsInMany = false
    values.forEach {
        when(it){
            null -> hasNulls = true
            is AnyRow -> hasRows = true
            is AnyFrame -> hasFrames = true
            is Many<*> -> {
                hasMany = true
                it.forEach {
                    if(it == null) nullsInMany = true
                    else classesInMany.add(it.javaClass.kotlin)
                }
            }
            else -> classes.add(it.javaClass.kotlin)
        }
    }
    val allManyWithRows = classesInMany.isNotEmpty() && classesInMany.all { it.isSubclassOf(DataRow::class) } && !nullsInMany
    return when {
        classes.isNotEmpty() -> {
            if(hasRows) classes.add(DataRow::class)
            if(hasFrames) classes.add(DataFrame::class)
            if(hasMany) {
                if(classesInMany.isNotEmpty()) {
                    val typeInLists = classesInMany.commonType(nullsInMany, upperBound)
                    val typeOfOthers = classes.commonType(nullsInMany, upperBound)
                    if(typeInLists == typeOfOthers){
                        return Many::class.createTypeWithArgument(typeInLists, false)
                    }
                }
                classes.add(Many::class)
            }
            return classes.commonType(hasNulls, upperBound)
        }
        (hasFrames && (!hasMany || allManyWithRows)) || (!hasFrames && allManyWithRows) -> DataFrame::class.createStarProjectedType(hasNulls)
        hasRows && !hasFrames && !hasMany -> DataRow::class.createStarProjectedType(false)
        hasMany && !hasFrames && !hasRows -> Many::class.createTypeWithArgument(classesInMany.commonType(nullsInMany, upperBound))
        else -> {
            if(hasRows) classes.add(DataRow::class)
            if(hasFrames) classes.add(DataFrame::class)
            if(hasMany) classes.add(Many::class)
            return classes.commonType(hasNulls, upperBound)
        }
    }
}

internal fun guessColumnType(name: String, values: List<Any?>) = guessColumnType(name, values, null)

internal fun guessColumnType(name: String, values: List<Any?>, suggestedType: KType? = null, suggestedTypeIsUpperBound: Boolean = false, defaultValue: Any? = null): AnyCol  {
    val type = when {
        suggestedType == null || suggestedTypeIsUpperBound -> guessValueType(values.asSequence(), suggestedType)
        else -> suggestedType
    }

    return when(type.classifier!! as KClass<*>) {
        DataRow::class -> {
            val df = values.map { (it as AnyRow).toDataFrame() }.union()
            DataColumn.create(name, df) as AnyCol
        }
        DataFrame::class -> {
            val frames = values.map {
                when(it) {
                    null -> null
                    is AnyFrame -> it
                    is AnyRow -> it.toDataFrame()
                    is List<*> -> (it as List<AnyRow>).toDataFrame()
                    else -> throw IllegalStateException()
                }
            }
            DataColumn.create(name, frames, type.isMarkedNullable)
        }
        Many::class -> {
            val nullable = type.isMarkedNullable
            val lists = values.map {
                when(it){
                    null -> if(nullable) null else emptyMany()
                    is Many<*> -> it
                    else -> manyOf(it)
                }
            }
            DataColumn.create(name, lists, type, defaultValue)
        }
        else -> DataColumn.create(name, values, type, defaultValue)
    }
}