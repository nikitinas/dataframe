package org.jetbrains.dataframe

import org.jetbrains.dataframe.annotations.ColumnName
import org.jetbrains.dataframe.columns.AnyCol
import org.jetbrains.dataframe.columns.AnyColumn
import org.jetbrains.dataframe.columns.ColumnAccessor
import org.jetbrains.dataframe.impl.columns.ColumnAccessorImpl
import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.columns.Columns
import org.jetbrains.dataframe.columns.ColumnWithPath
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.BaseColumn
import org.jetbrains.dataframe.columns.FrameColumn
import org.jetbrains.dataframe.columns.ColumnGroup
import org.jetbrains.dataframe.columns.StringCol
import org.jetbrains.dataframe.columns.ValueColumn
import org.jetbrains.dataframe.columns.guessColumnType
import org.jetbrains.dataframe.columns.size
import org.jetbrains.dataframe.columns.type
import org.jetbrains.dataframe.columns.name
import org.jetbrains.dataframe.columns.typeClass
import org.jetbrains.dataframe.columns.values
import org.jetbrains.dataframe.impl.asList
import org.jetbrains.dataframe.impl.columns.ConvertedColumnDef
import kotlin.reflect.KClass
import kotlin.reflect.KProperty
import kotlin.reflect.KType
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.isSubtypeOf
import kotlin.reflect.full.starProjectedType
import kotlin.reflect.full.withNullability
import kotlin.reflect.typeOf

enum class UnresolvedColumnsPolicy { Fail, Skip, Create }

class ColumnResolutionContext(val df: DataFrame<*>, val unresolvedColumnsPolicy: UnresolvedColumnsPolicy) {

    val allowMissingColumns = unresolvedColumnsPolicy == UnresolvedColumnsPolicy.Skip
}

fun <TD, T : DataFrame<TD>, C> Selector<T, Columns<C>>.toColumns(createReceiver: (ColumnResolutionContext) -> T): Columns<C> =
    createColumnSet {
        val receiver = createReceiver(it)
        val columnSet = this(receiver, receiver)
        columnSet.resolve(receiver, it.unresolvedColumnsPolicy)
    }

fun <C> createColumnSet(resolver: (ColumnResolutionContext) -> List<ColumnWithPath<C>>): Columns<C> =
    object : Columns<C> {
        override fun resolve(context: ColumnResolutionContext) = resolver(context)
    }

inline fun <C, reified R> ColumnReference<C>.map(noinline transform: (C) -> R): ColumnReference<R> =
    map(getType<R>(), transform)

fun <C, R> ColumnReference<C>.map(targetType: KType?, transform: (C) -> R): ColumnReference<R> =
    ConvertedColumnDef(this, transform, targetType)

typealias Column = ColumnReference<*>

typealias MapColumnReference = ColumnReference<AnyRow>

fun String.toColumnDef(): ColumnAccessor<Any?> = ColumnAccessorImpl(this)

fun <T> String.toColumnOf(): ColumnAccessor<T> = ColumnAccessorImpl(this)

fun <T> ColumnPath.toColumnOf(): ColumnAccessor<T> = ColumnAccessorImpl(this)

fun ColumnPath.toColumnDef(): ColumnAccessor<Any?> = ColumnAccessorImpl(this)

fun ColumnPath.toGroupColumnDef(): ColumnAccessor<AnyRow> = ColumnAccessorImpl(this)

internal fun KProperty<*>.getColumnName() = this.findAnnotation<ColumnName>()?.name ?: name

fun <T> KProperty<T>.toColumnDef(): ColumnAccessor<T> = ColumnAccessorImpl<T>(name)

fun <T> ColumnAccessor<DataRow<*>>.subcolumn(childName: String): ColumnAccessor<T> =
    ColumnAccessorImpl(path() + childName)

inline fun <reified T> ColumnAccessor<T>.nullable() = changeType<T?>()

enum class ColumnKind {
    Value,
    Group,
    Frame
}

@OptIn(ExperimentalStdlibApi::class)
inline fun <reified T> getType() = typeOf<T>()

fun KClass<*>.createStarProjectedType(nullable: Boolean) =
    this.starProjectedType.let { if (nullable) it.withNullability(true) else it }

inline fun <reified T> ColumnReference<T>.withValues(vararg values: T) = withValues(values.asIterable())

inline fun <reified T> ColumnReference<T>.withValues(values: Iterable<T>) =
    DataColumn.create(name(), values.asList(), getType<T>())

fun AnyColumn.toDataFrame() = dataFrameOf(listOf(this))

fun <T, R> computeValues(df: DataFrame<T>, expression: AddExpression<T, R>): Pair<Boolean, List<R>> {
    var nullable = false
    val list = ArrayList<R>(df.nrow())
    df.indices().forEach {
        val row = AddDataRowImpl(it, df, list)
        val value = expression(row, row)
        if (value == null) nullable = true
        list.add(value)
    }
    return nullable to list
}

inline fun <T, reified R> DataFrameBase<T>.newColumn(name: String = "", noinline expression: AddExpression<T, R>): DataColumn<R> {
    val (nullable, values) = computeValues(this as DataFrame<T>, expression)
    if (R::class == DataFrame::class) return DataColumn.frames(name, values as List<AnyFrame?>) as DataColumn<R>
    return column(name, values, nullable)
}

fun <T, R> DataFrameBase<T>.newGuessColumn(name: String, expression: AddExpression<T, R>): DataColumn<R> {
    val (_, values) = computeValues(this as DataFrame<T>, expression)
    return guessColumnType(name, values) as DataColumn<R>
}

class ColumnDelegate<T>(private val parent: MapColumnReference? = null) {
    operator fun getValue(thisRef: Any?, property: KProperty<*>): ColumnAccessor<T> = named(property.name)

    infix fun named(name: String): ColumnAccessor<T> =
        parent?.let { ColumnAccessorImpl(it.path() + name) } ?: ColumnAccessorImpl(name)
}

fun AnyColumn.asFrame(): AnyFrame = when (this) {
    is ColumnGroup<*> -> df
    is ColumnWithPath<*> -> data.asFrame()
    else -> error("Can not extract DataFrame from ${javaClass.kotlin}")
}

fun AnyColumn.isGroup(): Boolean = kind() == ColumnKind.Group

fun <T> column() = ColumnDelegate<T>()

fun columnGroup() = column<AnyRow>()

fun columnGroup(parent: MapColumnReference) = column<AnyRow>(parent)

fun frameColumn() = column<AnyFrame>()

fun <T> columnList() = column<List<T>>()

fun <T> columnGroup(name: String) = column<DataRow<T>>(name)

fun <T> frameColumn(name: String) = column<DataFrame<T>>(name)

fun <T> columnList(name: String) = column<List<T>>(name)

fun <T> column(name: String): ColumnAccessor<T> = ColumnAccessorImpl(name)

fun <T> column(parent: MapColumnReference): ColumnDelegate<T> = ColumnDelegate(parent)

fun <T> column(parent: MapColumnReference, name: String): ColumnAccessor<T> =
    ColumnAccessorImpl(parent.path() + name)

inline fun <reified T> columnOf(vararg values: T): DataColumn<T> = column(values.asIterable())

fun columnOf(vararg values: AnyColumn): DataColumn<AnyRow> = columnOf(values.asIterable())

fun <T> columnOf(vararg frames: DataFrame<T>?): FrameColumn<T> = columnOf(frames.asIterable())

fun columnOf(columns: Iterable<AnyColumn>): DataColumn<AnyRow> = DataColumn.create("", dataFrameOf(columns)) as DataColumn<AnyRow>

fun <T> columnOf(frames: Iterable<DataFrame<T>?>): FrameColumn<T> = DataColumn.create("", frames.toList())

inline fun <reified T> column(values: Iterable<T>): DataColumn<T> = when {
    values.all { it is AnyCol } -> DataColumn.create("", (values as Iterable<AnyCol>).toDataFrame()) as DataColumn<T>
    values.all { it == null || it is AnyFrame} -> DataColumn.frames("", values.map { it as? AnyFrame }) as DataColumn<T>
    else -> DataColumn.create("", values.toList(), getType<T>())
}

fun <T> Iterable<DataFrame<T>?>.toFrameColumn(name: String): FrameColumn<T> =
    DataColumn.create(name, asList())

inline fun <reified T> Iterable<T>.toColumn(name: String = ""): ValueColumn<T> =
    asList().let { DataColumn.create(name, it, getType<T>().withNullability(it.any { it == null })) }

fun Iterable<Any?>.toColumnGuessType(name: String = ""): AnyCol =
    guessColumnType(name, asList())

inline fun <reified T> Iterable<T>.toColumn(ref: ColumnReference<T>): ValueColumn<T> =
    toColumn(ref.name())

fun Iterable<AnyFrame?>.toColumn() = columnOf(this)

fun Iterable<AnyFrame?>.toColumn(name: String) = DataColumn.create(name, toList())

inline fun <reified T> column(name: String, values: List<T>): DataColumn<T> = when {
    values.size > 0 && values.all { it is AnyCol } -> DataColumn.create(
        name,
        values.map { it as AnyCol }.toDataFrame()
    ) as DataColumn<T>
    else -> column(name, values, values.any { it == null })
}

inline fun <reified T> column(name: String, values: List<T>, hasNulls: Boolean): DataColumn<T> =
    DataColumn.create(name, values, getType<T>().withNullability(hasNulls))

fun <C> BaseColumn<C>.single() = values.single()

fun <T> FrameColumn<T>.toDefinition() = frameColumn<T>(name)
fun <T> ColumnGroup<T>.toDefinition() = columnGroup<T>(name)
fun <T> ValueColumn<T>.toDefinition() = column<T>(name)

operator fun AnyColumn.plus(other: AnyColumn) = dataFrameOf(listOf(this, other))

fun StringCol.len() = map { it?.length }
fun StringCol.lowercase() = map { it?.lowercase() }
fun StringCol.uppercase() = map { it?.uppercase() }

infix fun <T, C: ColumnReference<T>> C.named(name: String) = rename(name) as C

infix fun <T> DataColumn<T>.eq(value: T): BooleanArray = isMatching { it == value }
infix fun <T> DataColumn<T>.neq(value: T): BooleanArray = isMatching { it != value }

infix fun DataColumn<Int>.gt(value: Int): BooleanArray = isMatching { it > value }
infix fun DataColumn<Double>.gt(value: Double): BooleanArray = isMatching { it > value }
infix fun DataColumn<Float>.gt(value: Float): BooleanArray = isMatching { it > value }
infix fun DataColumn<String>.gt(value: String): BooleanArray = isMatching { it > value }

infix fun DataColumn<Int>.lt(value: Int): BooleanArray = isMatching { it < value }
infix fun DataColumn<Double>.lt(value: Double): BooleanArray = isMatching { it < value }
infix fun DataColumn<Float>.lt(value: Float): BooleanArray = isMatching { it < value }
infix fun DataColumn<String>.lt(value: String): BooleanArray = isMatching { it < value }

infix fun <T> DataColumn<T>.isMatching(predicate: Predicate<T>): BooleanArray = BooleanArray(size) {
    predicate(this[it])
}

fun <T> BaseColumn<T>.first() = get(0)
fun <T> BaseColumn<T>.firstOrNull() = if(size > 0) first() else null
fun <T> BaseColumn<T>.first(predicate: (T)->Boolean) = values.first(predicate)
fun <T> BaseColumn<T>.firstOrNull(predicate: (T)->Boolean) = values.firstOrNull(predicate)
fun <T> BaseColumn<T>.last() = get(size-1)
fun <T> DataColumn<T>.lastOrNull() = if(size > 0) last() else null

fun <C> DataColumn<C>.allNulls() = size == 0 || all { it == null }

internal fun KType.isSubtypeWithNullabilityOf(type: KType) = this.isSubtypeOf(type) && (!this.isMarkedNullable || type.isMarkedNullable)

fun AnyCol.hasElementsOfType(type: KType) = typeOfElement().isSubtypeWithNullabilityOf(type)

fun AnyCol.isSubtypeOf(type: KType) = this.type.isSubtypeOf(type) && (!this.type.isMarkedNullable || type.isMarkedNullable)

inline fun <reified T> AnyCol.hasElementsOfType() = hasElementsOfType(getType<T>())
inline fun <reified T> AnyCol.isSubtypeOf() = isSubtypeOf(getType<T>())
inline fun <reified T> AnyCol.isType() = type() == getType<T>()

fun AnyCol.isNumber() = hasElementsOfType<Number?>()
fun AnyCol.isMany() = typeClass == Many::class
fun AnyCol.typeOfElement(): KType =
    if(isMany()) type.arguments[0].type ?: getType<Any?>()
    else type

fun AnyCol.elementTypeIsNullable(): Boolean = typeOfElement().isMarkedNullable

fun AnyCol.isComparable() = isSubtypeOf<Comparable<*>>()

fun AnyCol.guessType() = DataColumn.create(name, toList())