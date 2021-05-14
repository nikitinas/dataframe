package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.size
import org.jetbrains.dataframe.columns.hasNulls
import org.jetbrains.dataframe.columns.values
import org.jetbrains.dataframe.impl.columns.asGroup
import kotlin.reflect.KClass
import kotlin.reflect.KProperty
import kotlin.reflect.KType
import kotlin.reflect.full.isSubtypeOf

fun <T, C> DataFrame<T>.spread(column: KProperty<C>) = spread { column.toColumnDef() }
fun <T> DataFrame<T>.spread(column: String) = spread { column.toColumnDef() }
fun <T, C> DataFrame<T>.spread(column: ColumnReference<C>) = spread { column }
fun <T, C> DataFrame<T>.spread(selector: ColumnSelector<T, C>) =
    SpreadClause.inDataFrame(this, selector)

class DataFrameForSpreadImpl<T>(df: DataFrame<T>) : DataFrame<T> by df, DataFrameForSpread<T>

typealias SpreadColumnSelector<T, C> = DataFrameForSpread<T>.(DataFrameForSpread<T>) -> ColumnReference<C>

interface DataFrameForSpread<out T> : DataFrame<T>

interface SpreadContext {
    class DataFrame<T>(val df: org.jetbrains.dataframe.DataFrame<T>) : SpreadContext
    class GroupedDataFrame<T, G>(val df: org.jetbrains.dataframe.GroupedDataFrame<T, G>) : SpreadContext
    class GroupAggregator<T>(val builder: GroupAggregateBuilder<T>) : SpreadContext
}

typealias Reducer<T, R> = DataFrame<T>.(DataFrame<T>) -> R

class SpreadClause<T, K, V, C : SpreadContext>(
    val context: C,
    val keyColumn: ColumnSelector<T, K>,
    val valueColumn: ColumnSelector<T, *>?,
    val valueSelector: Reducer<T, V>,
    val valueType: KType,
    val defaultValue: Any? = null,
    val columnPath: (K) -> List<String>?
) {

    companion object {
        fun <T, K> inDataFrame(df: DataFrame<T>, keyColumn: ColumnSelector<T, K>) =
            create(SpreadContext.DataFrame(df), keyColumn)

        fun <T, G, K> inGroupedDataFrame(df: GroupedDataFrame<T, G>, keyColumn: ColumnSelector<G, K>) =
            create(SpreadContext.GroupedDataFrame(df), keyColumn)

        fun <T, K> inAggregator(builder: GroupAggregateBuilder<T>, keyColumn: ColumnSelector<T, K>) =
            create(SpreadContext.GroupAggregator(builder), keyColumn)

        fun <T, K, C : SpreadContext> create(context: C, keyColumn: ColumnSelector<T, K>) =
            SpreadClause(context, keyColumn, null, { true }, getType<Boolean>(), false) { listOf(it.toString()) }
    }
}

fun <T, G, C> GroupedDataFrame<T, G>.spread(selector: ColumnSelector<G, C>) =
    SpreadClause.inGroupedDataFrame(this, selector)

@JvmName("addPathTKVC")
internal fun <T, K, V, C : SpreadContext> SpreadClause<T, K, V, C>.addPath(keyTransform: (K) -> ColumnPath?) =
    SpreadClause(context, keyColumn, valueColumn, valueSelector, valueType, defaultValue) { keyTransform(it) }

inline fun <T, K, reified V, C : SpreadContext> SpreadClause<T, K, *, C>.with(noinline valueSelector: Reducer<T, V>) =
    SpreadClause(context, keyColumn, valueColumn, valueSelector, getType<V>(), null, columnPath)

fun <T, K, V, C : SpreadContext> SpreadClause<T, K, V, C>.useDefault(defaultValue: V) =
    SpreadClause(context, keyColumn, valueColumn, valueSelector, valueType, defaultValue, columnPath)

@JvmName("useDefaultTKVC")
fun <T, K, V, C : SpreadContext> SpreadClause<T, K, DataColumn<V>, C>.useDefault(defaultValue: V): SpreadClause<T, K, DataColumn<V>, C> =
    SpreadClause(context, keyColumn, valueColumn, valueSelector, valueType, defaultValue, columnPath)

internal fun <T, K, V, C : SpreadContext> SpreadClause<T, K, V, *>.changeContext(newContext: C) =
    SpreadClause(newContext, keyColumn, valueColumn, valueSelector, valueType, defaultValue, columnPath)

fun <T, K> SpreadClause<T, K, *, SpreadContext.DataFrame<T>>.by(column: String) = by { column.toColumnDef() }
inline fun <T, K, reified V> SpreadClause<T, K, *, SpreadContext.DataFrame<T>>.by(column: KProperty<V>) =
    by { column.toColumnDef() }

inline fun <T, K, reified V> SpreadClause<T, K, *, SpreadContext.DataFrame<T>>.by(column: ColumnReference<V>) = by { column }
inline fun <T, K, reified V> SpreadClause<T, K, *, SpreadContext.DataFrame<T>>.by(noinline columnSelector: ColumnSelector<T, V>): SpreadClause<T, K, DataColumn<V>, SpreadContext.DataFrame<T>> =
    SpreadClause(
        context,
        keyColumn,
        columnSelector,
        { column(columnSelector) },
        getType<DataColumn<V>>(),
        null,
        columnPath
    )

inline fun <T, K, V, reified R> SpreadClause<T, K, DataColumn<V>, SpreadContext.DataFrame<T>>.map(noinline transform: (V) -> R) =
    SpreadClause(
        context,
        keyColumn,
        valueColumn,
        { valueSelector(it, it).map(getType<R>(), transform) },
        getType<DataColumn<R>>(),
        null,
        columnPath
    )

inline fun <T, K, reified V, C : SpreadContext> SpreadClause<T, K, *, C>.withSingle(noinline valueSelector: RowSelector<T, V>) =
    with {
        when (it.nrow()) {
            0 -> null
            1 -> {
                val row = it[0]
                valueSelector(row, row)
            }
            else -> throw Exception()
        }
    }

fun <T, G> GroupedDataFrame<T, G>.countBy(keySelector: ColumnSelector<G, String?>) = aggregate {
    countBy(keySelector).into { it }
}

inline infix fun <T, K, V, reified C : SpreadContext> SpreadClause<T, K, V, C>.into(noinline keyTransform: (K) -> String?) =
    doSpreadInto(this, C::class) { keyTransform(it)?.let { listOf(it) } }

inline infix fun <T, K, V, reified C : SpreadContext> SpreadClause<T, K, V, C>.into(groupPath: ColumnPath) =
    intoPaths { groupPath.toList() + it.toString() }

inline infix fun <T, K, V, reified C : SpreadContext> SpreadClause<T, K, V, C>.into(groupName: String) =
    intoPaths { listOf(groupName, it.toString()) }

inline infix fun <T, K, V, reified C : SpreadContext> SpreadClause<T, K, V, C>.into(column: MapColumnReference) =
    intoPaths { column.getPath() + it.toString() }

inline infix fun <T, K, V, reified C : SpreadContext> SpreadClause<T, K, V, C>.intoPaths(noinline keyTransform: (K) -> ColumnPath?) =
    doSpreadInto(this, C::class, keyTransform)

fun <T, K, V, C : SpreadContext> doSpreadInto(
    clause: SpreadClause<T, K, V, C>,
    contextType: KClass<C>,
    keyTransform: (K) -> ColumnPath?
): DataFrame<T> {
    val withPath = clause.addPath(keyTransform)
    return when (contextType) {
        SpreadContext.DataFrame::class -> (withPath as SpreadClause<T, K, V, SpreadContext.DataFrame<T>>).execute()
        SpreadContext.GroupAggregator::class -> (withPath as SpreadClause<T, K, V, SpreadContext.GroupAggregator<T>>).execute()
        SpreadContext.GroupedDataFrame::class -> (withPath as SpreadClause<T, K, V, SpreadContext.GroupedDataFrame<T, T>>).execute()
        else -> throw UnsupportedOperationException()
    }
}

@JvmName("spreadForDataFrame")
internal fun <T, K, V> SpreadClause<T, K, V, SpreadContext.DataFrame<T>>.execute(): DataFrame<T> {
    val df = context.df
    val grouped = df.groupBy {
        val columnsToExclude = valueColumn?.let { keyColumn() and it() } ?: keyColumn()
        except(columnsToExclude)
    }
    return grouped.aggregate {
        val clause = changeContext(SpreadContext.GroupAggregator(this))
        clause.execute()
    }
}

@JvmName("spreadForGroupedDataFrame")
internal fun <T, K, V, G> SpreadClause<G, K, V, SpreadContext.GroupedDataFrame<T, G>>.execute(): DataFrame<T> {
    val df = context.df
    return df.aggregate {
        val clause = changeContext(SpreadContext.GroupAggregator(this))
        clause.execute()
    }
}

internal fun <T, K, V> SpreadClause<T, K, V, SpreadContext.GroupAggregator<T>>.execute(): DataFrame<T> {
    val df = context.builder.df
    val isColumnType = valueType.isSubtypeOf(getType<AnyCol>())

    val defaultType = valueType.let {
        if (isColumnType) it.arguments[0].type else it
    }.takeUnless { it?.classifier == Any::class }

    df.groupBy(keyColumn).forEach { key, group ->
        if(group == null) return@forEach

        val keyValue = key[0] as K
        val path = columnPath(keyValue) ?: return@forEach

        var value: Any? = valueSelector(group, group)
        var type = defaultType

        // if computed value is column, extract a single value or a list of values from it
        if (isColumnType && value != null) {
            val col = value as AnyCol
            if (col.size == 1) {
                value = col[0]
            } else {
                if(col.isGroup()){
                    type = DataFrame::class.createStarProjectedType(false)
                    value = col.asGroup().df
                }else {
                    val elementType = defaultType
                        ?: (col.values.mapNotNull { it?.javaClass?.kotlin }
                            .commonParent()).createStarProjectedType(col.hasNulls)
                    type = List::class.createType(elementType)
                    value = col.toList()
                }
            }
        }

        if (type == null) {
            type = value?.javaClass?.kotlin?.createStarProjectedType(false) ?: getType<Unit?>()
        }
        context.builder.addValue(path, value, type, defaultValue)
    }
    return df
}