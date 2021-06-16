package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.AnyCol
import org.jetbrains.dataframe.columns.AnyColumn
import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.ColumnWithPath
import org.jetbrains.dataframe.columns.guessColumnType
import org.jetbrains.dataframe.impl.ColumnNameGenerator
import org.jetbrains.dataframe.impl.DataFrameImpl
import org.jetbrains.dataframe.impl.asList
import org.jetbrains.dataframe.impl.columns.ColumnWithParent
import kotlin.random.Random
import kotlin.reflect.KClass
import kotlin.reflect.KProperty
import kotlin.reflect.KType
import kotlin.reflect.full.declaredMembers
import kotlin.reflect.full.withNullability
import kotlin.reflect.jvm.javaField

class IterableDataFrameBuilder<T>(val source: Iterable<T>) {
    internal val columns = mutableListOf<AnyColumn>()

    fun add(column: AnyColumn) = columns.add(column)

    inline fun <reified R> add(name: String, noinline expression: T.(T) -> R?) =
        add(column(name, source.map { expression(it, it) }))

    inline infix fun <reified R> String.to(noinline expression: T.(T) -> R?) = add(this, expression)

    inline infix operator fun <reified R> String.invoke(noinline expression: T.(T) -> R?) = add(this, expression)

    inline infix operator fun <reified R> KProperty<R>.invoke(noinline expression: T.(T) -> R) = add(name, expression)
}

fun <T> Iterable<T>.toDataFrame(body: IterableDataFrameBuilder<T>.() -> Unit): AnyFrame {
    val builder = IterableDataFrameBuilder(this)
    builder.body()
    return dataFrameOf(builder.columns)
}

inline fun <reified T> Iterable<T>.toDataFrameByProperties() = T::class.declaredMembers
    .filter { it.parameters.toList().size == 1 }
    .filter { it is KProperty }
    .map {
        val property = (it as KProperty)
        property.javaField?.isAccessible = true
        var nullable = false
        val values = this.map { obj ->
            if (obj == null) {
                nullable = true
                null
            } else {
                val value = it.call(obj)
                if (value == null) nullable = true
                value
            }
        }
        DataColumn.create(it.name, values, property.returnType.withNullability(nullable))
    }.let { dataFrameOf(it) }

fun DataFrame.Companion.of(columns: Iterable<AnyColumn>) = dataFrameOf(columns)
fun DataFrame.Companion.of(vararg header: String) = dataFrameOf(header.toList())
fun DataFrame.Companion.of(vararg columns: AnyColumn) = dataFrameOf(columns.asIterable())

fun dataFrameOf(columns: Iterable<AnyColumn>): AnyFrame {
    val cols = columns.map { it.unbox() }
    if(cols.isEmpty()) return DataFrame.empty()
    return DataFrameImpl<Unit>(cols)
}

fun dataFrameOf(vararg header: ColumnReference<*>) = DataFrameBuilder(header.map { it.name() })

fun dataFrameOf(vararg columns: AnyColumn): AnyFrame = dataFrameOf(columns.asIterable())

fun dataFrameOf(vararg header: String) = dataFrameOf(header.toList())

inline fun <T, reified C> dataFrameOf(first: T, second: T, vararg other: T, fill: (T) -> Iterable<C>) = dataFrameOf(listOf(first, second) + other, fill)

fun <T> dataFrameOf(first: T, second: T, vararg other: T) = dataFrameOf((listOf(first, second) + other).map {it.toString()})

fun <T> dataFrameOf(header: Iterable<T>) = dataFrameOf(header.map {it.toString()})

inline fun <T, reified C> dataFrameOf(header: Iterable<T>, fill: (T) -> Iterable<C>) = header.map { value -> fill(value).asList().let { DataColumn.createWithNullCheck(value.toString(), it)} }.toDataFrame()

fun dataFrameOf(header: CharProgression) = dataFrameOf(header.map { it.toString() })

fun dataFrameOf(header: List<String>) = DataFrameBuilder(header)

fun emptyDataFrame(nrow: Int) = DataFrame.empty(nrow)


// TODO: remove checks for ColumnWithParent types
internal fun AnyColumn.unbox(): AnyCol = when (this) {
    is ColumnWithPath<*> -> data.unbox()
    is ColumnWithParent<*> -> source.unbox()
    else -> this as AnyCol
}

fun <T> Iterable<AnyColumn>.asDataFrame() = dataFrameOf(this).typed<T>()

@JvmName("toDataFramePairColumnPathAnyCol")
fun <T> Iterable<Pair<ColumnPath, AnyColumn>>.toDataFrame(): DataFrame<T> {

    val nameGenerator = ColumnNameGenerator()
    val columnNames = mutableListOf<String>()
    val columnGroups = mutableListOf<MutableList<Pair<ColumnPath, AnyColumn>>?>()
    val columns = mutableListOf<AnyColumn?>()
    val columnIndices = mutableMapOf<String, Int>()
    val columnGroupName = mutableMapOf<String, String>()

    forEach { (path, col) ->
        when(path.size){
            0 -> {}
            1 -> {
                val name = path[0]
                val uniqueName = nameGenerator.addUnique(name)
                val index = columns.size
                columnNames.add(uniqueName)
                columnGroups.add(null)
                columns.add(col.rename(uniqueName))
                columnIndices[uniqueName] = index
            }
            else -> {
                val name = path[0]
                val uniqueName = columnGroupName.getOrPut(name){
                    nameGenerator.addUnique(name)
                }
                val index = columnIndices.getOrPut(uniqueName){
                    columnNames.add(uniqueName)
                    columnGroups.add(mutableListOf())
                    columns.add(null)
                    columns.size - 1
                }
                val list = columnGroups[index]!!
                list.add(path.subList(1, path.size) to col)
            }
        }
    }
    columns.indices.forEach { index ->
        val group = columnGroups[index]
        if(group != null){
            val nestedDf = group.toDataFrame<Unit>()
            val col = DataColumn.create(columnNames[index], nestedDf)
            assert(columns[index] == null)
            columns[index] = col
        } else assert(columns[index] != null)
    }
    return columns.map { it!! }.asDataFrame()
}

@JvmName("toDataFrameAnyCol")
fun Iterable<AnyColumn>.toDataFrame() = asDataFrame<Unit>()

class DataFrameBuilder(private val header: List<String>) {

    operator fun invoke(vararg columns: AnyCol) = invoke(columns.asIterable())

    operator fun invoke(columns: Iterable<AnyCol>): AnyFrame {
        val cols = columns.asList()
        require(cols.size == header.size) { "Number of columns differs from number of column names" }
        return cols.mapIndexed { i, col ->
            col.rename(header[i])
        }.asDataFrame<Unit>()
    }

    operator fun invoke(vararg values: Any?) = invoke(values.asIterable())

    @JvmName("invoke1")
    operator fun invoke(values: Iterable<Any?>): AnyFrame {

        val list = values.asList()

        val ncol = header.size

        require(header.size > 0 && list.size.rem(ncol) == 0) {
            "Number of values ${list.size} is not divisible by number of columns $ncol"
        }

        val nrow = list.size / ncol

        return (0 until ncol).map { col ->
            val colValues = (0 until nrow).map { row ->
                list[row * ncol + col]
            }
            DataColumn.create(header[col], colValues)
        }.toDataFrame()
    }

    operator fun invoke(args: Sequence<Any?>) = invoke(*args.toList().toTypedArray())

    fun withColumns(columnBuilder: (String)->AnyCol): AnyFrame = header.map(columnBuilder).toDataFrame()

    inline operator fun <reified T> invoke(crossinline valuesBuilder: (String) -> Iterable<T>) = withColumns { name -> valuesBuilder(name).let { DataColumn.createWithNullCheck(name, it.asList()) }}

    inline fun <reified C> fill(nrow: Int, value: C) = withColumns { name -> DataColumn.create(name, List(nrow) { value }, getType<C>().withNullability(value == null)) }

    inline fun <reified C> nulls(nrow: Int) = fill<C?>(nrow, null)

    inline fun <reified C> fillIndexed(nrow: Int, crossinline init: (Int, String) -> C) = withColumns { name -> DataColumn.createWithNullCheck(name, List(nrow){ init(it, name) })}

    inline fun <reified C> fill(nrow: Int, crossinline init: (Int) -> C) = withColumns { name -> DataColumn.createWithNullCheck(name, List(nrow, init))}

    private inline fun <reified C> fillNotNull(nrow: Int, crossinline init: (Int) -> C) = withColumns { name -> DataColumn.create(name, List(nrow, init), getType<C>())}

    fun randomInt(nrow: Int) = fillNotNull(nrow) { Random.nextInt() }

    fun randomDouble(nrow: Int) = fillNotNull(nrow) { Random.nextDouble() }

    fun randomFloat(nrow: Int) = fillNotNull(nrow) { Random.nextFloat() }

    fun randomBoolean(nrow: Int) = fillNotNull(nrow) { Random.nextBoolean() }
}

internal fun Iterable<KClass<*>>.commonParent() = commonParents(this).withMostSuperclasses() ?: Any::class

internal fun Iterable<KClass<*>>.commonType(nullable: Boolean, upperBound: KType? = null) = commonParents(this).createType(nullable, upperBound)

fun Map<String, Iterable<Any?>>.toDataFrame(): AnyFrame {
    return map { DataColumn.create(it.key, it.value.asList()) }.toDataFrame()
}

@JvmName("toDataFrameColumnPathAny?")
fun Map<ColumnPath, Iterable<Any?>>.toDataFrame(): AnyFrame {
    return map { it.key to DataColumn.create(it.key.last(), it.value.asList()) }.toDataFrame<Unit>()
}

@JvmName("toDataFrameColumnPathAny?")
fun Iterable<Pair<ColumnPath, Iterable<Any?>>>.toDataFrame(): AnyFrame {
    return map { it.first to guessColumnType(it.first.last(), it.second.asList()) }.toDataFrame<Unit>()
}

fun Iterable<Pair<String, Iterable<Any?>>>.toDataFrame(): AnyFrame {
    return map { listOf(it.first) to guessColumnType(it.first, it.second.asList()) }.toDataFrame<Unit>()
}