package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.*
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

public class IterableDataFrameBuilder<T>(public val source: Iterable<T>) {
    internal val columns = mutableListOf<AnyColumn>()

    public fun add(column: AnyColumn): Boolean = columns.add(column)

    public inline fun <reified R> add(name: String, noinline expression: T.(T) -> R?): Boolean =
        add(column(name, source.map { expression(it, it) }))

    public inline infix fun <reified R> String.to(noinline expression: T.(T) -> R?): Boolean = add(this, expression)

    public inline infix operator fun <reified R> String.invoke(noinline expression: T.(T) -> R?): Boolean = add(this, expression)

    public inline infix operator fun <reified R> KProperty<R>.invoke(noinline expression: T.(T) -> R): Boolean = add(name, expression)
}

public fun <T> Iterable<T>.toDataFrame(body: IterableDataFrameBuilder<T>.() -> Unit): AnyFrame {
    val builder = IterableDataFrameBuilder(this)
    builder.body()
    return dataFrameOf(builder.columns)
}

public inline fun <reified T> Iterable<T>.toDataFrameByProperties(): AnyFrame = T::class.declaredMembers
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

public fun DataFrame.Companion.of(columns: Iterable<AnyColumn>): AnyFrame = dataFrameOf(columns)
public fun DataFrame.Companion.of(vararg header: String): DataFrameBuilder = dataFrameOf(header.toList())
public fun DataFrame.Companion.of(vararg columns: AnyColumn): AnyFrame = dataFrameOf(columns.asIterable())

public fun dataFrameOf(columns: Iterable<AnyColumn>): AnyFrame {
    val cols = columns.map { it.unbox() }
    if (cols.isEmpty()) return DataFrame.empty()
    return DataFrameImpl<Unit>(cols)
}

public fun dataFrameOf(vararg header: ColumnReference<*>): DataFrameBuilder = DataFrameBuilder(header.map { it.name() })

public fun dataFrameOf(vararg columns: AnyColumn): AnyFrame = dataFrameOf(columns.asIterable())

public fun dataFrameOf(vararg header: String): DataFrameBuilder = dataFrameOf(header.toList())

public inline fun <T, reified C> dataFrameOf(first: T, second: T, vararg other: T, fill: (T) -> Iterable<C>): DataFrame<Unit> = dataFrameOf(listOf(first, second) + other, fill)

public fun <T> dataFrameOf(first: T, second: T, vararg other: T): DataFrameBuilder = dataFrameOf((listOf(first, second) + other).map { it.toString() })

public fun <T> dataFrameOf(header: Iterable<T>): DataFrameBuilder = dataFrameOf(header.map { it.toString() })

public inline fun <T, reified C> dataFrameOf(header: Iterable<T>, fill: (T) -> Iterable<C>): DataFrame<Unit> = header.map { value -> fill(value).asList().let { DataColumn.createWithNullCheck(value.toString(), it) } }.toDataFrame()

public fun dataFrameOf(header: CharProgression): DataFrameBuilder = dataFrameOf(header.map { it.toString() })

public fun dataFrameOf(header: List<String>): DataFrameBuilder = DataFrameBuilder(header)

public fun emptyDataFrame(nrow: Int): AnyFrame = DataFrame.empty(nrow)

// TODO: remove checks for ColumnWithParent types
internal fun AnyColumn.unbox(): AnyCol = when (this) {
    is ColumnWithPath<*> -> data.unbox()
    is ColumnWithParent<*> -> source.unbox()
    else -> this as AnyCol
}

public fun <T> Iterable<AnyColumn>.asDataFrame(): DataFrame<T> = dataFrameOf(this).typed<T>()

@JvmName("toDataFramePairColumnPathAnyCol")
public fun <T> Iterable<Pair<ColumnPath, AnyColumn>>.toDataFrame(): DataFrame<T> {
    val nameGenerator = ColumnNameGenerator()
    val columnNames = mutableListOf<String>()
    val columnGroups = mutableListOf<MutableList<Pair<ColumnPath, AnyColumn>>?>()
    val columns = mutableListOf<AnyColumn?>()
    val columnIndices = mutableMapOf<String, Int>()
    val columnGroupName = mutableMapOf<String, String>()

    forEach { (path, col) ->
        when (path.size) {
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
                val uniqueName = columnGroupName.getOrPut(name) {
                    nameGenerator.addUnique(name)
                }
                val index = columnIndices.getOrPut(uniqueName) {
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
        if (group != null) {
            val nestedDf = group.toDataFrame<Unit>()
            val col = DataColumn.create(columnNames[index], nestedDf)
            assert(columns[index] == null)
            columns[index] = col
        } else assert(columns[index] != null)
    }
    return columns.map { it!! }.asDataFrame()
}

@JvmName("toDataFrameAnyCol")
public fun Iterable<AnyColumn>.toDataFrame(): DataFrame<Unit> = asDataFrame()

public class DataFrameBuilder(private val header: List<String>) {

    public operator fun invoke(vararg columns: AnyCol): AnyFrame = invoke(columns.asIterable())

    public operator fun invoke(columns: Iterable<AnyCol>): AnyFrame {
        val cols = columns.asList()
        require(cols.size == header.size) { "Number of columns differs from number of column names" }
        return cols.mapIndexed { i, col ->
            col.rename(header[i])
        }.asDataFrame<Unit>()
    }

    public operator fun invoke(vararg values: Any?): AnyFrame = invoke(values.asIterable())

    @JvmName("invoke1")
    public operator fun invoke(values: Iterable<Any?>): AnyFrame {
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

    public operator fun invoke(args: Sequence<Any?>): AnyFrame = invoke(*args.toList().toTypedArray())

    public fun withColumns(columnBuilder: (String) -> AnyCol): AnyFrame = header.map(columnBuilder).toDataFrame()

    public inline operator fun <reified T> invoke(crossinline valuesBuilder: (String) -> Iterable<T>): AnyFrame = withColumns { name -> valuesBuilder(name).let { DataColumn.createWithNullCheck(name, it.asList()) } }

    public inline fun <reified C> fill(nrow: Int, value: C): AnyFrame = withColumns { name -> DataColumn.create(name, List(nrow) { value }, getType<C>().withNullability(value == null)) }

    public inline fun <reified C> nulls(nrow: Int): AnyFrame = fill<C?>(nrow, null)

    public inline fun <reified C> fillIndexed(nrow: Int, crossinline init: (Int, String) -> C): AnyFrame = withColumns { name -> DataColumn.createWithNullCheck(name, List(nrow) { init(it, name) }) }

    public inline fun <reified C> fill(nrow: Int, crossinline init: (Int) -> C): AnyFrame = withColumns { name -> DataColumn.createWithNullCheck(name, List(nrow, init)) }

    private inline fun <reified C> fillNotNull(nrow: Int, crossinline init: (Int) -> C) = withColumns { name -> DataColumn.create(name, List(nrow, init), getType<C>()) }

    public fun randomInt(nrow: Int): AnyFrame = fillNotNull(nrow) { Random.nextInt() }

    public fun randomDouble(nrow: Int): AnyFrame = fillNotNull(nrow) { Random.nextDouble() }

    public fun randomFloat(nrow: Int): AnyFrame = fillNotNull(nrow) { Random.nextFloat() }

    public fun randomBoolean(nrow: Int): AnyFrame = fillNotNull(nrow) { Random.nextBoolean() }
}

internal fun Iterable<KClass<*>>.commonParent() = commonParents(this).withMostSuperclasses() ?: Any::class

internal fun Iterable<KClass<*>>.commonType(nullable: Boolean, upperBound: KType? = null) = commonParents(this).createType(nullable, upperBound)

public fun Map<String, Iterable<Any?>>.toDataFrame(): AnyFrame {
    return map { DataColumn.create(it.key, it.value.asList()) }.toDataFrame()
}

@JvmName("toDataFrameColumnPathAny?")
public fun Map<ColumnPath, Iterable<Any?>>.toDataFrame(): AnyFrame {
    return map { it.key to DataColumn.create(it.key.last(), it.value.asList()) }.toDataFrame<Unit>()
}

@JvmName("toDataFrameColumnPathAny?")
public fun Iterable<Pair<ColumnPath, Iterable<Any?>>>.toDataFrame(): AnyFrame {
    return map { it.first to guessColumnType(it.first.last(), it.second.asList()) }.toDataFrame<Unit>()
}

public fun Iterable<Pair<String, Iterable<Any?>>>.toDataFrame(): AnyFrame {
    return map { listOf(it.first) to guessColumnType(it.first, it.second.asList()) }.toDataFrame<Unit>()
}
