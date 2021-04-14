package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.columns.ColumnWithPath
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.FrameColumn
import org.jetbrains.dataframe.columns.MapColumn
import org.jetbrains.dataframe.impl.TreeNode
import org.jetbrains.dataframe.impl.columns.DataColumnWithParent
import org.jetbrains.dataframe.impl.columns.ColumnWithParent
import org.jetbrains.dataframe.impl.columns.addPath
import org.jetbrains.dataframe.impl.columns.asGroup
import org.jetbrains.dataframe.impl.columns.changePath
import org.jetbrains.dataframe.impl.columns.withDf
import org.jetbrains.dataframe.impl.getAncestor
import org.jetbrains.dataframe.impl.getOrPutEmpty
import kotlin.reflect.KClass
import kotlin.reflect.KType
import kotlin.reflect.KTypeProjection
import kotlin.reflect.KVisibility
import kotlin.reflect.full.allSuperclasses
import kotlin.reflect.full.createType
import kotlin.reflect.full.isSubclassOf
import kotlin.reflect.full.superclasses
import kotlin.reflect.jvm.jvmErasure

fun rowNumber(columnName: String = "id") = AddRowNumberStub(columnName)

data class AddRowNumberStub(val columnName: String)

// size

val AnyFrame.size: DataFrameSize get() = DataFrameSize(ncol(), nrow())

fun commonParent(classes: Iterable<KClass<*>>) = commonParents(classes).withMostSuperclasses()

fun commonParent(vararg classes: KClass<*>) = commonParent(classes.toList())

fun Iterable<KClass<*>>.withMostSuperclasses() = maxByOrNull { it.allSuperclasses.size }

fun commonParents(vararg classes: KClass<*>) = commonParents(classes.toList())

fun commonParents(classes: Iterable<KClass<*>>) =
    when {
        !classes.any() -> emptyList()
        else -> {
            classes.distinct().let {
                when {
                    it.size == 1 && it[0].visibility == KVisibility.PUBLIC -> { // if there is only one class - return it
                        listOf(it[0])
                    }
                    else -> it.fold(null as (Set<KClass<*>>?)) { set, clazz ->
                        // collect a set of all common superclasses from original classes
                        val superclasses =
                            (clazz.allSuperclasses + clazz).filter { it.visibility == KVisibility.PUBLIC }
                        set?.intersect(superclasses) ?: superclasses.toSet()
                    }!!.let {
                        it - it.flatMap { it.superclasses } // leave only 'leaf' classes, that are not super to some other class in a set
                    }.toList()
                }
            }
        }
    }

internal fun baseType(types: Set<KType>): KType {
    val nullable = types.any { it.isMarkedNullable }
    return when (types.size) {
        0 -> getType<Unit>()
        1 -> types.single()
        else -> {
            val classes = types.map { it.jvmErasure }.distinct()
            when {
                classes.size == 1 -> {
                    val typeProjections = classes[0].typeParameters.mapIndexed { index, parameter ->
                        val arguments = types.map { it.arguments[index].type }.toSet()
                        if (arguments.contains(null)) KTypeProjection.STAR
                        else {
                            val type = baseType(arguments as Set<KType>)
                            KTypeProjection(parameter.variance, type)
                        }
                    }
                    classes[0].createType(typeProjections, nullable)
                }
                classes.any { it == List::class } && classes.all { it == List::class || !it.isSubclassOf(Collection::class) } -> {
                    val listTypes =
                        types.map { if (it.classifier == List::class) it.arguments[0].type else it }.toMutableSet()
                    if (listTypes.contains(null)) List::class.createStarProjectedType(nullable)
                    else {
                        val type = baseType(listTypes as Set<KType>)
                        List::class.createType(listOf(KTypeProjection.invariant(type)), nullable)
                    }
                }
                else -> {
                    val commonClass = commonParent(classes) ?: Any::class
                    commonClass.createStarProjectedType(nullable)
                }
            }
        }
    }
}

internal fun indexColumn(columnName: String, size: Int): AnyCol = column(columnName, (0 until size).toList())

fun <T> DataFrame<T>.addRowNumber(column: ColumnReference<Int>) = addRowNumber(column.name())
fun <T> DataFrame<T>.addRowNumber(columnName: String = "id"): DataFrame<T> = dataFrameOf(
    columns() + indexColumn(
        columnName,
        nrow()
    )
).typed<T>()

fun AnyCol.addRowNumber(columnName: String = "id") = dataFrameOf(listOf(indexColumn(columnName, size), this))

// Update

inline fun <reified C> headPlusArray(head: C, cols: Array<out C>) = (listOf(head) + cols.toList()).toTypedArray()

// column grouping

internal fun TreeNode<ColumnPosition>.allRemovedColumns() = dfs { it.data.wasRemoved && it.data.column != null }

internal fun TreeNode<ColumnPosition>.allWithColumns() = dfs { it.data.column != null }

internal fun Iterable<ColumnWithPath<*>>.colsDfs(): List<ColumnWithPath<*>> {

    val result = mutableListOf<ColumnWithPath<*>>()
    fun dfs(cols: Iterable<ColumnWithPath<*>>) {
        cols.forEach {
            result.add(it)
            val path = it.path
            val df = it.df
            if (it.data.isGroup())
                dfs(it.data.asGroup().columns().map { it.addPath(path + it.name(), df) })
        }
    }
    dfs(this)
    return result
}

internal fun AnyFrame.collectTree() = columns().map { it.addPath(this) }.collectTree()

internal fun List<ColumnWithPath<*>>.collectTree() = collectTree(DataColumn.empty()) { it }

internal fun <D> AnyFrame.collectTree(emptyData: D, createData: (AnyCol) -> D) =
    columns().map { it.addPath(this) }.collectTree(emptyData, createData)

internal fun <D> List<ColumnWithPath<*>>.collectTree(emptyData: D, createData: (AnyCol) -> D): TreeNode<D> {

    val root = TreeNode.createRoot(emptyData)

    fun collectColumns(col: AnyCol, parentNode: TreeNode<D>) {
        val newNode = parentNode.getOrPut(col.name()) { createData(col) }
        if (col.isGroup()) {
            col.asGroup().columns().forEach {
                collectColumns(it, newNode)
            }
        }
    }
    forEach {
        if (it.path.isEmpty()) {
            it.data.asGroup().df.columns().forEach {
                collectColumns(it, root)
            }
        } else {
            val node = root.getOrPutEmpty(it.path.dropLast(1), emptyData)
            collectColumns(it.data, node)
        }
    }
    return root
}

//TODO: make immutable
internal data class ColumnPosition(val originalIndex: Int, var wasRemoved: Boolean, var column: AnyCol?)

internal data class ColumnToInsert(
    val insertionPath: ColumnPath,
    val referenceNode: TreeNode<ColumnPosition>?,
    val column: AnyCol
)

fun Column.getParent(): MapColumnReference? = when (this) {
    is ColumnWithParent<*> -> parent
    is DataColumnWithParent<*> -> parent
    else -> null
}

fun Column.getPath(): ColumnPath {
    val list = mutableListOf<String>()
    var c = this.asNullable()
    while (c != null) {
        list.add(c.name())
        c = c.getParent()
    }
    list.reverse()
    return list
}

internal fun <T> DataFrame<T>.collectTree(selector: AnyColumnsSelector<T, *>): TreeNode<AnyCol?> {

    val colPaths = getColumnPaths(selector)

    val root = TreeNode.createRoot(null as AnyCol?)

    colPaths.forEach {

        var column: AnyCol? = null
        var node: TreeNode<AnyCol?> = root
        it.forEach {
            when (column) {
                null -> column = this[it]
                else -> column = column!!.asFrame()[it]
            }
            node = node.getOrPut(it) { null }
        }
        node.data = column
    }

    return root
}

internal fun <T> DataFrame<T>.doInsert(columns: List<ColumnToInsert>) = insertColumns(this, columns)

internal fun <T> insertColumns(df: DataFrame<T>?, columns: List<ColumnToInsert>) =
    insertColumns(df, columns, columns.firstOrNull()?.referenceNode?.getRoot(), 0)

internal fun insertColumns(columns: List<ColumnToInsert>) =
    insertColumns<Unit>(null, columns, columns.firstOrNull()?.referenceNode?.getRoot(), 0)

internal fun <T> insertColumns(
    df: DataFrame<T>?,
    columns: List<ColumnToInsert>,
    treeNode: TreeNode<ColumnPosition>?,
    depth: Int
): DataFrame<T> {

    if (columns.isEmpty()) return df ?: DataFrame.empty().typed()

    val childDepth = depth + 1

    val columnsMap = columns.groupBy { it.insertionPath[depth] }.toMutableMap() // map: columnName -> columnsToAdd

    val newColumns = mutableListOf<AnyCol>()

    // insert new columns under existing
    df?.columns()?.forEach {
        val subTree = columnsMap[it.name()]
        if (subTree != null) {
            // assert that new columns go directly under current column so they have longer paths
            val invalidPath = subTree.firstOrNull { it.insertionPath.size == childDepth }
            assert(invalidPath == null) { "Can't insert column `" + invalidPath!!.insertionPath.joinToString(".") + "`. Column with this path already exists" }
            val group = it as? MapColumn<*>
            assert(group != null) { "Can not insert columns under a column '${it.name()}', because it is not a column group" }
            val newDf = insertColumns(group!!.df, subTree, treeNode?.get(it.name()), childDepth)
            val newCol = group.withDf(newDf)
            newColumns.add(newCol)
            columnsMap.remove(it.name())
        } else newColumns.add(it)
    }

    // collect new columns to insert
    val columnsToAdd = columns.mapNotNull {
        val name = it.insertionPath[depth]
        val subTree = columnsMap[name]
        if (subTree != null) {
            columnsMap.remove(name)

            // look for columns in subtree that were originally located at the current insertion path
            // find the minimal original index among them
            // new column will be inserted at that position
            val minIndex = subTree.minOf {
                if (it.referenceNode == null) Int.MAX_VALUE
                else {
                    var col = it.referenceNode
                    if (col.depth > depth) col = col.getAncestor(depth + 1)
                    if (col.parent === treeNode) {
                        if (col.data.wasRemoved) col.data.originalIndex else col.data.originalIndex + 1
                    } else Int.MAX_VALUE
                }
            }

            minIndex to (name to subTree)
        } else null
    }.sortedBy { it.first } // sort by insertion index

    val removedSiblings = treeNode?.children
    var k = 0 // index in 'removedSiblings' list
    var insertionIndexOffset = 0

    columnsToAdd.forEach { (insertionIndex, pair) ->
        val (name, columns) = pair

        // adjust insertion index by number of columns that were removed before current index
        if (removedSiblings != null) {
            while (k < removedSiblings.size && removedSiblings[k].data.originalIndex < insertionIndex) {
                if (removedSiblings[k].data.wasRemoved) insertionIndexOffset--
                k++
            }
        }

        val nodeToInsert =
            columns.firstOrNull { it.insertionPath.size == childDepth } // try to find existing node to insert
        val newCol = if (nodeToInsert != null) {
            val column = nodeToInsert.column
            if (columns.size > 1) {
                assert(columns.count { it.insertionPath.size == childDepth } == 1) { "Can not insert more than one column into the path ${nodeToInsert.insertionPath}" }
                val group = column as MapColumn<*>
                val newDf = insertColumns(
                    group.df,
                    columns.filter { it.insertionPath.size > childDepth },
                    treeNode?.get(name),
                    childDepth
                )
                group.withDf(newDf)
            } else column.rename(name)
        } else {
            val newDf = insertColumns<Unit>(null, columns, treeNode?.get(name), childDepth)
            DataColumn.create(name, newDf) // new node needs to be created
        }
        if (insertionIndex == Int.MAX_VALUE)
            newColumns.add(newCol)
        else {
            newColumns.add(insertionIndex + insertionIndexOffset, newCol)
            insertionIndexOffset++
        }
    }

    return newColumns.asDataFrame()
}

internal fun <T> DataFrame<T>.splitByIndices(startIndices: Sequence<Int>): Sequence<DataFrame<T>> {
    return (startIndices + nrow()).zipWithNext { start, endExclusive ->
        get(start until endExclusive)
    }
}

internal fun <T> List<T>.splitByIndices(startIndices: Sequence<Int>): Sequence<List<T>> {
    return (startIndices + size).zipWithNext { start, endExclusive ->
        subList(start, endExclusive)
    }
}

internal fun KClass<*>.createType(typeArgument: KType? = null, nullable: Boolean = false) =
    if (typeArgument != null) createType(listOf(KTypeProjection.invariant(typeArgument)), nullable)
    else createStarProjectedType(nullable)

internal inline fun <reified T> createType(typeArgument: KType? = null) = T::class.createType(typeArgument)

fun <T> FrameColumn<T>.union() = if (size > 0) values.union() else emptyDataFrame(0)

internal fun <T> T.asNullable() = this as T?

internal fun <T> List<T>.last(count: Int) = subList(size - count, size)

/**
 * Shorten column paths as much as possible to keep them unique
 */
internal fun <C> List<ColumnWithPath<C>>.shortenPaths(): List<ColumnWithPath<C>> {

    // try to use just column name as column path
    val map = groupBy { it.path.last(1) }.toMutableMap()

    fun add(path: ColumnPath, column: ColumnWithPath<C>) {
        val list: MutableList<ColumnWithPath<C>> =
            (map.getOrPut(path) { mutableListOf() } as? MutableList<ColumnWithPath<C>>)
                ?: let {
                    val values = map.remove(path)!!
                    map.put(path, values.toMutableList()) as MutableList<ColumnWithPath<C>>
                }
        list.add(column)
    }

    // resolve name collisions by using more parts of column path
    var conflicts = map.filter { it.value.size > 1 }
    while (conflicts.size > 0) {
        conflicts.forEach {
            val key = it.key
            val keyLength = key.size
            map.remove(key)
            it.value.forEach {
                val path = it.path
                val newPath = if (path.size < keyLength) path.last(keyLength + 1) else path
                add(newPath, it)
            }
        }
        conflicts = map.filter { it.value.size > 1 }
    }

    val pathRemapping = map.map { it.value.single().path to it.key }.toMap()

    return map { it.changePath(pathRemapping[it.path]!!) }
}