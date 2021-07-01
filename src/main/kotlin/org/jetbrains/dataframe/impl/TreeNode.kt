package org.jetbrains.dataframe.impl

import org.jetbrains.dataframe.ColumnPath

internal interface ReadonlyTreeNode<out T> {

    val name: String
    val depth: Int
    val data: T
    val parent: ReadonlyTreeNode<T>?
    val children: List<ReadonlyTreeNode<T>>

    fun getRoot(): ReadonlyTreeNode<T>?

    operator fun get(childName: String): ReadonlyTreeNode<T>?
}

internal class TreeNode<T>(
    override val name: String,
    override val depth: Int,
    override var data: T,
    override val parent: TreeNode<T>? = null
) : ReadonlyTreeNode<T> {

    companion object {
        fun <T> createRoot(data: T) = TreeNode<T>("", 0, data)
    }

    private val myChildren = mutableListOf<TreeNode<T>>()
    private val childrenMap = mutableMapOf<String, TreeNode<T>>()

    override val children: List<TreeNode<T>>
        get() = myChildren

    override fun getRoot(): TreeNode<T> = parent?.getRoot() ?: this

    fun contains(childName: String) = childrenMap.containsKey(childName)

    override operator fun get(childName: String) = childrenMap[childName]

    fun pathFromRoot(): List<String> {
        val path = mutableListOf<String>()
        var node: TreeNode<T>? = this
        while (node != null && node.parent != null) {
            path.add(node.name)
            node = node.parent
        }
        path.reverse()
        return path
    }

    fun addChild(childName: String, childData: T): TreeNode<T> {
        val node = TreeNode(childName, depth + 1, childData, this)
        myChildren.add(node)
        childrenMap[childName] = node
        return node
    }

    fun getOrPut(childName: String, createData: () -> T): TreeNode<T> {
        childrenMap[childName]?.let { return it }
        return addChild(childName, createData())
    }

    fun dfs(enterCondition: (TreeNode<T>) -> Boolean = { true }, yieldCondition: (TreeNode<T>) -> Boolean = { true }): List<TreeNode<T>> {
        val result = mutableListOf<TreeNode<T>>()
        fun doDfs(node: TreeNode<T>) {
            if (yieldCondition(node)) {
                result.add(node)
            }
            if (enterCondition(node)) {
                node.children.forEach {
                    doDfs(it)
                }
            }
        }
        doDfs(this)
        return result
    }
}

internal tailrec fun <T> ReadonlyTreeNode<T>.getAncestor(depth: Int): ReadonlyTreeNode<T> {
    if (depth > this.depth) throw UnsupportedOperationException()
    if (depth == this.depth) return this
    val p = parent ?: throw UnsupportedOperationException()
    return p.getAncestor(depth)
}

internal fun <T> TreeNode<T?>.getOrPut(path: ColumnPath) = getOrPutEmpty(path, null)

internal fun <T> TreeNode<T>.getOrPutEmpty(path: ColumnPath, emptyData: T): TreeNode<T> = getOrPut(path) { emptyData }

internal fun <T> TreeNode<T?>.put(path: ColumnPath, data: T): TreeNode<T?> = getOrPut(path).also { it.data = data }

internal fun <T> TreeNode<T>.getOrPut(path: ColumnPath, createData: (ColumnPath) -> T): TreeNode<T> {
    var node = this
    path.indices.forEach {
        node = node.getOrPut(path[it]) { createData(path.take(it + 1)) }
    }
    return node
}

internal fun <T> TreeNode<T>.topDfs(yieldCondition: (TreeNode<T>) -> Boolean): List<TreeNode<T>> = dfs(enterCondition = { !yieldCondition(it) }, yieldCondition = yieldCondition)

internal fun <T> TreeNode<T>.topDfsExcluding(excludeRoot: TreeNode<*>): List<TreeNode<T>> {
    val result = mutableListOf<TreeNode<T>>()
    fun doDfs(node: TreeNode<T>, exclude: TreeNode<*>) {
        if (exclude.children.isNotEmpty()) {
            node.children.filter { !exclude.contains(it.name) }.forEach { result.add(it) }
            exclude.children.forEach {
                val srcNode = node[it.name]
                if (srcNode != null) {
                    doDfs(srcNode, it)
                }
            }
        }
    }
    doDfs(this, excludeRoot)
    return result
}

internal fun <T> TreeNode<T?>.dfsNotNull() = dfs { it.data != null }.map { it as TreeNode<T> }
internal fun <T> TreeNode<T?>.dfsTopNotNull() = dfs(enterCondition = { it.data == null }, yieldCondition = { it.data != null }).map { it as TreeNode<T> }
