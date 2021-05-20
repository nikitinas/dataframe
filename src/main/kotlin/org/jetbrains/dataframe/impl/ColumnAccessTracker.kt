package org.jetbrains.dataframe.impl

import kotlin.concurrent.getOrSet

internal class ColumnAccessTracker {

    var isEnabled = false

    val accessedColumns = mutableSetOf<String>()

    fun <T> track(body: () -> T): List<String> {
        accessedColumns.clear()
        isEnabled = true
        body()
        isEnabled = false
        return accessedColumns.toList()
    }

    fun registerAccess(columnName: String){
        if(isEnabled) accessedColumns.add(columnName)
    }

    companion object {

        fun registerColumnAccess(name: String) = get().registerAccess(name)

        fun get() = columnAccessTracker.getOrSet { ColumnAccessTracker() }
    }
}

internal val columnAccessTracker = ThreadLocal<ColumnAccessTracker>()

fun trackColumnAccess(body: () -> Unit): List<String> = ColumnAccessTracker.get().track(body)