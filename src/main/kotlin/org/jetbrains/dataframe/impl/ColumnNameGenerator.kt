package org.jetbrains.dataframe.impl

import org.jetbrains.dataframe.AnyFrame
import org.jetbrains.dataframe.columns.DataColumn

internal class ColumnNameGenerator(columnNames: List<String> = emptyList()) {

    private val usedNames = columnNames.toMutableSet()

    private val colNames = columnNames.toMutableList()

    fun addUnique(preferredName: String): String {
        var name = preferredName
        var k = 1
        while (usedNames.contains(name)) {
            name = "${preferredName}_${k++}"
        }
        usedNames.add(name)
        colNames.add(name)
        return name
    }

    fun addIfAbsent(name: String) {
        if (!usedNames.contains(name)) {
            usedNames.add(name)
            colNames.add(name)
        }
    }

    val names: List<String>
        get() = colNames

    fun contains(name: String) = usedNames.contains(name)
}

internal fun AnyFrame.nameGenerator() = ColumnNameGenerator(columnNames())

internal fun nameGenerator(vararg usedNames: String) = ColumnNameGenerator(usedNames.asList())

internal fun <C> DataColumn<C>.ensureUniqueName(nameGenerator: ColumnNameGenerator) = rename(nameGenerator.addUnique(name()))