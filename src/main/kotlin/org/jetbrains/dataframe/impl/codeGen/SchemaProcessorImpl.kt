package org.jetbrains.dataframe.impl.codeGen

import org.jetbrains.dataframe.internal.codeGen.GeneratedField
import org.jetbrains.dataframe.internal.codeGen.Marker
import org.jetbrains.dataframe.internal.codeGen.SchemaProcessor
import org.jetbrains.dataframe.internal.schema.ColumnSchema
import org.jetbrains.dataframe.internal.schema.DataFrameSchema

internal class SchemaProcessorImpl(
    existingMarkers: Iterable<Marker>,
    override val namePrefix: String
) : SchemaProcessor {

    private val registeredMarkers = existingMarkers.toMutableList()

    private val usedMarkerNames = existingMarkers.map { it.shortName }.toMutableSet()

    override val generatedMarkers = mutableListOf<Marker>()

    private fun DataFrameSchema.getAllSuperMarkers() = registeredMarkers
        .filter { it.schema.compare(this).isSuperOrEqual() }

    private fun List<Marker>.onlyLeafs(): List<Marker> {
        val skip = flatMap { it.allBaseMarkers.keys }.toSet()
        return filter { !skip.contains(it.name) }
    }

    private fun generateValidFieldName(columnName: String, index: Int, usedNames: Collection<String>): String {
        var result = columnName
        val needsQuote = columnName.needsQuoting()
        if (needsQuote) {
            result = columnName.replace("<", "{")
                .replace(">", "}")
                .replace("::", " - ")
                .replace(": ", " - ")
                .replace(":", " - ")
                .replace(".", " ")
                .replace("/", "-")
                .replace("[", "{")
                .replace("]", "}")
                .replace("(", "{")
                .replace(")", "}")
        }
        if (result.isEmpty()) result = "_$index"
        val baseName = result
        result = if (needsQuote) "`$baseName`" else baseName
        var attempt = 2
        while (usedNames.contains(result)) {
            result = if (needsQuote) "`$baseName ($attempt)`" else "${baseName}_$attempt"
            attempt++
        }
        return result
    }

    private fun generateUniqueMarkerClassName(prefix: String): String {
        if (!usedMarkerNames.contains(prefix)) return prefix
        var id = 1
        while (usedMarkerNames.contains("$prefix$id"))
            id++
        return "$prefix$id"
    }

    private fun generateFields(
        schema: DataFrameSchema,
        requiredSuperMarkers: List<Marker> = emptyList()
    ): List<GeneratedField> {
        val usedFieldNames = requiredSuperMarkers.flatMap { it.allFields.map { it.fieldName } }.toMutableSet()

        fun getMarker(column: ColumnSchema) = when (column) {
            is ColumnSchema.Value -> null
            is ColumnSchema.Map -> process(column.schema, false)
            is ColumnSchema.Frame -> process(column.schema, false)
            else -> throw NotImplementedError()
        }

        return schema.sortedColumns.flatMapIndexed { index, column ->
            val (columnName, columnSchema) = column

            // find all fields that were already generated for this column name in base interfaces
            val superFields = requiredSuperMarkers.mapNotNull { it.allFieldsByColumn[columnName] }

            val fieldsToOverride = superFields.filter { it.columnSchema != columnSchema }

            val newFields = when {
                fieldsToOverride.isNotEmpty() -> fieldsToOverride.map {
                    GeneratedField(it.fieldName, it.columnName, true, columnSchema, getMarker(columnSchema)?.name)
                }
                superFields.isNotEmpty() -> emptyList()
                else -> {
                    val fieldName = generateValidFieldName(columnName, index, usedFieldNames)
                    usedFieldNames.add(fieldName)
                    listOf(GeneratedField(fieldName, columnName, false, columnSchema, getMarker(columnSchema)?.name))
                }
            }
            newFields
        }
    }

    private fun createMarkerSchema(
        scheme: DataFrameSchema,
        name: String,
        withBaseInterfaces: Boolean,
        isOpen: Boolean
    ): Marker {
        val baseMarkers = mutableListOf<Marker>()
        val fields = if (withBaseInterfaces) {
            baseMarkers += scheme.getRequiredMarkers().onlyLeafs()

            val columnNames = scheme.columns.keys
            val superColumns = baseMarkers.flatMap { it.allFields.map { it.columnName } }.toSet()

            val newColumns = (columnNames - superColumns).toMutableSet()

            if (newColumns.isNotEmpty()) {
                val availableMarkers = scheme.getAllSuperMarkers().toMutableList()
                availableMarkers -= baseMarkers

                while (newColumns.size > 0) {
                    val bestMarker =
                        availableMarkers.map { marker -> marker to newColumns.count { marker.containsColumn(it) } }
                            .maxByOrNull { it.second }
                    if (bestMarker != null && bestMarker.second > 0) {
                        newColumns.removeAll(bestMarker.first.columnNames)
                        baseMarkers += bestMarker.first
                        availableMarkers -= bestMarker.first
                    } else break
                }
            }
            generateFields(scheme, baseMarkers)
        } else generateFields(scheme)
        return Marker(name, isOpen, fields, baseMarkers.onlyLeafs())
    }

    private fun DataFrameSchema.getRequiredMarkers() = getRequiredMarkers(this, registeredMarkers)

    override fun process(
        schema: DataFrameSchema,
        isOpen: Boolean
    ): Marker {
        val markerName: String
        val required = schema.getRequiredMarkers()
        val existingMarker = registeredMarkers.firstOrNull {
            (!isOpen || it.isOpen) && it.schema == schema && it.implementsAll(required)
        }
        if (existingMarker != null) {
            return existingMarker
        } else {
            markerName = generateUniqueMarkerClassName(namePrefix)
            usedMarkerNames.add(markerName)
            val marker = createMarkerSchema(schema, markerName, true, isOpen)
            registeredMarkers.add(marker)
            generatedMarkers.add(marker)
            return marker
        }
    }
}
