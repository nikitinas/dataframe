package org.jetbrains.dataframe.internal.codeGen

import org.jetbrains.dataframe.internal.schema.DataFrameSchema

internal open class Marker(
    val name: String,
    val isOpen: Boolean,
    val fields: List<GeneratedField>,
    base: List<Marker>
) {

    val shortName: String
        get() = name.substringAfterLast(".")

    val baseMarkers = base.associateBy { it.name }

    val allBaseMarkers: Map<String, Marker> by lazy {
        val result = baseMarkers.toMutableMap()
        baseMarkers.forEach {
            result.putAll(it.value.allBaseMarkers)
        }
        result
    }

    val allFields: List<GeneratedField> by lazy {

        val fieldsMap = mutableMapOf<String, GeneratedField>()
        baseMarkers.values.forEach {
            it.allFields.forEach {
                fieldsMap[it.fieldName] = it
            }
        }
        fields.forEach {
            fieldsMap[it.fieldName] = it
        }
        fieldsMap.values.sortedBy { it.fieldName }
    }

    val allFieldsByColumn by lazy {
        allFields.associateBy { it.columnName }
    }

    fun containsColumn(columnName: String) = allFieldsByColumn.containsKey(columnName)

    val columnNames get() = allFields.map { it.columnName }

    val schema by lazy { DataFrameSchema(allFields.map { it.columnName to it.columnSchema }.toMap()) }

    fun implements(schema: Marker): Boolean = if (schema.name == name) true else baseMarkers[schema.name]?.let { it === schema } ?: false

    fun implementsAll(schemas: Iterable<Marker>): Boolean = schemas.all { implements(it) }
}
