package org.jetbrains.dataframe.internal.codeGen

import org.jetbrains.dataframe.internal.schema.ColumnSchema

internal data class GeneratedField(
    val fieldName: String,
    val columnName: String,
    val overrides: Boolean,
    val columnSchema: ColumnSchema,
    val markerName: String?
) {
    val columnKind get() = columnSchema.kind
}
