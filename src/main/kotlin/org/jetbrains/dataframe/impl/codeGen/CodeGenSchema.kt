package org.jetbrains.dataframe.impl.codeGen

internal class CodeGenSchema(val values: List<FieldInfo>) {

    val byColumn: Map<String, FieldInfo> by lazy { values.associateBy { it.columnName } }

    val byField: Map<String, FieldInfo> by lazy { values.associateBy { it.fieldName } }

    fun contains(field: FieldInfo) = byField[field.fieldName]?.equals(field) ?: false

    fun compare(other: CodeGenSchema): CompareResult {
        if(this === other) return CompareResult.Equals
        var result = CompareResult.Equals
        values.forEach {
            val otherField = other.byColumn[it.columnName]
            if(otherField == null)
                result = result.combine(CompareResult.IsDerived)
            else
                result = result.combine(it.compare(otherField))
            if(result == CompareResult.None) return result
        }
        other.values.forEach {
            val thisField = byColumn[it.columnName]
            if(thisField == null) {
                result = result.combine(CompareResult.IsSuper)
                if (result == CompareResult.None) return result
            }
        }
        return result
    }

    override fun hashCode(): Int {
        return values.sortedBy { it.fieldName }.hashCode()
    }

}