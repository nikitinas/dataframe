package org.jetbrains.dataframe.impl.codeGen

import org.jetbrains.dataframe.ColumnKind
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.createType
import kotlin.reflect.KType
import kotlin.reflect.full.isSubtypeOf
import kotlin.reflect.full.isSupertypeOf

internal data class FieldInfo(val fieldName: String, val columnName: String, private val type: KType?, val columnKind: ColumnKind = ColumnKind.Value, val childScheme: CodeGenSchema? = null) {

    init {
        when(columnKind) {
            ColumnKind.Value -> assert(type != null && childScheme == null)
            ColumnKind.Map -> assert(childScheme != null)
            ColumnKind.Frame -> assert(childScheme != null)
        }
    }

    val columnType: KType
        get() = when(columnKind) {
        ColumnKind.Value -> DataColumn::class.createType(type!!)
        ColumnKind.Map -> CodeGeneratorImpl.GroupedColumnType.createType(type)
        ColumnKind.Frame -> DataColumn::class.createType(CodeGeneratorImpl.DataFrameFieldType.createType(type))
    }

    val fieldType: KType
        get() = when(columnKind) {
        ColumnKind.Value -> type!!
        ColumnKind.Map -> CodeGeneratorImpl.GroupedFieldType.createType(type)
        ColumnKind.Frame -> CodeGeneratorImpl.DataFrameFieldType.createType(type)
    }

    fun compare(other: FieldInfo): CompareResult {
        if(fieldName != other.fieldName || columnName != other.columnName || columnKind != other.columnKind) return CompareResult.None
        if(childScheme == null) {
            if(other.childScheme != null) return CompareResult.None
            if(type == other.type) return CompareResult.Equals
            if(type!!.isSubtypeOf(other.type!!)) return CompareResult.IsDerived
            if(type.isSupertypeOf(other.type)) return CompareResult.IsSuper
            return CompareResult.None
        }
        if(other.childScheme == null) return CompareResult.None
        return childScheme.compare(other.childScheme)
    }
}