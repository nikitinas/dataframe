package org.jetbrains.dataframe.impl.codeGen

import org.jetbrains.dataframe.AnyFrame
import org.jetbrains.dataframe.ColumnKind
import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.DataFrameBase
import org.jetbrains.dataframe.DataRow
import org.jetbrains.dataframe.DataRowBase
import org.jetbrains.dataframe.annotations.ColumnName
import org.jetbrains.dataframe.annotations.DataSchema
import org.jetbrains.dataframe.columns.ColumnGroup
import org.jetbrains.dataframe.impl.schema.ColumnSchema
import org.jetbrains.dataframe.impl.schema.DataFrameSchema
import org.jetbrains.dataframe.impl.schema.extractSchema
import org.jetbrains.dataframe.keywords.HardKeywords
import org.jetbrains.dataframe.keywords.ModifierKeywords
import org.jetbrains.dataframe.stubs.DataFrameToListNamedStub
import org.jetbrains.dataframe.stubs.DataFrameToListTypedStub
import org.jetbrains.kotlinx.jupyter.api.Code
import kotlin.reflect.KClass
import kotlin.reflect.KMutableProperty
import kotlin.reflect.KProperty
import kotlin.reflect.KType
import kotlin.reflect.full.allSuperclasses
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.hasAnnotation
import kotlin.reflect.full.isSubtypeOf
import kotlin.reflect.full.superclasses
import kotlin.reflect.jvm.jvmErasure


internal class CodeGeneratorImpl : CodeGenerator {

    companion object {

        internal val GroupedColumnType: KClass<*> = ColumnGroup::class

        internal val GroupedFieldType: KClass<*> = DataRow::class

        internal val DataFrameFieldType: KClass<*> = DataFrame::class

    }

    private fun String.needsQuoting(): Boolean {
        return contains(charsToQuote)
                || HardKeywords.VALUES.contains(this)
                || ModifierKeywords.VALUES.contains(this)
    }

    private fun String.quoteIfNeeded() = if(needsQuoting()) "`$this`" else this

    private fun getFields(marker: KClass<*>, withBaseTypes: Boolean): Map<String, FieldInfo> {
        val result = mutableMapOf<String, FieldInfo>()
        if (withBaseTypes)
            marker.superclasses.forEach { result.putAll(getFields(it, withBaseTypes)) }

        result.putAll(marker.declaredMemberProperties.mapIndexed { index, it ->
            val fieldName = it.name.quoteIfNeeded()
            val columnName = it.findAnnotation<ColumnName>()?.name ?: fieldName
            var valueType = it.returnType
            val valueClass = valueType.jvmErasure
            var marker: GeneratedMarker? = null
            var columnKind = when(valueClass) {
                GroupedFieldType -> ColumnKind.Map
                DataFrameFieldType -> ColumnKind.Frame
                else -> ColumnKind.Value
            }
            if(columnKind != ColumnKind.Value) {
                val typeArgument = valueType.arguments[0].type!!
                if (isMarkerType(typeArgument.jvmErasure)) {
                    marker = getMarkerScheme(typeArgument.jvmErasure)
                    valueType = typeArgument
                }
                else columnKind = ColumnKind.Value
            }
            fieldName to FieldInfo(fieldName, columnName, valueType, columnKind, marker?.fullScheme)
        })
        return result
    }

    private fun getScheme(marker: KClass<*>, withBaseTypes: Boolean) = CodeGenSchema(getFields(marker, withBaseTypes).values.toList())

    private val charsToQuote = """[ {}()<>'"/|.\\!?@:;%^&*#$-]""".toRegex()

    private fun generateValidFieldName(name: String, index: Int, usedNames: Collection<String>): String {
        var result = name
        val needsQuote = name.needsQuoting()
        if (needsQuote) {
            result = name.replace("<", "{")
                    .replace(">", "}")
                    .replace("::", " - ")
                    .replace(": ", " - ")
                    .replace(":", " - ")
                    .replace(".", " ")
                    .replace("/", "-")
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

    private fun DataFrameSchema.toCodegen(): CodeGenSchema {
        val generatedFieldNames = mutableSetOf<String>()
        return CodeGenSchema(columns.asIterable().sortedBy { it.key }.mapIndexed { index, it ->
            val columnName = it.key
            val columnType = it.value
            val fieldName = generateValidFieldName(columnName, index, generatedFieldNames)
            generatedFieldNames.add(fieldName)
            var type: KType? = null
            var childScheme : CodeGenSchema? = null
            var columnKind = ColumnKind.Value
            when(columnType) {
                is ColumnSchema.Value -> {
                    type = columnType.type
                }
                is ColumnSchema.Map -> {
                    childScheme = columnType.schema.toCodegen()
                    type = null
                    columnKind = ColumnKind.Map
                }
                is ColumnSchema.Frame -> {
                    childScheme = columnType.schema.toCodegen()
                    type = null
                    columnKind = ColumnKind.Frame
                }
            }
            FieldInfo(fieldName, columnName, type, columnKind, childScheme)
        })
    }

    private val AnyFrame.schema: CodeGenSchema
        get() = extractSchema().toCodegen()

    // Rendering

    override var mode = CodeGenerationMode.FullNames

    private fun shortTypeName(type: KType) =
            if (type.arguments.isNotEmpty()) null
            else type.jvmErasure.simpleName + if (type.isMarkedNullable) "?" else ""

    private fun render(clazz: KClass<*>) = when (mode) {
        CodeGenerationMode.ShortNames -> clazz.simpleName
        CodeGenerationMode.FullNames -> clazz.qualifiedName
    }

    private fun render(type: KType) = when (mode) {
        CodeGenerationMode.FullNames -> type.toString()
        CodeGenerationMode.ShortNames -> shortTypeName(type) ?: type.toString()
    }

    fun renderColumnName(name: String) = name
            .replace("\\", "\\\\")
            .replace("$", "\\\$")
            .replace("\"", "\\\"")

    // Generated marker interfaces tracking

    private data class GeneratedMarker(val fullScheme: CodeGenSchema, val ownScheme: CodeGenSchema, val kclass: KClass<*>, val isOpen: Boolean)

    private val registeredMarkers = mutableMapOf<KClass<*>, GeneratedMarker>()

    private val registeredMarkerClassNames = mutableSetOf<String>()

    private fun CodeGenSchema.getAllBaseMarkers() = registeredMarkers.values
            .filter { it.fullScheme.compare(this).isSuperOrEqual() }

    private fun List<GeneratedMarker>.onlyLeafs(): List<GeneratedMarker> {
        val skip = flatMap { it.kclass.allSuperclasses }.toSet()
        return filter { !skip.contains(it.kclass) }
    }

    private fun CodeGenSchema.getRequiredBaseMarkers() = registeredMarkers.values
            .filter { it.isOpen && it.fullScheme.compare(this).isSuperOrEqual() }

    // Code Generation

    private fun String.removeQuotes() = this.removeSurrounding("`")

    private fun generateExtensionProperties(scheme: CodeGenSchema, markerType: String): Code? {

        val shortMarkerName = markerType.substring(markerType.lastIndexOf('.')+1)
        fun generatePropertyCode(typeName: String, name: String, propertyType: String, getter: String): String {
            val jvmName = "${shortMarkerName}_${name.removeQuotes()}"
            return "val $typeName.$name: $propertyType @JvmName(\"$jvmName\") get() = $getter as $propertyType"
        }

        val declarations = mutableListOf<String>()
        val dfTypename = render(DataFrameBase::class) + "<$markerType>"
        val rowTypename = render(DataRowBase::class) + "<$markerType>"
        scheme.values.sortedBy { it.columnName }.forEach { field ->
            val getter = "this[\"${field.columnName}\"]"
            val name = field.fieldName
            val fieldType = render(field.fieldType)
            val columnType = render(field.columnType)
            declarations.add(generatePropertyCode(dfTypename, name, columnType, getter))
            declarations.add(generatePropertyCode(rowTypename, name, fieldType, getter))
        }
        if(declarations.isEmpty()) return null
        return declarations.joinToString("\n")
    }

    private fun isMarkerType(marker: KClass<*>) = marker.hasAnnotation<DataSchema>()

    private fun getMarkerScheme(marker: KClass<*>) =
        registeredMarkers.getOrPut(marker) {
            val annotation = marker.findAnnotation<DataSchema>() ?: throw Exception()
            val fullSet = getScheme(marker, withBaseTypes = true)
            val ownProperties = marker.declaredMemberProperties.map { it.name.quoteIfNeeded() }.toSet()
            val ownSet = CodeGenSchema(fullSet.values.filter { ownProperties.contains(it.fieldName) })
            val simpleName = marker.simpleName!!
            registeredMarkerClassNames.add(simpleName)
            GeneratedMarker(fullSet, ownSet, marker, annotation.isOpen)
        }

    override fun generateExtensionProperties(marker: KClass<*>): Code? {
        val generatedMarker = getMarkerScheme(marker)
        val qualifiedName = marker.qualifiedName!!
        return generateExtensionProperties(generatedMarker.ownScheme, qualifiedName)
    }

    private val processedProperties = mutableSetOf<KProperty<*>>()

    private fun generateUniqueMarkerClassName(prefix: String, usedNames: Set<String> = setOf()): String {
        var id = 1
        while (registeredMarkerClassNames.contains("$prefix$id") || usedNames.contains("$prefix$id"))
            id++
        return "$prefix$id"
    }

    private fun KClass<*>.implements(targetBaseMarkers: Iterable<KClass<*>>): Boolean {
        val superclasses = allSuperclasses + this
        return targetBaseMarkers.all { superclasses.contains(it) }
    }

    override fun generate(df: AnyFrame, property: KProperty<*>?): GeneratedCode? {

        var targetScheme = df.schema
        var isMutable = false

        if(property != null) {
            val wasProcessedBefore = property in processedProperties
            processedProperties.add(property)
            isMutable = property is KMutableProperty

            // maybe property is already properly typed, let's do some checks
            val currentMarkerType = getMarker(property.returnType)
            if (currentMarkerType != null) {
                // if property is mutable, we need to make sure that its marker type is open in order to let data frames with more columns be assignable to it
                if (!isMutable || currentMarkerType.findAnnotation<DataSchema>()?.isOpen == true) {
                    val markerScheme = getScheme(currentMarkerType, withBaseTypes = true)
                    // for mutable properties we do strong typing only at the first processing, after that we allow its type to be more general than actual data frame type
                    if (wasProcessedBefore || markerScheme.compare(targetScheme).isEqual()) {
                        // property scheme is valid for current data frame, but we should also check that all compatible open markers are implemented by it
                        val requiredBaseMarkers = markerScheme.getRequiredBaseMarkers().map { it.kclass }
                        if (currentMarkerType.implements(requiredBaseMarkers))
                            return null
                        // use current marker scheme as a target for generation of new marker interface, so that available properties won't change
                        targetScheme = markerScheme
                    }
                }
            }
        }

        return generate(targetScheme, GenerationOptions(isMutable, false))
    }

    private fun generate(scheme: CodeGenSchema, options: GenerationOptions): GeneratedCode {
        val declarations = mutableListOf<String>()
        val markerType = findOrCreateMarker(scheme, declarations, options)
        return GeneratedCode(declarations.joinToString("\n")) { "$it.typed<$markerType>()" }
    }

    data class GenerationOptions(val isMutable: Boolean, val generateExtensionProperties: Boolean, val markerNamePrefix: String = "DataFrameType", val usedNames: MutableSet<String> = mutableSetOf())

    private fun findOrCreateMarker(targetScheme: CodeGenSchema, declarations: MutableList<Code>, options: GenerationOptions): String {
        val markerName: String
        val requiredBaseMarkers = targetScheme.getRequiredBaseMarkers().map { it.kclass }
        val existingMarker = registeredMarkers.values.firstOrNull {
            (!options.isMutable || it.isOpen) && it.fullScheme.compare(targetScheme).isEqual() && it.kclass.implements(requiredBaseMarkers)
        }
        if (existingMarker != null) {
            markerName = existingMarker.kclass.qualifiedName!!
        } else {
            markerName = generateUniqueMarkerClassName(options.markerNamePrefix, options.usedNames)
            options.usedNames.add(markerName)
            declarations.addAll(generateInterfaceDeclarations(targetScheme, markerName, true, options))
        }
        return markerName
    }

    private fun getMarker(dataFrameType: KType) =
            when (dataFrameType.jvmErasure) {
                DataFrame::class -> dataFrameType.arguments[0].type?.jvmErasure
                else -> null
            }

    private enum class FieldGenerationMode { declare, override, skip }

    private fun computeFieldGenerationModes(scheme: CodeGenSchema, requiredBaseMarkers: List<GeneratedMarker>): List<Pair<FieldInfo, FieldGenerationMode>> {
        return scheme.values.map { field ->
            val fieldName = field.fieldName
            var generationMode = FieldGenerationMode.declare
            for (baseScheme in requiredBaseMarkers) {
                val baseField = baseScheme.fullScheme.byField[fieldName]
                if (baseField != null) {

                    val childSchemeComparison = if(field.childScheme != null) {
                        if(baseField.childScheme != null) field.childScheme.compare(baseField.childScheme) else CompareResult.Equals
                    }
                    else if(baseField.childScheme == null) CompareResult.None else CompareResult.Equals

                    generationMode = if (baseField.fieldType == field.fieldType || (baseField.columnKind == field.columnKind && childSchemeComparison == CompareResult.Equals)) FieldGenerationMode.skip
                    else if (field.fieldType.isSubtypeOf(baseField.fieldType) || (baseField.columnKind == field.columnKind && childSchemeComparison == CompareResult.IsDerived)) {
                        generationMode = FieldGenerationMode.override
                        break
                    } else throw Exception()
                }
            }
            field to generationMode
        }
    }

    private val annotationName = DataSchema::class.simpleName

    private fun generateInterfaceDeclarations(scheme: CodeGenSchema, name: String, withBaseInterfaces: Boolean, options: GenerationOptions): List<String> {

        val markers = mutableListOf<GeneratedMarker>()
        val fields = if (withBaseInterfaces) {
            markers += scheme.getRequiredBaseMarkers().onlyLeafs()
            val generatedFields = computeFieldGenerationModes(scheme, markers)

            // try to reduce number of generated fields by implementing some additional interfaces
            val remainedFields = generatedFields.filter { it.second == FieldGenerationMode.declare }.map { it.first }.toMutableList()
            var markersAdded = false

            if (remainedFields.size > 0) {
                val availableMarkers = scheme.getAllBaseMarkers().toMutableList()
                availableMarkers -= markers

                while (remainedFields.size > 0) {
                    val bestMarker = availableMarkers.map { marker -> marker to remainedFields.count { marker.fullScheme.contains(it) } }.maxBy { it.second }
                    if (bestMarker != null && bestMarker.second > 0) {
                        remainedFields.removeAll { bestMarker.first.fullScheme.byField[it.fieldName]?.fieldType == it.fieldType }
                        markers += bestMarker.first
                        markersAdded = true
                        availableMarkers -= bestMarker.first
                    } else break
                }
            }
            if (markersAdded) computeFieldGenerationModes(scheme, markers) else generatedFields
        } else scheme.values.map { it to FieldGenerationMode.declare }

        val leafMarkers = markers.onlyLeafs()
        val header = "@$annotationName${if(options.isMutable) "" else "(isOpen = false)"}\ninterface $name"
        val baseInterfacesDeclaration = if (leafMarkers.isNotEmpty()) " : " + leafMarkers.map { it.kclass.qualifiedName!! }.joinToString() else ""
        val resultDeclarations = mutableListOf<String>()

        val newOptions = options.copy(isMutable = false)

        val fieldsDeclaration = fields.filter { it.second != FieldGenerationMode.skip }.map {
            val field = it.first
            val override = when (it.second) {
                FieldGenerationMode.declare -> ""
                FieldGenerationMode.override -> "override "
                FieldGenerationMode.skip -> throw Exception()
            }
            val columnNameAnnotation = if (field.columnName != field.fieldName) "\t@ColumnName(\"${renderColumnName(field.columnName)}\")\n" else ""

            val fieldType = when(field.columnKind) {
                ColumnKind.Map -> {
                    val markerType = findOrCreateMarker(field.childScheme!!, resultDeclarations, newOptions)
                    "${render(GroupedFieldType)}<$markerType>"
                }
                ColumnKind.Frame -> {
                    val markerType = findOrCreateMarker(field.childScheme!!, resultDeclarations, newOptions)
                    "${render(DataFrameFieldType)}<$markerType>"
                }
                else -> render(field.fieldType)
            }

            "${columnNameAnnotation}    ${override}val ${field.fieldName}: $fieldType"
        }.joinToString("\n")
        val body = if (fieldsDeclaration.isNotBlank()) "{\n$fieldsDeclaration\n}" else ""
        resultDeclarations.add(header + baseInterfacesDeclaration + body)

        if(options.generateExtensionProperties)
            generateExtensionProperties(scheme, name)?.let(resultDeclarations::add)
        return resultDeclarations
    }

    internal fun generateInterfaceDeclarations(df: AnyFrame, name: String, generateExtensionProperties: Boolean) = generateInterfaceDeclarations(df.extractSchema().toCodegen(), name, false, GenerationOptions(true, generateExtensionProperties, name))

    // DataFrame -> List converters

    private fun generateToListConverter(className: String, columnNames: List<String>, scheme: CodeGenSchema, interfaceName: String? = null): GeneratedCode {
        val override = if (interfaceName != null) "override " else ""
        val baseTypes = if (interfaceName != null) " : $interfaceName" else ""
        val classDeclaration = "data class ${className}(" +
                columnNames.map {
                    val field = scheme.byColumn[it]!!
                    "${override}val ${field.fieldName}: ${render(field.fieldType)}"
                }.joinToString() + ") " + baseTypes

        fun converter(argumentName: String) = "$argumentName.df.rows.map { $className(" +
                columnNames.map {
                    val field = scheme.byColumn[it]!!
                    "it[\"${field.columnName}\"] as ${render(field.fieldType)}"
                }.joinToString() + ")}"

        return GeneratedCode(classDeclaration, ::converter)
    }

    override fun generate(stub: DataFrameToListTypedStub): GeneratedCode {
        val df = stub.df
        val dfScheme = df.schema
        val interfaceScheme = getScheme(stub.interfaceClass, withBaseTypes = true)
        if (!interfaceScheme.compare(dfScheme).isSuperOrEqual())
            throw Exception()
        val interfaceName = stub.interfaceClass.simpleName!!
        val interfaceFullName = stub.interfaceClass.qualifiedName!!
        val className = interfaceName + "Impl"
        val columnNames = interfaceScheme.byColumn.keys.toList()

        return generateToListConverter(className, columnNames, dfScheme, interfaceFullName)
    }

    override fun generate(stub: DataFrameToListNamedStub) =
            generateToListConverter(stub.className, stub.df.columns().map { it.name() }, stub.df.schema, null)

}
