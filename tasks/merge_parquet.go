package tasks

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/apache/arrow/go/v18/parquet/file"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
	"log"
	"os"
	"strings"
)

func arrowSchemaToClickhouseSQL(arrowSchema *arrow.Schema, tableName string, engine string, orderBy string) string {
	columnDefinitions := make([]string, 0, len(arrowSchema.Fields()))

	for _, field := range arrowSchema.Fields() {
		//to skip columns lens <=1 like % /
		if len(field.Name) <= 1 {
			continue
		}
		columnName := field.Name
		arrowType := field.Type
		clickhouseType := mapArrowTypeToClickhouse(arrowType)
		nullable := field.Nullable

		if nullable && !strings.HasPrefix(clickhouseType, "Nullable(") {
			clickhouseType = fmt.Sprintf("Nullable(%s)", clickhouseType)
		}

		columnDefinitions = append(columnDefinitions, fmt.Sprintf("    %s %s default NULL", columnName, clickhouseType))
	}

	createTableSQL := fmt.Sprintf("CREATE TABLE \"%s\" (%s) ENGINE = %s;\n", tableName, strings.Join(columnDefinitions, ",\n"), engine)

	if strings.HasPrefix(engine, "MergeTree") && orderBy != "" {
		createTableSQL += fmt.Sprintf("ORDER BY %s", orderBy)
	}

	return createTableSQL
}

func mapArrowTypeToClickhouse(arrowType arrow.DataType) string {
	switch arrowType.ID() {
	case arrow.INT8:
		return "Int8"
	case arrow.INT16:
		return "Int16"
	case arrow.INT32:
		return "Int32"
	case arrow.INT64:
		return "Int64"
	case arrow.UINT8:
		return "UInt8"
	case arrow.UINT16:
		return "UInt16"
	case arrow.UINT32:
		return "UInt32"
	case arrow.UINT64:
		return "UInt64"
	case arrow.FLOAT16:
		return "Float32" // ClickHouse doesn't have Float16
	case arrow.FLOAT32:
		return "Float32"
	case arrow.FLOAT64:
		return "Float64"
	case arrow.BOOL:
		return "Bool"
	case arrow.STRING:
		return "String"
	case arrow.BINARY:
		return "String" // Or FixedString(length) if known length
	case arrow.DATE32:
		return "Date"
	case arrow.DATE64:
		return "DateTime"
	case arrow.TIMESTAMP:
		return "DateTime64" // Consider specifying timezone
	case arrow.NULL:
		return "Nullable(String)" // Sensible Default
	default:
		log.Printf("Unsupported Arrow data type: %v", arrowType)
		return "String" // Sensible default, can error instead
	}
}

func SchemaToClickhouseSQL(arrowSchema *arrow.Schema) string {
	// Generate the ClickHouse CREATE TABLE statement
	tableName := "temporary_table"
	createTableSQL := arrowSchemaToClickhouseSQL(arrowSchema, tableName, "Memory", "")
	//fmt.Println(createTableSQL)
	return createTableSQL
	//// Example usage with Decimal128
	//decimalVal := decimal128.New(123456789, 2) // Example decimal value
	//_ = decimalVal                             // Use decimalVal to prevent "declared and not used" error
	//
	//// Example usage with Float16
	//float16Val := float16.New(1.23)
	//_ = float16Val // Use float16Val to prevent "declared and not used" error
}

func MergeSchema(schema1, schema2 *arrow.Schema) (*arrow.Schema, error) {
	fields := make([]arrow.Field, 0)
	fieldMap := make(map[string]arrow.Field)

	// Add fields from the first schema
	if schema1 != nil {
		for _, field := range schema1.Fields() {
			fields = append(fields, field)
			fieldMap[field.Name] = field
		}
	}

	// Add fields from the second schema that are not already in the first schema
	for _, field := range schema2.Fields() {
		if _, exists := fieldMap[field.Name]; !exists {
			fields = append(fields, field)
			fieldMap[field.Name] = field // Ensure the field is added to the map even if not present in the first schema.
		} else {
			// Check for type compatibility if the field exists in both schemas.
			existingField := fieldMap[field.Name]
			if existingField.Type.ID() != field.Type.ID() {
				log.Printf("Warning: Conflicting types for field '%s'. Using type from first schema: %s, ignoring type from second schema: %s",
					field.Name, existingField.Type.String(), field.Type.String())
				// Ideally, resolve the type conflict by promoting to a common type, casting, or other strategies.
				// For simplicity, we use the type from the first schema as-is.
			}
		}
	}

	// Create a new schema from the combined fields
	return arrow.NewSchema(fields, nil), nil
}

func GetParquetSchema(parquetPath string) (*arrow.Schema, error) {
	f, err := os.Open(parquetPath)
	if err != nil {
		fmt.Errorf("failed to open Parquet file: %w", err)
		return nil, err
	}
	defer f.Close()

	// Create a new ParquetReader
	reader, err := file.NewParquetReader(f)
	if err != nil {
		fmt.Errorf("Error creating Parquet reader: %w", err)
		return nil, err
	}
	defer reader.Close()
	mem := memory.NewGoAllocator()

	// Create a ParquetFileReader to Arrow converter
	arrowReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{}, mem)
	if err != nil {
		fmt.Printf("Error creating Arrow reader: %v\n", err)
		return nil, err
	}
	// Get the Arrow schema
	arrowSchema, err := arrowReader.Schema()
	return arrowSchema, nil
}

func FileExists(filename string) bool {
	_, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return true
}

func MergeParquetFile(ctx context.Context, parquet1, parquet2, parquetOut string, chdburl string) error {
	var sc1, sc2 *arrow.Schema
	var err error
	if FileExists(parquet1) {
		sc1, err = GetParquetSchema(parquet1)
		if err != nil {
			fmt.Printf("Error get file schema : %s\n,%v\n", parquet1, err)
			return err
		}
	}

	if FileExists(parquet2) {
		sc2, err = GetParquetSchema(parquet2)
		if err != nil {
			fmt.Printf("Error get file schema : %s\n,%v\n", parquet2, err)
			return err
		}
	}

	allSchema, err := MergeSchema(sc1, sc2)
	if err != nil {
		fmt.Printf("Error merge file schema : %s   %s\n,%v\n", parquet1, parquet2, err)
		return err
	}
	generateSql := SchemaToClickhouseSQL(allSchema)
	if FileExists(parquet1) {
		generateSql = generateSql + fmt.Sprintf("INSERT INTO temporary_table FROM INFILE '%s' FORMAT Parquet;\n", parquet1)
	}
	if FileExists(parquet2) {
		generateSql = generateSql + fmt.Sprintf("INSERT INTO temporary_table FROM INFILE '%s' FORMAT Parquet;\n", parquet2)
	}
	generateSql = generateSql + fmt.Sprintf("SELECT * FROM temporary_table order by time INTO OUTFILE '%s' TRUNCATE FORMAT Parquet;", parquetOut)
	//log.Println(generateSql)

	result := DoSqlProcess(generateSql)
	//check if the generate file influence the original select
	if len(result) == 0 {
		err := os.Remove(parquet2)
		if err != nil {
			fmt.Println("Error deleting file:", err)
		}
		return nil
	} else {
		return errors.New(string(result))
	}
}
