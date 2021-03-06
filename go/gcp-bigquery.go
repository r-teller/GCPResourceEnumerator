package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	assetpb "google.golang.org/genproto/googleapis/cloud/asset/v1"
)

var BigqueryDebugLevel = DebugLevel(ERROR)
var InferSchemaDebugLevel = DebugLevel(ERROR)

func InferSchema(st interface{}) (bigquery.Schema, error) {
	var fieldSchema []bigquery.FieldSchema

	stTypeOf := reflect.TypeOf(st)
	stValueOf := reflect.ValueOf(st)

	for i := 0; i < stTypeOf.NumField(); i++ {
		if stTypeOf.Field(i).Tag.Get("json") == "-" {
			continue
		}
		name := stTypeOf.Field(i).Name
		kind := stTypeOf.Field(i).Type.Kind()
		value := stValueOf.Field(i)

		switch kind {
		case reflect.String:
			field := bigquery.FieldSchema{}
			field.Name = name
			field.Type = bigquery.StringFieldType
			field.Required = false
			field.Repeated = false
			fieldSchema = append(fieldSchema, field)
			if InferSchemaDebugLevel.EnumIndex() >= DebugLevel(TRACE).EnumIndex() {
				fmt.Printf("TRACE: InferSchema:String %s \n", field)
			}
		case reflect.Bool:
			field := bigquery.FieldSchema{}
			field.Name = name
			field.Type = bigquery.BooleanFieldType
			field.Required = false
			field.Repeated = false
			fieldSchema = append(fieldSchema, field)
			if InferSchemaDebugLevel.EnumIndex() >= DebugLevel(TRACE).EnumIndex() {
				fmt.Printf("TRACE: InferSchema:Bool %s \n", field)
			}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			field := bigquery.FieldSchema{}
			field.Name = name
			field.Type = bigquery.IntegerFieldType
			field.Required = false
			field.Repeated = false
			fieldSchema = append(fieldSchema, field)
			if InferSchemaDebugLevel.EnumIndex() >= DebugLevel(TRACE).EnumIndex() {
				fmt.Printf("TRACE: InferSchema:Int %s \n", field)
			}
		case reflect.Float32, reflect.Float64:
			field := bigquery.FieldSchema{}
			field.Name = name
			field.Type = bigquery.FloatFieldType
			field.Required = false
			field.Repeated = false
			fieldSchema = append(fieldSchema, field)
			if InferSchemaDebugLevel.EnumIndex() >= DebugLevel(TRACE).EnumIndex() {
				fmt.Printf("TRACE: InferSchema:Float32 %s \n", field)
			}
		case reflect.Map:
			// This Field is skipped because it is not supported by BQ
			// This could be implemented as two Key/Value String Fields
			if InferSchemaDebugLevel.EnumIndex() >= DebugLevel(WARN).EnumIndex() {
				fmt.Printf("WARNING: InferSchema %s is of type <%s> and is not currently supported. This item was passed from Parent: %s \n", name, kind, stTypeOf)
			}
		case reflect.Uint, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			// This Field is skipped because it is not supported by BQ
			if InferSchemaDebugLevel.EnumIndex() >= DebugLevel(WARN).EnumIndex() {
				fmt.Printf("WARNING: InferSchema %s is of type <%s> and is not currently supported. This item was passed from Parent: %s \n", name, kind, stTypeOf)
			}
		case reflect.Ptr:
			field := bigquery.FieldSchema{}
			field.Name = name
			field.Type = bigquery.RecordFieldType
			field.Required = false
			field.Repeated = false

			rvTypeOf := reflect.TypeOf(value.Interface())
			ptrKind := stTypeOf.Field(i).Type.Elem().Kind()
			switch ptrKind {
			case reflect.Struct:
				nestedFields := InferFields(rvTypeOf)
				for ii := range nestedFields {
					field.Schema = append(field.Schema, &nestedFields[ii])
				}
			case reflect.Ptr:
				fieldSchema = append(fieldSchema, InferField(name, rvTypeOf))
			case reflect.Uint, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				// This Field is skipped because it is not supported by BQ
				if InferSchemaDebugLevel.EnumIndex() >= DebugLevel(WARN).EnumIndex() {
					fmt.Printf("WARNING: InferFields:PTR %s is of type <%s> and is not currently supported. This item was passed from Parent: %s \n", name, ptrKind, stTypeOf)
				}
			default:
				if InferSchemaDebugLevel.EnumIndex() >= DebugLevel(ERROR).EnumIndex() {
					fmt.Printf("ERROR: InferSchema:PTR %s is of type <%s> and is not currently defined.\n", name, ptrKind)
				}
			}
			if InferSchemaDebugLevel.EnumIndex() >= DebugLevel(TRACE).EnumIndex() {
				fmt.Printf("TRACE: InferSchema:PTR %s \n", field)
			}
			fieldSchema = append(fieldSchema, field)
		case reflect.Slice:
			fieldSchema = append(fieldSchema, InferFieldSchema(name, value))
		default:
			if InferSchemaDebugLevel.EnumIndex() >= DebugLevel(ERROR).EnumIndex() {
				fmt.Printf("ERROR: InferSchema %s is of type <%s> and is not currently defined.\n", name, kind)
			}
		}
	}
	var schema bigquery.Schema
	for i := 0; i < len(fieldSchema); i++ {
		schema = append(schema, &fieldSchema[i])
	}
	if InferSchemaDebugLevel.EnumIndex() >= DebugLevel(TRACE).EnumIndex() {
		fieldSchemaJSON, _ := json.Marshal(fieldSchema)
		fmt.Printf("DEBUG: InferSchema:fieldSchema %s \n\n", string(fieldSchemaJSON))

		fmt.Printf("DEBUG: InferSchema:schema %s \n\n", schema)
	}
	return schema, nil
}

func InferFieldSchema(fieldName string, rv reflect.Value) bigquery.FieldSchema {
	rvTypeOf := reflect.TypeOf(rv.Interface()).Elem()
	kind := rvTypeOf.Kind()
	switch kind {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		field := bigquery.FieldSchema{}
		field.Name = fieldName
		field.Type = bigquery.IntegerFieldType
		field.Required = false
		field.Repeated = true
		if InferSchemaDebugLevel.EnumIndex() >= DebugLevel(TRACE).EnumIndex() {
			fmt.Printf("TRACE: InferFieldSchema:Int %s \n", field)
		}
		return field
	case reflect.String:
		field := bigquery.FieldSchema{}
		field.Name = fieldName
		field.Type = bigquery.StringFieldType
		field.Required = false
		field.Repeated = true
		if InferSchemaDebugLevel.EnumIndex() >= DebugLevel(TRACE).EnumIndex() {
			fmt.Printf("TRACE: InferFieldSchema:String %s \n", field)
		}
		return field
	case reflect.Ptr:
		field := bigquery.FieldSchema{}
		field.Name = fieldName
		field.Type = bigquery.RecordFieldType
		field.Required = false
		field.Repeated = true

		nestedFields := InferFields(rvTypeOf)
		for i := range nestedFields {
			field.Schema = append(field.Schema, &nestedFields[i])
		}
		if InferSchemaDebugLevel.EnumIndex() >= DebugLevel(TRACE).EnumIndex() {
			fmt.Printf("TRACE: InferFieldSchema:Ptr %s \n", field)
		}
		return field
	default:
		if InferSchemaDebugLevel.EnumIndex() >= DebugLevel(ERROR).EnumIndex() {
			fmt.Printf("ERROR: InferFieldSchema %s is of type <%s> and is not currently defined.\n", fieldName, kind)
		}
	}
	return bigquery.FieldSchema{}
}

func InferField(fieldName string, rt reflect.Type) bigquery.FieldSchema {

	kind := rt.Elem().Kind()

	switch kind {
	case reflect.Bool:
		field := bigquery.FieldSchema{}
		field.Name = fieldName
		field.Type = bigquery.BooleanFieldType
		field.Required = false
		field.Repeated = false
		if InferSchemaDebugLevel.EnumIndex() >= DebugLevel(TRACE).EnumIndex() {
			fmt.Printf("TRACE: InferField:Bool %s \n", field)
		}
		return field
	case reflect.String:
		field := bigquery.FieldSchema{}
		field.Name = fieldName
		field.Type = bigquery.StringFieldType
		field.Required = false
		field.Repeated = false
		if InferSchemaDebugLevel.EnumIndex() >= DebugLevel(TRACE).EnumIndex() {
			fmt.Printf("TRACE: InferField:String %s \n", field)
		}
		return field
	default:
		if InferSchemaDebugLevel.EnumIndex() >= DebugLevel(ERROR).EnumIndex() {
			fmt.Printf("ERROR: InferField %s is of type <%s> and is not currently defined.\n", fieldName, kind)
		}
	}
	return bigquery.FieldSchema{}
}

func InferFields(rt reflect.Type) []bigquery.FieldSchema {
	parent := rt.Elem().Name()

	var schema []bigquery.FieldSchema
	for i := 0; i < rt.Elem().NumField(); i++ {
		if rt.Elem().Field(i).Tag.Get("json") == "-" {
			continue
		}
		name := rt.Elem().Field(i).Name
		kind := rt.Elem().Field(i).Type.Kind()

		switch kind {
		case reflect.String:
			field := bigquery.FieldSchema{}
			field.Name = name
			field.Type = bigquery.StringFieldType
			field.Required = false
			field.Repeated = false
			if InferSchemaDebugLevel.EnumIndex() >= DebugLevel(TRACE).EnumIndex() {
				fmt.Printf("TRACE: InferFields:String %s \n", field)
			}
			schema = append(schema, field)
		case reflect.Bool:
			field := bigquery.FieldSchema{}
			field.Name = name
			field.Type = bigquery.BooleanFieldType
			field.Required = false
			field.Repeated = false
			if InferSchemaDebugLevel.EnumIndex() >= DebugLevel(TRACE).EnumIndex() {
				fmt.Printf("TRACE: InferFields:Bool Parent: %s Field: %v \n", parent, field)
			}
			schema = append(schema, field)
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			field := bigquery.FieldSchema{}
			field.Name = name
			field.Type = bigquery.IntegerFieldType
			field.Required = false
			field.Repeated = false
			if InferSchemaDebugLevel.EnumIndex() >= DebugLevel(TRACE).EnumIndex() {
				fmt.Printf("TRACE: InferFields:Int Parent: %s Field: %v \n", parent, field)
			}
			schema = append(schema, field)
		case reflect.Float32, reflect.Float64:
			field := bigquery.FieldSchema{}
			field.Name = name
			field.Type = bigquery.FloatFieldType
			field.Required = false
			field.Repeated = false
			if InferSchemaDebugLevel.EnumIndex() >= DebugLevel(TRACE).EnumIndex() {
				fmt.Printf("TRACE: InferFields:Float32 %s \n", field)
			}
			schema = append(schema, field)
		case reflect.Uint, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			// This Field is skipped because it is not supported by BQ
			if InferSchemaDebugLevel.EnumIndex() >= DebugLevel(WARN).EnumIndex() {
				fmt.Printf("WARNING: InferFields %s is of type <%s> and is not currently supported. This item was passed from Parent: %s \n", name, kind, parent)
			}
		case reflect.Map:
			// This Field is skipped because it is not supported by BQ
			if InferSchemaDebugLevel.EnumIndex() >= DebugLevel(WARN).EnumIndex() {
				fmt.Printf("WARNING: InferFields %s is of type <%s> and is not currently supported. This item was passed from Parent: %s \n", name, kind, parent)
			}
		case reflect.Slice:
			sliceType := rt.Elem().Field(i).Type.Elem().Kind()
			field := bigquery.FieldSchema{}
			field.Name = name
			field.Required = false
			field.Repeated = true
			switch sliceType {
			case reflect.String:
				field.Type = bigquery.StringFieldType
			case reflect.Ptr:
				field.Type = bigquery.RecordFieldType

				nestedFields := InferFields(rt.Elem().Field(i).Type.Elem())
				for ii := range nestedFields {
					field.Schema = append(field.Schema, &nestedFields[ii])
				}
			default:
				if InferSchemaDebugLevel.EnumIndex() >= DebugLevel(ERROR).EnumIndex() {
					fmt.Printf("ERROR: InferFields:Slice %s is of type <%s> and is not currently defined. This item was passed from Parent: %s \n", name, sliceType, parent)
				}
			}
			if InferSchemaDebugLevel.EnumIndex() >= DebugLevel(TRACE).EnumIndex() {
				fmt.Printf("TRACE: InferFields:Slice %s \n", field)
			}
			schema = append(schema, field)
		case reflect.Ptr:
			rtTypeOf := rt.Elem().Field(i).Type
			ptrKind := rtTypeOf.Elem().Kind()
			switch ptrKind {
			case reflect.Struct:
				field := bigquery.FieldSchema{}
				field.Name = name
				field.Required = false
				field.Repeated = false
				field.Type = bigquery.RecordFieldType

				nestedFields := InferFields(rtTypeOf)
				for ii := range nestedFields {
					field.Schema = append(field.Schema, &nestedFields[ii])
				}
				if InferSchemaDebugLevel.EnumIndex() >= DebugLevel(TRACE).EnumIndex() {
					fmt.Printf("TRACE: InferFields:Ptr %s \n", field)
				}
				schema = append(schema, field)
			case reflect.String, reflect.Bool:
				schema = append(schema, InferField(name, rtTypeOf))
			default:
				if InferSchemaDebugLevel.EnumIndex() >= DebugLevel(ERROR).EnumIndex() {
					fmt.Printf("ERROR: InferFields:PTR %s is of type <%s> and is not currently defined. This item was passed from Parent: %s \n", name, ptrKind, parent)
				}
			}
		default:
			if InferSchemaDebugLevel.EnumIndex() >= DebugLevel(ERROR).EnumIndex() {
				fmt.Printf("ERROR: InferFields %s is of type <%s> and is not currently defined. This item was passed from Parent: %s \n", name, kind, parent)
			}
		}
	}
	return schema
}

func bqDatasetExist(projectID string, datasetID string) (bool, error) {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return false, fmt.Errorf("bigquery.NewClient: %v", err)
	}

	dataset := client.Dataset(datasetID)
	metadata, err := dataset.Metadata(ctx)
	if err != nil {
		if BigqueryDebugLevel.EnumIndex() >= DebugLevel(INFO).EnumIndex() {
			fmt.Printf("INFO: bqDataset:EXIST == FALSE `datasetID: %s` \n", datasetID)
		}
		return false, nil
	}
	_ = metadata
	if BigqueryDebugLevel.EnumIndex() >= DebugLevel(INFO).EnumIndex() {
		fmt.Printf("INFO: bqDataset:EXIST == TRUE `datasetID: %s` \n", datasetID)
	}
	return true, nil
}

func bqDatasetCreate(projectID string, datasetID string, datasetRegion string) error {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("bigquery.NewClient: %v", err)
	}

	metadata := bigquery.DatasetMetadata{}

	metadata.Location = datasetRegion
	dataset := client.Dataset(datasetID)
	if err := dataset.Create(ctx, &metadata); err != nil {
		return fmt.Errorf("bigquery.dataset.Create: %v", err)
	}
	if BigqueryDebugLevel.EnumIndex() >= DebugLevel(INFO).EnumIndex() {
		fmt.Printf("INFO: bqTable:CREATE `datasetID: %s` \n", datasetID)
	}
	return nil
}

func bqTableExist(projectID string, datasetID string, tableID string) (bool, error) {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return false, fmt.Errorf("bigquery.NewClient: %v", err)
	}

	table := client.Dataset(datasetID).Table(tableID)

	metadata, err := table.Metadata(ctx)
	if err != nil {
		if BigqueryDebugLevel.EnumIndex() >= DebugLevel(INFO).EnumIndex() {
			fmt.Printf("INFO: bqTable:EXIST == FALSE `datasetID: %s tableID: %s` \n", datasetID, tableID)
		}
		return false, nil
	}
	_ = metadata
	if BigqueryDebugLevel.EnumIndex() >= DebugLevel(INFO).EnumIndex() {
		fmt.Printf("INFO: bqTable:EXIST == TRUE `datasetID: %s tableID: %s` \n", datasetID, tableID)
	}
	return true, nil
}

var TabelCreate = bqTableCreate

func bqTableCreate(projectID string, datasetID string, tableID string, schema bigquery.Schema) error {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	table := client.Dataset(datasetID).Table(tableID)

	if err := table.Create(ctx, &bigquery.TableMetadata{Schema: schema}); err != nil {
		return fmt.Errorf("bigquery.table.Create: %v", err)
	}

	if BigqueryDebugLevel.EnumIndex() >= DebugLevel(INFO).EnumIndex() {
		fmt.Printf("INFO: bqTable:CREATE `datasetID: %s tableID: %s` \n", datasetID, tableID)
	}
	return nil
}

func bqTableDelete(projectID string, datasetID string, tableID string) error {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	table := client.Dataset(datasetID).Table(tableID)

	if err := table.Delete(ctx); err != nil {
		return fmt.Errorf("bigquery.table.Delete: %v", err)
	}

	if BigqueryDebugLevel.EnumIndex() >= DebugLevel(INFO).EnumIndex() {
		fmt.Printf("INFO: bqTable:DELETE `datasetID: %s tableID: %s` \n", datasetID, tableID)
	}

	return nil
}

func bqDatasetAssetInventoryRefresh(projectID string, datasetID string, tableID string, assetList []*assetpb.Asset) error {
	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	// Converts returns Asset List to strut that matches schema
	var assets []Asset
	for i := range assetList {
		var asset = Asset{
			Name:       assetList[i].Name,
			Asset_type: assetList[i].AssetType,
			Ancestors:  assetList[i].Ancestors,
			Resource: AssetResource{
				Version:                assetList[i].Resource.GetVersion(),
				Discovery_document_url: assetList[i].Resource.GetDiscoveryDocumentUri(),
				Discovery_name:         assetList[i].Resource.GetDiscoveryName(),
				Resource_url:           assetList[i].Resource.GetResourceUrl(),
				Parent:                 assetList[i].Resource.GetParent(),
				Data:                   assetList[i].Resource.GetData().String(),
				Location:               assetList[i].Resource.GetLocation(),
			},
			Update_Time: time.Unix(assetList[i].UpdateTime.Seconds, int64(assetList[i].UpdateTime.Nanos)),
		}
		assets = append(assets, asset)
	}
	table := client.Dataset(datasetID).Table(tableID)

	inserter := table.Inserter()
	var insertCounter int

	for {
		// Reads information from the Asset List Table
		tableRead := table.Read(ctx)
		var row []bigquery.Value

		tableRead.Next(&row)
		pageInfo := tableRead.PageInfo()

		// Export information from the err field since it is not flagged for export
		reflectedPage := reflect.ValueOf(pageInfo).Elem().FieldByName("err")

		// Checks if the table contains any rows
		// The err field is NOT nil when no more items in iterator
		if reflectedPage.IsNil() {
			break
		}

		// Max number of tries to insert Assets before timing out
		if insertCounter > 60 {
			return fmt.Errorf("bigquery.table.Ready: %s was not ready in time for use, maxTimeout is 60 seconds\n", tableID)
		}

		// Sleep for period of time before trying to insert Assets
		time.Sleep(time.Millisecond * 5000)

		// Insert assets in the specified table that was just created
		if err := inserter.Put(ctx, assets); err != nil {
			return fmt.Errorf("bigquery.table.Inserter: %v", err)
		}
		if BigqueryDebugLevel.EnumIndex() >= DebugLevel(TRACE).EnumIndex() {
			fmt.Printf("TRACE: bqDatasetAssetInventoryRefresh:INSERTER %+v \n", assets)
		}
		insertCounter++
	}

	return nil
}

var bqQueryDistincAssetTableIDs = bqAssetTypesQueryDistinc

func bqAssetTypesQueryDistinc(projectID string, datasetID string, assetInventoryTableID string) ([]string, error) {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("bigquery.NewClient: %v", err)
	}

	defer client.Close()

	var queryString = fmt.Sprintf(`SELECT distinct(asset_type) FROM %s.%s.%s order by asset_type`, projectID, datasetID, assetInventoryTableID)

	if BigqueryDebugLevel.EnumIndex() >= DebugLevel(DEBUG).EnumIndex() {
		fmt.Printf("DEBUG: bqAssetTypesQueryDistinc:QUERY `%s` \n", queryString)
	}

	query := client.Query(queryString)
	query.DisableQueryCache = true

	result, err := query.Read(ctx)
	if err != nil {
		return nil, err
	}

	type assetRow struct {
		Name bigquery.NullString `bigquery:"asset_type"`
	}

	var assetTypes []string
	for {
		var row assetRow

		err := result.Next(&row)

		if err == iterator.Done {
			break
		}

		if err != nil {
			return nil, fmt.Errorf("bigquery.query.Iterator: %v", err)
		}
		if BigqueryDebugLevel.EnumIndex() >= DebugLevel(TRACE).EnumIndex() {
			fmt.Printf("TRACE: bqAssetTypesQueryDistinc:ROW %+v \n", row)
		}
		//// There is a better way to do this but this works for now
		// Start Cleanup Needed
		var _name = string(row.Name.StringVal)
		_name = strings.Replace(_name, ".", "_", -1)
		_name = strings.Replace(_name, "/", "_", -1)
		// STOP Cleanup Needed
		assetTypes = append(assetTypes, _name)
	}

	return assetTypes, nil
}

func bqQueryAssetCompare(projectID string, datasetID string, assetInventoryTableID string, assetTableID string, assetType string) ([]Asset, error) {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("bigquery.NewClient: %v", err)
	}

	defer client.Close()

	var queryString = fmt.Sprintf(`
			WITH assetInventoryTable AS (
				SELECT
					name,
					REGEXP_SUBSTR(name,'projects/.*') as customName,
					update_time
				from %s.%s.%s
				where asset_type = '%s'
			),
			assetTable AS (
				SELECT
					selfLink,
					REGEXP_SUBSTR(selfLink,'projects/.*') as customName,
					updatedTimestamp
				from %s.%s.%s
			)
			SELECT Name,selfLink,update_time
			FROM assetInventoryTable AS FullData_Edited
			FULL OUTER JOIN  assetTable AS InstallDate
			USING (customName)
			WHERE 
				selfLink is null --Exists in list but not in detailed
				or name is null  --Exists in detailed but not in list
				or update_time > updatedTimestamp --Detailed needs to be udpated`,
		projectID, datasetID, assetInventoryTableID, assetType, projectID, datasetID, assetTableID)

	if BigqueryDebugLevel.EnumIndex() >= DebugLevel(DEBUG).EnumIndex() {
		fmt.Printf("DEBUG: bqQueryAssetCompare:QUERY `%s` \n", queryString)
	}
	query := client.Query(queryString)
	query.DisableQueryCache = true

	result, err := query.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("ERROR: bqQueryAssetCompare: bigquery.Query.Read: %v", err)
	}

	var assetList []Asset
	for {
		var row Asset

		err := result.Next(&row)

		if err == iterator.Done {
			break
		}

		if err != nil {
			if BigqueryDebugLevel.EnumIndex() >= DebugLevel(ERROR).EnumIndex() {
				fmt.Printf("ERROR: bqQueryAssetCompare:ROW %+v \n", err)
			}
			return nil, fmt.Errorf("bqQueryAssetCompare:bigquery.Query.Iterator: %v", err)
		}

		if row.SelfLink == "" {
			// 	// Asset only exists in List Table, Asset needs to be added to Get Table
			row.Action = "CREATE"
		} else if row.Name == "" {
			// 	// Asset only exists in Get Table, Asset needs to be removed from Get Table
			row.Action = "DELETE"
		} else if row.Update_Time.After(row.UpdatedTimestamp) {
			// 	// Asset exists in List and Get Table, but Asset details in Get Table is outdated
			row.Action = "UPDATE"
		} else {
			row.Action = "UNKNOWN"
		}

		if BigqueryDebugLevel.EnumIndex() >= DebugLevel(TRACE).EnumIndex() {
			fmt.Printf("TRACE: bqQueryAssetCompare:ROW %+v \n", row)
		}

		assetList = append(assetList, row)
	}
	return assetList, nil
}
func bqExecutQuery(projectID string, queryString string) ([]bigquery.Value, error) {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("bigquery.NewClient: %v", err)
	}

	defer client.Close()

	if BigqueryDebugLevel.EnumIndex() >= DebugLevel(DEBUG).EnumIndex() {
		fmt.Printf("DEBUG: bqAssetTypesQueryDistinc:QUERY `%s` \n", queryString)
	}

	query := client.Query(queryString)
	query.DisableQueryCache = true

	result, err := query.Read(ctx)
	if err != nil {
		return nil, err
	}

	var rows []bigquery.Value
	for {
		var row []bigquery.Value
		err := result.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("bigquery.query.Iterator: %v", err)
		}
		if BigqueryDebugLevel.EnumIndex() >= DebugLevel(TRACE).EnumIndex() {
			fmt.Printf("TRACE: bqExecutQuery:ROW %+v \n", row)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func bqAssetDelete(projectID string, datasetID string, tableID string, selfLink string) error {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("bigquery.NewClient: %v", err)
	}

	defer client.Close()
	var queryString = fmt.Sprintf(`
		DELETE FROM %s.%s.%s
		WHERE SelfLink = "%s"`,
		projectID, datasetID, tableID, selfLink)

	query := client.Query(queryString)
	if err != nil {
		return err
	}
	job, err := query.Run(ctx)
	if err != nil {
		return fmt.Errorf("bigquery.Query.Run: %v", err)
	}
	it, err := job.Read(ctx)
	if err != nil {
		return fmt.Errorf("bigquery.Read.Run: %v", err)
	}
	_ = it // TODO: iterate using Next or iterator.Pager.
	return nil
}
