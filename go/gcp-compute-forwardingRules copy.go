package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"golang.org/x/net/context"
	"google.golang.org/api/compute/v1"

	"cloud.google.com/go/bigquery"
)

type BackendService compute.BackendService

func (z BackendService) AssetType() string {
	return "compute.googleapis.com/BackendService"
}
func (z BackendService) AssetTableID() string {
	return "compute_googleapis_com_BackendService"
}

// https://cloud.google.com/compute/docs/reference/rest/v1/backendServices/get
func (z *BackendService) GetAsset(computeService *compute.Service, assetName string) (BackendService, error) {
	nameSplit := strings.Split(assetName, "/")
	project := nameSplit[4]
	resourceId := nameSplit[len(nameSplit)-1]

	assetGetCall := computeService.BackendServices.Get(project, resourceId)
	asset, err := assetGetCall.Do()
	if err != nil {
		return BackendService{}, err
	} else {

		return BackendService(*asset), nil
	}
}

func (z BackendService) GetSchema() (bigquery.Schema, error) {
	schema, err := InferSchema(z)
	if err != nil {
		return nil, err
	}

	field := bigquery.FieldSchema{}
	field.Name = "UpdatedTimestamp"
	field.Type = bigquery.TimestampFieldType
	field.Required = false
	field.Repeated = false
	schema = append(schema, &field)

	return schema, nil
}

func (z *BackendService) RefreshAssetInventory(projectID string, datasetID string, assetInventoryTableID string) {
	assetTableID := z.AssetTableID()
	assetType := z.AssetType()
	schema, err := z.GetSchema()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	computeService, err := gcpComputeService()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	_ = computeService
	tableExist, err := bqTableExist(projectID, datasetID, assetTableID)

	// If the table does not exists then Create
	if !(tableExist) {
		if err := bqTableCreate(projectID, datasetID, assetTableID, schema); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}

	assets, err := bqQueryAssetCompare(projectID, datasetID, assetInventoryTableID, assetTableID, assetType)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	for i := 0; i < len(assets); i++ {
		asset := assets[i]
		if asset.Action != "CREATE" {
			if err := bqAssetDelete(projectID, datasetID, assetTableID, asset.SelfLink); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		}
		if asset.Action != "DELETE" {
			assetDetail, err := z.GetAsset(computeService, asset.Name)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			if err := assetDetail.InsertAssetBQ(projectID, datasetID); err != nil {
				fmt.Println(err)
			}
		}
	}
}

func (z *BackendService) InsertAssetBQ(projectID string, datasetID string) error {
	if z.SelfLink == "" {
		return fmt.Errorf("SelfLink is a required field")
	}
	asset := z
	assetTableID := asset.AssetTableID()

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	assetJSON, _ := json.Marshal(asset)

	bqReaderSource := bigquery.NewReaderSource(strings.NewReader(string(assetJSON)))

	bqReaderSource.SourceFormat = bigquery.JSON
	schema, err := z.GetSchema()
	if err != nil {
		fmt.Println(err)
	}
	bqReaderSource.Schema = schema
	bqReaderSource.IgnoreUnknownValues = true

	table := client.Dataset(datasetID).Table(assetTableID)
	loader := table.LoaderFrom(bqReaderSource)

	loader.CreateDisposition = bigquery.CreateNever

	job, err := loader.Run(ctx)
	if err != nil {
		fmt.Println(err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		fmt.Println(err)
	}
	if status.Err() != nil {
		fmt.Println(status.Err())
	}

	return nil
}
