package main

import (
	"fmt"
	"os"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/api/iterator"

	asset "cloud.google.com/go/asset/apiv1"
	bigquery "cloud.google.com/go/bigquery"
	assetpb "google.golang.org/genproto/googleapis/cloud/asset/v1"
)

// type CustomAsset struct {
// 	CustomName       bigquery.NullString    //From List & Detailed Table
// 	Name             bigquery.NullString    //From List Table
// 	Update_time      bigquery.NullTimestamp //From List Table
// 	SelfLink         bigquery.NullString    //From Detailed Table
// 	UpdatedTimestamp bigquery.NullTimestamp //From Detailed Table
// 	Project          string                 //Derived from CustomName
// 	Region           string                 //Derived from CustomName
// 	ShortName        string                 //Derived from CustomName
// 	Action           AssetAction
// }

type AssetAction string

const (
	CREATE AssetAction = "CREATE"
	UPDATE             = "UPDATE"
	DELETE             = "DELETE"
)

func (a *Asset) GetSchema() (bigquery.Schema, error) {
	schema, err := bigquery.InferSchema(Asset{})
	if err != nil {
		return nil, err
	}

	return schema.Relax(), nil
}

type Asset struct {
	Name             string        //From List Table
	Asset_type       string        //From List Table
	Ancestors        []string      //From List Table
	Update_Time      time.Time     //From List Table
	Resource         AssetResource //From List Table
	SelfLink         string        `bigquery:"-"` //From Detailed Table
	UpdatedTimestamp time.Time     `bigquery:"-"` //From Detailed Table
	Action           AssetAction   `bigquery:"-"` //Derived from Deatiled and List DIFF
}

type AssetResource struct {
	Version                string
	Discovery_document_url string
	Discovery_name         string
	Resource_url           string
	Parent                 string
	Data                   string
	Location               string
}

func gcpAssetList(parent string, assetTypes []string) ([]*assetpb.Asset, error) {
	ctx := context.Background()
	client, err := asset.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	request := &assetpb.ListAssetsRequest{
		Parent:     parent,
		AssetTypes: assetTypes[:],
	}
	result := client.ListAssets(ctx, request)
	var assets []*assetpb.Asset
	for {
		resp, err := result.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		assets = append(assets, resp)
	}

	return assets, nil
}

func gcpAssetInventoryRefresh(projectID string, assetScope string, datasetID string, datasetRegion string, assetTypes []string) {
	var assetInventoryTableID = string("gcp_asset_inventory")

	//// Example Scope Options
	// projects/PROJECT_ID (e.g., "projects/foo-bar")
	// projects/PROJECT_NUMBER (e.g., "projects/12345678")
	// folders/FOLDER_NUMBER (e.g., "folders/1234567")
	// organizations/ORGANIZATION_NUMBER (e.g., "organizations/123456")

	//// Supported Searchable Assets
	// https://cloud.google.com/asset-inventory/docs/supported-asset-types#searchable_asset_types
	assetList, err := gcpAssetList(assetScope, assetTypes[:])
	if err != nil {
		fmt.Println(err)
	}

	datasetExist, err := bqDatasetExist(projectID, datasetID)
	if err != nil {
		fmt.Println(err)
	}

	if !(datasetExist) {
		if err := bqDatasetCreate(projectID, datasetID, datasetRegion); err != nil {
			fmt.Println(err)
		}
	}
	tableExist, _ := bqTableExist(projectID, datasetID, assetInventoryTableID)

	// If the table exists then Delete it so it can be Re-Created
	if tableExist {
		if err := bqTableDelete(projectID, datasetID, assetInventoryTableID); err != nil {
			fmt.Println(err)
			os.Exit(12)
		}
	}
	for {
		tableExist, _ := bqTableExist(projectID, datasetID, assetInventoryTableID)
		if !(tableExist) {
			// schema, _ := bigquery.InferSchema(Asset{})
			asset := Asset{}
			schema, _ := asset.GetSchema()

			if err := bqTableCreate(projectID, datasetID, assetInventoryTableID, schema); err != nil {
				fmt.Println(err)
				os.Exit(12)
			} else {
				fmt.Println("Table Created")
			}
			break
			// continue
		}
		time.Sleep(time.Millisecond * 5000)
	}

	// fmt.Println(assetList[0])
	// ProjectID
	// DataSet
	// Table
	// Asset Collection
	if err := bqDatasetAssetInventoryRefresh(projectID, datasetID, assetInventoryTableID, assetList); err != nil {
		fmt.Println(err)
	}
}
