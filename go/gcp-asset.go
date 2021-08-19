package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/api/iterator"

	asset "cloud.google.com/go/asset/apiv1"
	bigquery "cloud.google.com/go/bigquery"
	assetpb "google.golang.org/genproto/googleapis/cloud/asset/v1"
)

var AssetDebugLevel = DebugLevel(ERROR)

type Asset struct {
	Name              string           //From List Table
	Asset_type        string           //From List Table
	Ancestors         []string         //From List Table
	Update_Time       time.Time        //From List Table
	Resource          AssetResource    //From List Table
	SelfLink          string           `bigquery:"-"` //From Detailed Table
	UpdatedTimestamp  time.Time        `bigquery:"-"` //From Detailed Table
	Action            AssetAction      `bigquery:"-"` //Derived from Deatiled and List DIFF
	AssetList         []*assetpb.Asset `bigquery:"-"` //Derived from ListAssets method
	DistinctAssetList []string         `bigquery:"-"` //Derived from Bigquery Distinct Query
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

//// Supported AssetTypes
// https://cloud.google.com/asset-inventory/docs/supported-asset-types#searchable_asset_types
func (a *Asset) CollectAssets(parent string, assetTypes []string) error {
	ctx := context.Background()
	client, err := asset.NewClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	if AssetDebugLevel.EnumIndex() >= DebugLevel(DEBUG).EnumIndex() {
		fmt.Printf("DEBUG: Asset:CollectAssets  AssetTypes = %s \n", assetTypes)
	}

	if AssetDebugLevel.EnumIndex() >= DebugLevel(DEBUG).EnumIndex() {
		fmt.Printf("DEBUG: Asset:CollectAssets  Paret = %s \n", parent)
	}

	//// Example Parent Options
	// projects/PROJECT_ID (e.g., "projects/foo-bar")
	// projects/PROJECT_NUMBER (e.g., "projects/12345678")
	// folders/FOLDER_NUMBER (e.g., "folders/1234567")
	// organizations/ORGANIZATION_NUMBER (e.g., "organizations/123456")
	request := &assetpb.ListAssetsRequest{
		Parent:      parent,
		AssetTypes:  assetTypes[:],
		ContentType: assetpb.ContentType_CONTENT_TYPE_UNSPECIFIED,
	}

	// https://cloud.google.com/asset-inventory/docs/reference/rest/v1/assets/list
	response := client.ListAssets(ctx, request)
	var assetList []*assetpb.Asset
	for {
		asset, err := response.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			if AssetDebugLevel.EnumIndex() >= DebugLevel(TRACE).EnumIndex() {
				fmt.Printf("ERROR: Asset:assetList  %+v \n", err)
			}
			return err
		}
		assetList = append(assetList, asset)
		if AssetDebugLevel.EnumIndex() >= DebugLevel(TRACE).EnumIndex() {
			fmt.Printf("TRACE: Asset:assetList  %s \n", asset)
		}
	}
	a.AssetList = assetList
	return nil
}

func (a *Asset) ListDistinctAssets(projectID string, datasetID string, assetInventoryTableID string) []string {
	var queryString = fmt.Sprintf(`SELECT distinct(asset_type) FROM %s.%s.%s order by asset_type`, projectID, datasetID, assetInventoryTableID)
	results, err := bqExecutQuery(projectID, queryString)
	if err != nil {
		fmt.Println("bigquery.NewClient: %v", err)
	}

	var assetTableIDs []string
	for _, row := range results {
		//// There is a better way to do this but this works for now
		// Start Cleanup Needed
		var _name = row.([]bigquery.Value)[0].(string)
		_name = strings.Replace(_name, ".", "_", -1)
		_name = strings.Replace(_name, "/", "_", -1)
		assetTableIDs = append(assetTableIDs, _name)
		// STOP Cleanup Needed
	}
	return assetTableIDs
}

func (a *Asset) RefreshInventory(projectID string, datasetID string, datasetRegion string, assetInventoryTableID string) {
	if projectID == "" || datasetID == "" || datasetRegion == "" || assetInventoryTableID == "" {
		fmt.Println("projectID is empty: ", projectID == "")
		fmt.Println("datasetID is empty: ", datasetID == "")
		fmt.Println("datasetRegion is empty: ", datasetRegion == "")
		fmt.Println("assetInventoryTableID is empty: ", assetInventoryTableID == "")
		err := fmt.Errorf("An empty variable was passed to the RefreshInventory method")
		fmt.Println(err.Error())
		os.Exit(1)

	}

	datasetExist, err := bqDatasetExist(projectID, datasetID)
	if err != nil {
		fmt.Println(err)
	}
	if !(datasetExist) {
		if err := bqDatasetCreate(projectID, datasetID, datasetRegion); err != nil {
			fmt.Println(err)
		}
		if AssetDebugLevel.EnumIndex() >= DebugLevel(DEBUG).EnumIndex() {
			fmt.Printf("DEBUG: Asset:RefreshInventory:DataSet:CREATE DatasetID: %s Region: %s\n", datasetID, datasetRegion)
		}
	}
	tableExist, _ := bqTableExist(projectID, datasetID, assetInventoryTableID)

	// If the table exists then Delete it so it can be Re-Created
	if tableExist {
		if err := bqTableDelete(projectID, datasetID, assetInventoryTableID); err != nil {
			fmt.Println(err)
			os.Exit(12)
		}
		if AssetDebugLevel.EnumIndex() >= DebugLevel(DEBUG).EnumIndex() {
			fmt.Printf("DEBUG: Asset:RefreshInventory:Table:Delete DatasetID: TableID: %s\n", datasetID, assetInventoryTableID)
		}
	}

	schema, _ := a.GetSchema()
	for {
		tableExist, _ := bqTableExist(projectID, datasetID, assetInventoryTableID)
		if !(tableExist) {
			if err := bqTableCreate(projectID, datasetID, assetInventoryTableID, schema); err != nil {
				fmt.Println(err)
				os.Exit(12)
			}
			if AssetDebugLevel.EnumIndex() >= DebugLevel(DEBUG).EnumIndex() {
				fmt.Printf("DEBUG: Asset:RefreshInventory:Table:Create DatasetID: TableID: %s \n", datasetID, assetInventoryTableID)
			}
			break
		}
		time.Sleep(time.Millisecond * 5000)
	}
	if err := bqDatasetAssetInventoryRefresh(projectID, datasetID, assetInventoryTableID, a.AssetList); err != nil {
		fmt.Println(err)
	}
	if AssetDebugLevel.EnumIndex() >= DebugLevel(DEBUG).EnumIndex() {
		fmt.Printf("DEBUG: Asset:RefreshInventory DatasetID: %s TableID: %s \n", datasetID, assetInventoryTableID)
	}
}
