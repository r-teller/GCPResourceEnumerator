package main

import (
	"fmt"
	"os"
	"strings"
)

var gcpRegions []string = []string{
	"asia-east1",
	"asia-east2",
	"asia-northeast1",
	"asia-northeast2",
	"asia-northeast3",
	"asia-south1",
	"asia-south2",
	"asia-southeast1",
	"asia-southeast2",
	"australia-southeast1",
	"australia-southeast2",
	"europe-central2",
	"europe-north1",
	"europe-west1",
	"europe-west2",
	"europe-west3",
	"europe-west4",
	"europe-west6",
	"northamerica-northeast1",
	"northamerica-northeast2",
	"southamerica-east1",
	"us-central1",
	"us-east1",
	"us-east4",
	"us-west1",
	"us-west2",
	"us-west3",
	"us-west4",
}

func contains(s []string, str string) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}

	return false
}
func main() {
	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if projectID == "" {
		fmt.Println("env.GOOGLE_CLOUD_PROJECT environment variable must be set.")
		os.Exit(1)
	}

	// https://cloud.google.com/asset-inventory/docs/supported-asset-types#supported_resource_types
	assetTypes := strings.Split(os.Getenv("GOOGLE_CLOUD_ASSET_TYPES"), ",")
	if len(assetTypes) == 1 && assetTypes[0] == "" {
		fmt.Println("env.GOOGLE_CLOUD_ASSET_TYPES environment variable must be set and contain atleast one item")
		os.Exit(1)
	}
	assetScopes := []string{"projects", "folders", "organizations"}

	assetScope := strings.ToLower(os.Getenv("GOOGLE_CLOUD_ASSET_SCOPE"))

	if assetScope == "" {
		err := fmt.Errorf("env.GOOGLE_CLOUD_ASSET_SCOPE: environment variable must be set.")
		fmt.Println(err.Error())
		os.Exit(1)
	}
	_assetScope := strings.Split(strings.Replace(assetScope, "-", "_", -1), "/")
	if !(contains(assetScopes, _assetScope[0])) {
		err := fmt.Errorf("env.GOOGLE_CLOUD_ASSET_SCOPE: The scope type `%s` is not one of the supported scopes types %v", _assetScope, assetScopes)
		fmt.Println(err.Error())
		os.Exit(1)
	}

	datasetID := os.Getenv("GOOGLE_CLOUD_DATASET_ID")
	if datasetID == "" {
		datasetID = fmt.Sprintf(`gcp_asset_inventory_%s_%s`, _assetScope[0], _assetScope[1])
	}

	datasetRegion := strings.ToLower(os.Getenv("GOOGLE_CLOUD_DATASET_REGION"))
	datasetRegions := append(gcpRegions, "us", "eu")
	if datasetRegion == "" {
		datasetRegion = "us"
	} else if datasetRegion != "" && !(contains(datasetRegions, datasetRegion)) {
		err := fmt.Errorf("env.GOOGLE_CLOUD_DATASET_REGION: Dataset Region `%s` is not one of the supported regions %v", datasetRegion, datasetRegions)
		fmt.Println(err.Error())
		os.Exit(1)
	}

	assetInventoryTableID := strings.ToLower(os.Getenv("GOOGLE_CLOUD_INVENTORY_TABLE_ID"))
	if assetInventoryTableID == "" {
		assetInventoryTableID = "cloudasset_googleapis_com_Asset"
	}
	AssetDebugLevel = DEBUG
	asset := Asset{}

	if err := asset.CollectAssets(assetScope, assetTypes); err != nil {
		os.Exit(1)
	}
	asset.RefreshInventory(projectID, datasetID, datasetRegion, assetInventoryTableID)
	assetTableIDs := asset.ListDistinctAssets(projectID, datasetID, assetInventoryTableID)

	for i := 0; i < len(assetTableIDs); i++ {
		switch assetTableID := assetTableIDs[i]; assetTableID {
		case (ForwardingRule{}).AssetTableID():
			fmt.Printf("Funciton Exist for:> %s\n", assetTableID)
			(&ForwardingRule{}).RefreshAssetInventory(projectID, datasetID, assetInventoryTableID)
		case (Network{}).AssetTableID():
			fmt.Printf("Funciton Exist for:> %s\n", assetTableID)
			(&Network{}).RefreshAssetInventory(projectID, datasetID, assetInventoryTableID)
		case (Subnetwork{}).AssetTableID():
			fmt.Printf("Funciton Exist for:> %s\n", assetTableID)
			(&Subnetwork{}).RefreshAssetInventory(projectID, datasetID, assetInventoryTableID)
		case (Instance{}).AssetTableID():
			fmt.Printf("Funciton Exist for:> %s\n", assetTableID)
			(&Instance{}).RefreshAssetInventory(projectID, datasetID, assetInventoryTableID)
		case (Address{}).AssetTableID():
			fmt.Printf("Funciton Exist for:> %s\n", assetTableID)
			(&Address{}).RefreshAssetInventory(projectID, datasetID, assetInventoryTableID)
		default:
			fmt.Printf("No funciton defined for:> %s\n", assetTableID)
		}
	}
}
