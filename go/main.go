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
	if datasetRegion != "" && !(contains(datasetRegions, datasetRegion)) {
		err := fmt.Errorf("env.GOOGLE_CLOUD_DATASET_REGION: Dataset Region `%s` is not one of the supported regions %v", datasetRegion, datasetRegions)
		fmt.Println(err.Error())
		os.Exit(1)
	}
	// BigqueryDebugLevel = TRACE
	// gcpAssetInventoryRefresh(projectID, assetScope, datasetID, datasetRegion, assetTypes)
	assetTableIDs, err := bqQueryDistincAssetTableIDs(projectID, datasetID, "gcp_asset_inventory")
	if err != nil {
		fmt.Println(err)
	}

	// ab := Address{}
	// fmt.Println(ab.GetSchema())
	// fmt.Println(ab.AssetType())
	// fmt.Println((&Address{}).AssetType())
	for i := 0; i < len(assetTableIDs); i++ {
		switch assetTableID := assetTableIDs[i]; assetTableID {
		// 	case "compute_googleapis_com_Network":
		// 		fmt.Printf("Funciton Exist for:> %s\n", assetType)
		// 		// gcpAssetInventoryComputeNetwork(projectID, datasetID)
		// 	case "compute_googleapis_com_Subnetwork":
		// 		fmt.Printf("Funciton Exist for:> %s\n", assetType)
		// 		// gcpAssetInventoryComputeSubnetwork(projectID, datasetID)
		// 	case "compute_googleapis_com_Instance":
		// 		fmt.Printf("Funciton Exist for:> %s\n", assetType)
		// 		// gcpAssetInventoryComputeInstance(projectID, datasetID)
		case (&Address{}).AssetTableID():
			// InferSchemaDebugLevel = TRACE
			fmt.Printf("Funciton Exist for:> %s\n", assetTableID)
			// gcpAssetInventory(projectID, datasetID, Address{})
			(&Address{}).RefreshAssetInventory(projectID, datasetID)
		default:
			// fmt.Printf("No funciton for:> %s\n", assetType)
		}
	}
}

func (a *Address) RefreshAssetInventory(projectID string, datasetID string) {
	assetInventoryTableID := "gcp_asset_inventory"
	assetTableID := a.AssetTableID()
	assetType := a.AssetType()
	schema, err := a.GetSchema()
	if err != nil {
		fmt.Println(err)
	}

	computeService, err := gcpComputeService()
	if err != nil {
		fmt.Println(err)
	}
	_ = computeService
	tableExist, err := bqTableExist(projectID, datasetID, assetTableID)
	// If the table does not exists then Create
	if !(tableExist) {
		if err := bqTableCreate(projectID, datasetID, assetTableID, schema); err != nil {
			fmt.Println(err)
			os.Exit(12)
		}
	}
	// bqQueryAssetCompare(projectID string, datasetID string, assetInventoryTableID string, assetTableID string, assetType string)
	gcpAssets, err := bqQueryAssetCompare(projectID, datasetID, assetInventoryTableID, assetTableID, assetType)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	for i := 0; i < len(gcpAssets); i++ {
		gcpAsset := gcpAssets[i]
		if gcpAsset.Action != "CREATE" {
			if err := bqAssetDelete(projectID, datasetID, assetTableID, gcpAsset.SelfLink); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		}
		if gcpAsset.Action != "DELETE" {
			assetDetail, err := a.GetAssetDetails(computeService, gcpAsset.Name)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			if err := bqAssetInsert(projectID, datasetID, assetTableID, assetDetail); err != nil {
				fmt.Println(err)
			}
		}
	}
}

// func RefreshAssetInventor(a string, b string, i interface){
// 	computeService, err := gcpComputeService()
// 	if err != nil {
// 		fmt.Println(err)
// 	}
// }
// func gcpAssetInventory(a string, b string, i interface{}) {
// 	// ab := reflect.TypeOf(i)
// 	fmt.Println(*i{})
// }

// computeService, err := gcpComputeService()
// if err != nil {
// 	fmt.Println(err)
// }

// tableExist, err := bqTableExist(projectID, datasetID, i.AssetType)

// 	// If the table does not exists then Create
// 	if !(tableExist) {
// 		if err := bqTableCreate2(projectID, datasetID, "gcp_asset_compute_googleapis_com_Network", &Network{}); err != nil {
// 			fmt.Println(err)
// 			os.Exit(12)
// 		}
// 	}

// 	gcpNetworks, err := bqQueryAssetCompare(projectID, datasetID, "gcp_asset_inventory", "gcp_asset_compute_googleapis_com_Network", "compute.googleapis.com/Network")
// 	if err != nil {
// 		fmt.Println(err)
// 		os.Exit(1)
// 	}
// 	for s := 0; s < len(gcpNetworks); s++ {
// 		network := gcpNetworks[s]
// 		if network.Action != "CREATE" {
// 			if err := bqAssetDelete(projectID, datasetID, "gcp_asset_compute_googleapis_com_Network", network.SelfLink.StringVal); err != nil {
// 				fmt.Println(err)
// 				os.Exit(1)
// 			}
// 		}
// 		if network.Action != "DELETE" {
// 			gcpNetwork, err := gcpComputeNetworkGet(computeService, network.Project, network.ShortName)
// 			if err != nil {
// 				fmt.Println(err)
// 				os.Exit(1)
// 			}
// 			n := Network{}
// 			formartedStruct, nil := bqConvertStruct(gcpNetwork, &Network{}, network.Update_time.Timestamp)
// 			if err != nil {
// 				fmt.Println(err)
// 				os.Exit(1)
// 			}

// 			_, _ = gcpNetwork, n
// 			if err := bqAssetInsert(projectID, datasetID, "gcp_asset_compute_googleapis_com_Network", formartedStruct); err != nil {
// 				fmt.Println(err)
// 			}
// 		}
// 	}
// }

// aa, _ := gcpComputeService()
// var address Address
// _ = address
// // t1 := TableCreate(address.GetSchema())
// _ = aa
// // ab, err := a1.GetAsset(aa, "//compute.googleapis.com/projects/rteller-demo-host-aaaa/regions/us-central1/addresses/trusted-resrved")
// // if err != nil {
// // 	fmt.Println(err)
// // }
// // fmt.Printf("Address:> %+v", ab)
// // _ = ab
// // InferSchemaDebugLevel = WARN
// // // if InferSchemaDebugLevel.EnumIndex() >= InferSchemaDebug(WARN).EnumIndex() {
// // ac, _ := a1.GetSchema()
// // fmt.Println(ac)
