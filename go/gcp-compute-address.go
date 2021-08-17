package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"golang.org/x/net/context"
	"google.golang.org/api/compute/v1"

	"cloud.google.com/go/bigquery"
	"golang.org/x/oauth2/google"
)

var supportedAssets []string = []string{
	(&Address{}).AssetType(),
}

func gcpComputeService() (*compute.Service, error) {
	ctx := context.Background()
	client, err := google.DefaultClient(ctx, compute.ComputeScope)
	if err != nil {
		return nil, err
	}
	return compute.New(client)
}

type Address compute.Address

// https://cloud.google.com/compute/docs/reference/rest/v1/addresses/get
func (a *Address) GetAssetDetails(computeService *compute.Service, assetName string) (Address, error) {
	nameSplit := strings.Split(assetName, "/")
	project := nameSplit[4]
	region := nameSplit[6]
	resourceId := nameSplit[len(nameSplit)-1]

	assetGetCall := computeService.Addresses.Get(project, region, resourceId)
	assetGet, err := assetGetCall.Do()

	if err != nil {
		return Address{}, err
	} else {
		return Address(*assetGet), nil
	}
}

func (a *Address) AssetType() string {
	return string("compute.googleapis.com/Address")
}
func (a *Address) AssetTableID() string {
	return string("compute_googleapis_com_Address")
}

func (a *Address) GetSchema() (bigquery.Schema, error) {
	schema, err := InferSchema(Address{})
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

func (a *Address) InsertIntoBQ(projectID string, datasetID string) error {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	js, _ := json.Marshal(st)
	_ = js
	jss := string(js)
	// fmt.Println(jss)
	ab := strings.NewReader(jss)
	// fmt.Println(ab)
	rs := bigquery.NewReaderSource(ab)
	// rs.SourceFormat = bigquery.DataFormat(bigquery.JSON)

	rs.SourceFormat = bigquery.JSON
	s1, err := (&Address{}).GetSchema()
	if err != nil {
		fmt.Println(err)
	}
	rs.Schema = s1
	rs.IgnoreUnknownValues = true

	table := client.Dataset(datasetID).Table(tableID)
	loader := table.LoaderFrom(rs)

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

func bqAssetInsert(projectID string, datasetID string, tableID string, st interface{}) error {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	js, _ := json.Marshal(st)
	_ = js
	jss := string(js)
	// fmt.Println(jss)
	ab := strings.NewReader(jss)
	// fmt.Println(ab)
	rs := bigquery.NewReaderSource(ab)
	// rs.SourceFormat = bigquery.DataFormat(bigquery.JSON)

	rs.SourceFormat = bigquery.JSON
	s1, err := (&Address{}).GetSchema()
	if err != nil {
		fmt.Println(err)
	}
	rs.Schema = s1
	rs.IgnoreUnknownValues = true

	table := client.Dataset(datasetID).Table(tableID)
	loader := table.LoaderFrom(rs)

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
	os.Exit(999)
	// if err := inserter.Put(ctx, st); err != nil {
	// 	return fmt.Errorf("bigquery.Inserter.Put: %v", err)
	// }

	return nil
}
