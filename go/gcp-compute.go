package main

import (
	"golang.org/x/net/context"
	"google.golang.org/api/compute/v1"

	"golang.org/x/oauth2/google"
)

var supportedAssets []string

func gcpComputeService() (*compute.Service, error) {
	ctx := context.Background()
	client, err := google.DefaultClient(ctx, compute.ComputeScope)
	if err != nil {
		return nil, err
	}
	return compute.New(client)
}
