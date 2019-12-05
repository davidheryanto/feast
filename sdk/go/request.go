package feast

import (
	"fmt"
	"github.com/gojek/feast/sdk/go/protos/feast/serving"
	"strconv"
	"strings"
)

var (
	ErrInvalidFeatureName = "invalid feature name %s provided, feature names must be in the format featureSet:version:featureName"
)

// OnlineFeaturesRequest wrapper on feast.serving.GetOnlineFeaturesRequest.
type OnlineFeaturesRequest struct {

	// Features is the list of features to obtain from Feast. Each feature must be given by its fully qualified ID,
	// in the format featureSet:version:featureName.
	Features []string

	// Entities is the list of entity rows to retrieve features on. Each row is a map of entity name to entity value.
	Entities []Row
}

// Builds the feast-specified request payload from the wrapper.
func (r OnlineFeaturesRequest) buildRequest() (*serving.GetOnlineFeaturesRequest, error) {
	featureSets, err := buildFeatureSets(r.Features)
	if err != nil {
		return nil, err
	}

	entityRows := make([]*serving.GetOnlineFeaturesRequest_EntityRow, len(r.Entities))

	for i := range r.Entities {
		entityRows[i] = &serving.GetOnlineFeaturesRequest_EntityRow{
			Fields: r.Entities[i],
		}
	}
	return &serving.GetOnlineFeaturesRequest{
		FeatureSets: featureSets,
		EntityRows:  entityRows,
	}, nil
}

// buildFeatureSets create a slice of FeatureSetRequest object from
// a slice of "feature_set:version:feature_name" string.
// It returns an error when "feature_set:version:feature_name" string has an invalid format.
func buildFeatureSets(features []string) ([]*serving.FeatureSetRequest, error) {
	var featureSets []*serving.FeatureSetRequest

	// Map of "feature_name:version" to "FeatureSetRequest" pointer
	// to reference existing FeatureSetRequest, if any.
	featureNameVersionToRequest := make(map[string]*serving.FeatureSetRequest)

	for _, feature := range features {
		split := strings.Split(feature, ":")
		if len(split) != 3 {
			return nil, fmt.Errorf(ErrInvalidFeatureName, feature)
		}

		featureSetName, featureSetVersionString, featureName := split[0], split[1], split[2]
		featureSetVersion, err := strconv.Atoi(featureSetVersionString)
		if err != nil {
			return nil, fmt.Errorf(ErrInvalidFeatureName, feature)
		}

		featureNameVersion := featureSetName + ":" + featureSetVersionString
		if featureSet, ok := featureNameVersionToRequest[featureNameVersion]; !ok {
			featureSet = &serving.FeatureSetRequest{
				Name:         featureSetName,
				Version:      int32(featureSetVersion),
				FeatureNames: []string{featureName},
			}
			featureNameVersionToRequest[featureNameVersion] = featureSet
			featureSets = append(featureSets, featureSet)
		} else {
			featureSet.FeatureNames = append(featureSet.FeatureNames, featureName)
		}
	}

	return featureSets, nil
}
