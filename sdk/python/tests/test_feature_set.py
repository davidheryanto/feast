# Copyright 2019 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import pathlib
from concurrent import futures
from datetime import datetime

import grpc
import pandas as pd
import pytest
import pytz
from google.protobuf import json_format
from tensorflow_metadata.proto.v0 import schema_pb2

import dataframes
import feast.core.CoreService_pb2_grpc as Core
from feast.client import Client
from feast.entity import Entity
from feast.feature_set import Feature, FeatureSet, FeatureSetRef
from feast.value_type import ValueType
from feast_core_server import CoreServicer

CORE_URL = "core.feast.local"
SERVING_URL = "serving.feast.local"


class TestFeatureSet:
    @pytest.fixture(scope="function")
    def server(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        Core.add_CoreServiceServicer_to_server(CoreServicer(), server)
        server.add_insecure_port("[::]:50051")
        server.start()
        yield server
        server.stop(0)

    @pytest.fixture
    def client(self, server):
        return Client(core_url="localhost:50051")

    def test_add_remove_features_success(self):
        fs = FeatureSet("my-feature-set")
        fs.add(Feature(name="my-feature-1", dtype=ValueType.INT64))
        fs.add(Feature(name="my-feature-2", dtype=ValueType.INT64))
        fs.drop(name="my-feature-1")
        assert len(fs.features) == 1 and fs.features[0].name == "my-feature-2"

    def test_remove_feature_failure(self):
        with pytest.raises(ValueError):
            fs = FeatureSet("my-feature-set")
            fs.drop(name="my-feature-1")

    def test_update_from_source_failure(self):
        with pytest.raises(Exception):
            df = pd.DataFrame()
            fs = FeatureSet("driver-feature-set")
            fs.infer_fields_from_df(df)

    @pytest.mark.parametrize(
        "dataframe,feature_count,entity_count,discard_unused_fields,features,entities",
        [
            (
                dataframes.GOOD,
                3,
                1,
                True,
                [],
                [Entity(name="entity_id", dtype=ValueType.INT64)],
            ),
            (
                dataframes.GOOD_FIVE_FEATURES,
                5,
                1,
                True,
                [],
                [Entity(name="entity_id", dtype=ValueType.INT64)],
            ),
            (
                dataframes.GOOD_FIVE_FEATURES,
                6,
                1,
                True,
                [Feature(name="feature_6", dtype=ValueType.INT64)],
                [Entity(name="entity_id", dtype=ValueType.INT64)],
            ),
            (
                dataframes.GOOD_FIVE_FEATURES_TWO_ENTITIES,
                5,
                2,
                True,
                [],
                [
                    Entity(name="entity_1_id", dtype=ValueType.INT64),
                    Entity(name="entity_2_id", dtype=ValueType.INT64),
                ],
            ),
            (
                dataframes.GOOD_FIVE_FEATURES_TWO_ENTITIES,
                6,
                3,
                False,
                [],
                [
                    Entity(name="entity_1_id", dtype=ValueType.INT64),
                    Entity(name="entity_2_id", dtype=ValueType.INT64),
                ],
            ),
            (
                dataframes.NO_FEATURES,
                0,
                1,
                True,
                [],
                [Entity(name="entity_id", dtype=ValueType.INT64)],
            ),
            (
                pd.DataFrame(
                    {
                        "datetime": [
                            datetime.utcnow().replace(tzinfo=pytz.utc) for _ in range(3)
                        ]
                    }
                ),
                0,
                0,
                True,
                [],
                [],
            ),
        ],
        ids=[
            "Test small dataframe update with hardcoded entity",
            "Test larger dataframe update with hardcoded entity",
            "Test larger dataframe update with hardcoded entity and feature",
            "Test larger dataframe update with two hardcoded entities and discarding of existing fields",
            "Test larger dataframe update with two hardcoded entities and retention of existing fields",
            "Test dataframe with no featuresdataframe",
            "Test empty dataframe",
        ],
    )
    def test_add_features_from_df_success(
        self,
        dataframe,
        feature_count,
        entity_count,
        discard_unused_fields,
        features,
        entities,
    ):
        my_feature_set = FeatureSet(
            name="my_feature_set",
            features=[Feature(name="dummy_f1", dtype=ValueType.INT64)],
            entities=[Entity(name="dummy_entity_1", dtype=ValueType.INT64)],
        )
        my_feature_set.infer_fields_from_df(
            dataframe,
            discard_unused_fields=discard_unused_fields,
            features=features,
            entities=entities,
        )
        assert len(my_feature_set.features) == feature_count
        assert len(my_feature_set.entities) == entity_count
        
def test_update_schema(self):
        test_data_folder = (
            pathlib.Path(__file__).parent / "data" / "tensorflow_metadata"
        )
        schema_bikeshare = schema_pb2.Schema()
        json_format.Parse(
            open(test_data_folder / "schema_bikeshare.json").read(), schema_bikeshare
        )
        feature_set_bikeshare = FeatureSet(
            name="bikeshare",
            entities=[Entity(name="station_id", dtype=ValueType.INT64),],
            features=[
                Feature(name="name", dtype=ValueType.STRING),
                Feature(name="status", dtype=ValueType.STRING),
                Feature(name="latitude", dtype=ValueType.FLOAT),
                Feature(name="longitude", dtype=ValueType.FLOAT),
                Feature(name="location", dtype=ValueType.STRING),
            ],
        )
        # Before update
        for entity in feature_set_bikeshare.entities:
            assert entity.presence is None
            assert entity.shape is None
        for feature in feature_set_bikeshare.features:
            assert feature.presence is None
            assert feature.shape is None
            assert feature.string_domain is None
            assert feature.float_domain is None
            assert feature.int_domain is None

        feature_set_bikeshare.update_schema(schema_bikeshare)

        # After update
        for entity in feature_set_bikeshare.entities:
            assert entity.presence is not None
            assert entity.shape is not None
        for feature in feature_set_bikeshare.features:
            assert feature.presence is not None
            assert feature.shape is not None
            if feature.name in ["location", "name", "status"]:
                assert feature.string_domain is not None
            elif feature.name in ["latitude", "longitude"]:
                assert feature.float_domain is not None
            elif feature.name in ["station_id"]:
                assert feature.int_domain is not None


class TestFeatureSetRef:
    def test_from_feature_set(self):
        feature_set = FeatureSet("test", "test")
        feature_set.version = 2
        ref = FeatureSetRef.from_feature_set(feature_set)

        assert ref.name == "test"
        assert ref.project == "test"
        assert ref.version == 2

    def test_str_ref(self):
        original_ref = FeatureSetRef(project="test", name="test", version=2)
        ref_str = repr(original_ref)
        parsed_ref = FeatureSetRef.from_str(ref_str)
        assert original_ref == parsed_ref
