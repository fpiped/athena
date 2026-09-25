// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package athena_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	athena "github.com/dbt-labs/athena/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// getSetOptions is a helper to cast adbc.Database to adbc.GetSetOptions.
func getSetOptions(t *testing.T, db adbc.Database) adbc.GetSetOptions {
	t.Helper()
	gso, ok := db.(adbc.GetSetOptions)
	require.True(t, ok, "database does not implement adbc.GetSetOptions")
	return gso
}

func TestNewDriver(t *testing.T) {
	drv := athena.NewDriver(memory.DefaultAllocator)
	assert.NotNil(t, drv)
}

func TestNewDatabase_NoOptions(t *testing.T) {
	drv := athena.NewDriver(memory.DefaultAllocator)

	// Should succeed even with no options (validation deferred to Open)
	db, err := drv.NewDatabase(map[string]string{})
	require.NoError(t, err)
	require.NotNil(t, db)
	defer db.Close()
}

func TestNewDatabase_WithOptions(t *testing.T) {
	drv := athena.NewDriver(memory.DefaultAllocator)

	db, err := drv.NewDatabase(map[string]string{
		athena.OptionRegion:         "us-east-1",
		athena.OptionCatalog:        "AwsDataCatalog",
		athena.OptionSchema:         "default",
		athena.OptionOutputLocation: "s3://my-bucket/athena-results/",
		athena.OptionWorkGroup:      "primary",
		athena.OptionAuthType:       athena.AuthTypeDefault,
	})
	require.NoError(t, err)
	require.NotNil(t, db)
	defer db.Close()
}

func TestNewDatabase_InvalidAuthType(t *testing.T) {
	drv := athena.NewDriver(memory.DefaultAllocator)

	_, err := drv.NewDatabase(map[string]string{
		athena.OptionAuthType: "invalid_auth_type",
	})
	require.Error(t, err)
}

func TestGetSetOption(t *testing.T) {
	drv := athena.NewDriver(memory.DefaultAllocator)

	db, err := drv.NewDatabase(map[string]string{
		athena.OptionRegion:  "us-west-2",
		athena.OptionCatalog: "MyCatalog",
	})
	require.NoError(t, err)
	require.NotNil(t, db)
	defer db.Close()

	gso := getSetOptions(t, db)

	region, err := gso.GetOption(athena.OptionRegion)
	require.NoError(t, err)
	assert.Equal(t, "us-west-2", region)

	catalog, err := gso.GetOption(athena.OptionCatalog)
	require.NoError(t, err)
	assert.Equal(t, "MyCatalog", catalog)

	// Update an option
	err = gso.SetOption(athena.OptionRegion, "eu-west-1")
	require.NoError(t, err)

	region, err = gso.GetOption(athena.OptionRegion)
	require.NoError(t, err)
	assert.Equal(t, "eu-west-1", region)
}

func TestAuthTypeAccessKey_MissingKey(t *testing.T) {
	drv := athena.NewDriver(memory.DefaultAllocator)

	db, err := drv.NewDatabase(map[string]string{
		athena.OptionRegion:         "us-east-1",
		athena.OptionOutputLocation: "s3://bucket/prefix/",
		athena.OptionAuthType:       athena.AuthTypeAccessKey,
		// Missing access key ID and secret key
	})
	require.NoError(t, err)
	defer db.Close()

	// Open should fail because credentials are incomplete
	_, err = db.Open(context.Background())
	require.Error(t, err)
}

func TestAuthTypeProfile_MissingProfileName(t *testing.T) {
	drv := athena.NewDriver(memory.DefaultAllocator)

	db, err := drv.NewDatabase(map[string]string{
		athena.OptionRegion:         "us-east-1",
		athena.OptionOutputLocation: "s3://bucket/prefix/",
		athena.OptionAuthType:       athena.AuthTypeProfile,
		// Missing profile name
	})
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Open(context.Background())
	require.Error(t, err)
}

func TestAllAuthTypeConstants(t *testing.T) {
	assert.Equal(t, "iam", athena.AuthTypeDefault)
	assert.Equal(t, "access_key", athena.AuthTypeAccessKey)
	assert.Equal(t, "profile", athena.AuthTypeProfile)
}

func TestAllOptionConstants(t *testing.T) {
	assert.Equal(t, "athena.region", athena.OptionRegion)
	assert.Equal(t, "athena.catalog", athena.OptionCatalog)
	assert.Equal(t, "athena.schema", athena.OptionSchema)
	assert.Equal(t, "athena.output_location", athena.OptionOutputLocation)
	assert.Equal(t, "athena.work_group", athena.OptionWorkGroup)
	assert.Equal(t, "athena.auth_type", athena.OptionAuthType)
	assert.Equal(t, "athena.aws.access_key_id", athena.OptionAccessKeyID)
	assert.Equal(t, "athena.aws.secret_access_key", athena.OptionSecretKey)
	assert.Equal(t, "athena.aws.session_token", athena.OptionSessionToken)
	assert.Equal(t, "athena.aws.profile", athena.OptionProfileName)
	assert.Equal(t, "athena.aws.role_arn", athena.OptionRoleARN)
	assert.Equal(t, "athena.aws.role_external_id", athena.OptionRoleExternalID)
	assert.Equal(t, "athena.aws.role_session_name", athena.OptionRoleSessionName)
	assert.Equal(t, "athena.aws.role_duration", athena.OptionRoleDuration)
	assert.Equal(t, "athena.aws.max_attempts", athena.OptionMaxAttempts)
	assert.Equal(t, "athena.endpoint_url", athena.OptionEndpointURL)
	assert.Equal(t, "athena.poll_interval", athena.OptionPollInterval)
	assert.Equal(t, "athena.iceberg_commit_retries", athena.OptionIcebergCommitRetries)
}

func TestConnectionOptions_GetSet(t *testing.T) {
	drv := athena.NewDriver(memory.DefaultAllocator)
	db, err := drv.NewDatabase(map[string]string{})
	require.NoError(t, err)
	defer db.Close()
	gso := getSetOptions(t, db)

	pollInterval, err := gso.GetOption(athena.OptionPollInterval)
	require.NoError(t, err)
	assert.Equal(t, "500ms", pollInterval)

	for key, value := range map[string]string{
		athena.OptionRoleARN:              "arn:aws:iam::123456789012:role/dbt",
		athena.OptionRoleExternalID:       "ext-1",
		athena.OptionRoleSessionName:      "dbt-athena",
		athena.OptionRoleDuration:         "15m0s",
		athena.OptionMaxAttempts:          "6",
		athena.OptionEndpointURL:          "https://athena.example.internal",
		athena.OptionPollInterval:         "1.5s",
		athena.OptionIcebergCommitRetries: "3",
	} {
		require.NoError(t, gso.SetOption(key, value), key)
		got, err := gso.GetOption(key)
		require.NoError(t, err)
		assert.Equal(t, value, got, key)
	}
}

func TestConnectionOptions_InvalidValues(t *testing.T) {
	drv := athena.NewDriver(memory.DefaultAllocator)
	for key, value := range map[string]string{
		athena.OptionRoleDuration:         "3600",
		athena.OptionPollInterval:         "-1s",
		athena.OptionMaxAttempts:          "0",
		athena.OptionIcebergCommitRetries: "many",
	} {
		_, err := drv.NewDatabase(map[string]string{key: value})
		var adbcErr adbc.Error
		require.ErrorAs(t, err, &adbcErr, key)
		assert.Equal(t, adbc.StatusInvalidArgument, adbcErr.Code, key)
	}
}

// athenaServer answers every Athena API call with an error, recording the target
// operation and the access key that signed the request.
func athenaServer(t *testing.T) (*httptest.Server, *[]string) {
	t.Helper()
	var calls []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		keyID := strings.SplitN(strings.TrimPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 Credential="), "/", 2)[0]
		calls = append(calls, r.Header.Get("X-Amz-Target")+" "+keyID)
		w.Header().Set("Content-Type", "application/x-amz-json-1.1")
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, `{"__type": "InvalidRequestException", "Message": "test server"}`)
	}))
	t.Cleanup(srv.Close)
	return srv, &calls
}

func runAgainst(t *testing.T, opts map[string]string) error {
	t.Helper()
	drv := athena.NewDriver(memory.DefaultAllocator)
	db, err := drv.NewDatabase(opts)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	conn, err := db.Open(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	stmt, err := conn.NewStatement()
	require.NoError(t, err)
	t.Cleanup(func() { stmt.Close() })
	require.NoError(t, stmt.SetSqlQuery("SELECT 1"))
	_, _, err = stmt.ExecuteQuery(context.Background())
	return err
}

func TestEndpointURL_RoutesAthenaCalls(t *testing.T) {
	srv, calls := athenaServer(t)
	err := runAgainst(t, map[string]string{
		athena.OptionRegion:      "us-east-1",
		athena.OptionAuthType:    athena.AuthTypeAccessKey,
		athena.OptionAccessKeyID: "AKIDBASE",
		athena.OptionSecretKey:   "secret",
		athena.OptionEndpointURL: srv.URL,
		athena.OptionMaxAttempts: "1",
	})
	require.ErrorContains(t, err, "test server")
	assert.Equal(t, []string{"AmazonAthena.StartQueryExecution AKIDBASE"}, *calls)
}

func TestRoleARN_AssumesTheRole(t *testing.T) {
	athenaSrv, calls := athenaServer(t)
	var assumeRole url.Values
	stsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.NoError(t, r.ParseForm())
		assumeRole = r.PostForm
		w.Header().Set("Content-Type", "text/xml")
		fmt.Fprint(w, `<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><AssumeRoleResult>
<Credentials><AccessKeyId>AKIDASSUMED</AccessKeyId><SecretAccessKey>s</SecretAccessKey><SessionToken>t</SessionToken>
<Expiration>2099-01-01T00:00:00Z</Expiration></Credentials></AssumeRoleResult></AssumeRoleResponse>`)
	}))
	t.Cleanup(stsSrv.Close)
	t.Setenv("AWS_ENDPOINT_URL_STS", stsSrv.URL)

	err := runAgainst(t, map[string]string{
		athena.OptionRegion:          "us-east-1",
		athena.OptionAuthType:        athena.AuthTypeAccessKey,
		athena.OptionAccessKeyID:     "AKIDBASE",
		athena.OptionSecretKey:       "secret",
		athena.OptionEndpointURL:     athenaSrv.URL,
		athena.OptionMaxAttempts:     "1",
		athena.OptionRoleARN:         "arn:aws:iam::123456789012:role/dbt",
		athena.OptionRoleExternalID:  "ext-1",
		athena.OptionRoleSessionName: "dbt-athena",
		athena.OptionRoleDuration:    "15m",
	})
	require.ErrorContains(t, err, "test server")
	assert.Equal(t, "AssumeRole", assumeRole.Get("Action"))
	assert.Equal(t, "arn:aws:iam::123456789012:role/dbt", assumeRole.Get("RoleArn"))
	assert.Equal(t, "ext-1", assumeRole.Get("ExternalId"))
	assert.Equal(t, "dbt-athena", assumeRole.Get("RoleSessionName"))
	assert.Equal(t, "900", assumeRole.Get("DurationSeconds"))
	assert.Equal(t, []string{"AmazonAthena.StartQueryExecution AKIDASSUMED"}, *calls)
}

// integrationConn opens a real Athena connection from environment variables and
// registers cleanup. Skips the test if ADBC_ATHENA_TESTS is unset.
func integrationConn(t *testing.T) adbc.Connection {
	t.Helper()
	if os.Getenv("ADBC_ATHENA_TESTS") == "" {
		t.Skip("set ADBC_ATHENA_TESTS=1 to run integration tests")
	}

	region := os.Getenv("AWS_DEFAULT_REGION")
	if region == "" {
		region = "us-east-1"
	}
	outputLocation := os.Getenv("ATHENA_OUTPUT_LOCATION")
	// require.NotEmpty(t, outputLocation, "ATHENA_OUTPUT_LOCATION must be set for integration tests")

	catalog := os.Getenv("ATHENA_CATALOG")
	if catalog == "" {
		catalog = "AwsDataCatalog"
	}
	schema := os.Getenv("ATHENA_SCHEMA")
	if schema == "" {
		schema = "default"
	}

	opts := map[string]string{
		athena.OptionRegion:         region,
		athena.OptionOutputLocation: outputLocation,
		athena.OptionCatalog:        catalog,
		athena.OptionSchema:         schema,
		athena.OptionAuthType:       athena.AuthTypeDefault,
	}
	if profile := os.Getenv("AWS_PROFILE"); profile != "" {
		opts[athena.OptionAuthType]    = athena.AuthTypeProfile
		opts[athena.OptionProfileName] = profile
	}

	drv := athena.NewDriver(memory.DefaultAllocator)
	db, err := drv.NewDatabase(opts)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	conn, err := db.Open(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	return conn
}

// runQuery is a helper that executes sql and returns the first record.
func runQuery(t *testing.T, conn adbc.Connection, sql string) arrow.Record {
	t.Helper()
	stmt, err := conn.NewStatement()
	require.NoError(t, err)
	t.Cleanup(func() { stmt.Close() })

	require.NoError(t, stmt.SetSqlQuery(sql))

	rdr, _, err := stmt.ExecuteQuery(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { rdr.Release() })

	require.True(t, rdr.Next(), "expected at least one record")
	return rdr.Record()
}

func TestIntegration(t *testing.T) {
	conn := integrationConn(t)
	rec := runQuery(t, conn, "SELECT 1 AS n")
	assert.EqualValues(t, 1, rec.NumCols())
}

func TestIntegration_DataTypes(t *testing.T) {
	conn := integrationConn(t)

	// Covers scalar types and two nested types (array, map).
	// Nested types are returned by Athena as opaque strings and stored as
	// Arrow utf8 columns by this driver.
	const query = `
SELECT
  CAST('hello'           AS VARCHAR)   AS str_col,
  CAST(42                AS BIGINT)    AS bigint_col,
  CAST(7                 AS INTEGER)   AS int_col,
  CAST(3.14              AS DOUBLE)    AS double_col,
  true                                 AS bool_col,
  DATE        '2024-01-15'             AS date_col,
  TIMESTAMP   '2024-01-15 12:30:00.123' AS ts_col,
  ARRAY[1, 2, 3]                       AS array_col,
  MAP(ARRAY['k'], ARRAY['v'])          AS map_col
`
	rec := runQuery(t, conn, query)

	require.EqualValues(t, 9, rec.NumCols(), "expected 9 columns")
	require.EqualValues(t, 1, rec.NumRows(), "expected 1 row")

	schema := rec.Schema()

	// Scalar types map to their native Arrow types.
	assert.Equal(t, arrow.BinaryTypes.String,         schema.Field(0).Type, "str_col")
	assert.Equal(t, arrow.PrimitiveTypes.Int64,        schema.Field(1).Type, "bigint_col")
	assert.Equal(t, arrow.PrimitiveTypes.Int32,        schema.Field(2).Type, "int_col")
	assert.Equal(t, arrow.PrimitiveTypes.Float64,      schema.Field(3).Type, "double_col")
	assert.Equal(t, arrow.FixedWidthTypes.Boolean,     schema.Field(4).Type, "bool_col")
	assert.Equal(t, arrow.FixedWidthTypes.Date32,      schema.Field(5).Type, "date_col")
	assert.Equal(t, arrow.FixedWidthTypes.Timestamp_us, schema.Field(6).Type, "ts_col")
	// Nested types are stringified.
	assert.Equal(t, arrow.BinaryTypes.String, schema.Field(7).Type, "array_col")
	assert.Equal(t, arrow.BinaryTypes.String, schema.Field(8).Type, "map_col")

	// Spot-check scalar values.
	assert.Equal(t, "hello", rec.Column(0).(*array.String).Value(0))
	assert.EqualValues(t, 42, rec.Column(1).(*array.Int64).Value(0))
	assert.EqualValues(t, 7, rec.Column(2).(*array.Int32).Value(0))
	assert.InDelta(t, 3.14, rec.Column(3).(*array.Float64).Value(0), 1e-9)
	assert.True(t, rec.Column(4).(*array.Boolean).Value(0))
	assert.EqualValues(t, arrow.Date32(19737), rec.Column(5).(*array.Date32).Value(0), "date_col: days since epoch")
	assert.EqualValues(t, arrow.Timestamp(1705321800123), rec.Column(6).(*array.Timestamp).Value(0), "ts_col: millis since epoch")

	// Nested columns must be non-empty strings.
	assert.NotEmpty(t, rec.Column(7).(*array.String).Value(0), "array_col should be non-empty")
	assert.NotEmpty(t, rec.Column(8).(*array.String).Value(0), "map_col should be non-empty")
}
