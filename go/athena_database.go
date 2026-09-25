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

package athena

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	athenaSDK "github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/smithy-go/middleware"
)

type databaseImpl struct {
	driverbase.DatabaseImplBase

	region         string
	catalog        string
	schema         string
	outputLocation string
	workGroup      string

	authType     string
	accessKeyID  string
	secretKey    string
	sessionToken string
	profileName  string

	roleARN         string
	roleExternalID  string
	roleSessionName string
	roleDuration    time.Duration

	maxAttempts          int
	endpointURL          string
	pollInterval         time.Duration
	icebergCommitRetries int

	// testClient is non-nil only during testing. When set, Open uses it
	// directly instead of constructing a real AWS SDK client.
	testClient athenaClientAPI
}

func (d *databaseImpl) Open(ctx context.Context) (adbc.Connection, error) {
	var client athenaClientAPI
	if d.testClient != nil {
		client = d.testClient
	} else {
		cfg, err := d.buildAWSConfig(ctx)
		if err != nil {
			return nil, err
		}
		client = athenaSDK.NewFromConfig(cfg, func(o *athenaSDK.Options) {
			o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
				return awsmiddleware.AddUserAgentKeyValue("athena-adbc-go", driverVersion)(stack)
			})
			if d.endpointURL != "" {
				o.BaseEndpoint = aws.String(d.endpointURL)
			}
		})
	}

	conn := &connectionImpl{
		ConnectionImplBase: driverbase.NewConnectionImplBase(&d.DatabaseImplBase),
		athenaClient:       client,
		db:                 d,
		catalog:            d.catalog,
		schema:             d.schema,
	}

	return driverbase.NewConnectionBuilder(conn).
		WithCurrentNamespacer(conn).
		WithTableTypeLister(conn).
		WithDbObjectsEnumerator(conn).
		Connection(), nil
}

func (d *databaseImpl) buildAWSConfig(ctx context.Context) (aws.Config, error) {
	var opts []func(*awsconfig.LoadOptions) error

	if d.region != "" {
		opts = append(opts, awsconfig.WithRegion(d.region))
	}
	if d.maxAttempts > 0 {
		opts = append(opts, awsconfig.WithRetryMaxAttempts(d.maxAttempts))
	}

	switch d.authType {
	case AuthTypeAccessKey:
		if d.accessKeyID == "" {
			return aws.Config{}, adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("access key ID is required when using auth type '%s'. Set this via '%s'.", AuthTypeAccessKey, OptionAccessKeyID),
			}
		}
		if d.secretKey == "" {
			return aws.Config{}, adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("secret key is required when using auth type '%s'. Set this via '%s'.", AuthTypeAccessKey, OptionSecretKey),
			}
		}
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(d.accessKeyID, d.secretKey, d.sessionToken),
		))
	case AuthTypeProfile:
		if d.profileName == "" {
			return aws.Config{}, adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("profile name is required when using auth type '%s'. Set this via '%s'.", AuthTypeProfile, OptionProfileName),
			}
		}
		opts = append(opts, awsconfig.WithSharedConfigProfile(d.profileName))
	case AuthTypeDefault, "":
		// use default credential chain — no extra opts needed
	default:
		return aws.Config{}, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("unknown auth type '%s'", d.authType),
		}
	}

	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return aws.Config{}, adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  fmt.Sprintf("failed to build AWS config: %v", err),
		}
	}
	if d.roleARN != "" {
		cfg.Credentials = aws.NewCredentialsCache(stscreds.NewAssumeRoleProvider(
			sts.NewFromConfig(cfg), d.roleARN, func(o *stscreds.AssumeRoleOptions) {
				if d.roleExternalID != "" {
					o.ExternalID = aws.String(d.roleExternalID)
				}
				if d.roleSessionName != "" {
					o.RoleSessionName = d.roleSessionName
				}
				if d.roleDuration > 0 {
					o.Duration = d.roleDuration
				}
			}))
	}
	return cfg, nil
}

func (d *databaseImpl) GetOption(key string) (string, error) {
	switch key {
	case OptionRegion:
		return d.region, nil
	case OptionCatalog:
		return d.catalog, nil
	case OptionSchema:
		return d.schema, nil
	case OptionOutputLocation:
		return d.outputLocation, nil
	case OptionWorkGroup:
		return d.workGroup, nil
	case OptionAuthType:
		return d.authType, nil
	case OptionAccessKeyID:
		return d.accessKeyID, nil
	case OptionSecretKey:
		return d.secretKey, nil
	case OptionSessionToken:
		return d.sessionToken, nil
	case OptionProfileName:
		return d.profileName, nil
	case OptionRoleARN:
		return d.roleARN, nil
	case OptionRoleExternalID:
		return d.roleExternalID, nil
	case OptionRoleSessionName:
		return d.roleSessionName, nil
	case OptionRoleDuration:
		return d.roleDuration.String(), nil
	case OptionMaxAttempts:
		return strconv.Itoa(d.maxAttempts), nil
	case OptionEndpointURL:
		return d.endpointURL, nil
	case OptionPollInterval:
		return d.pollInterval.String(), nil
	case OptionIcebergCommitRetries:
		return strconv.Itoa(d.icebergCommitRetries), nil
	default:
		return d.DatabaseImplBase.GetOption(key)
	}
}

func (d *databaseImpl) SetOptions(options map[string]string) error {
	for k, v := range options {
		if err := d.SetOption(k, v); err != nil {
			return err
		}
	}
	return nil
}

func (d *databaseImpl) SetOption(key, value string) error {
	switch key {
	case OptionRegion:
		d.region = value
	case OptionCatalog:
		d.catalog = value
	case OptionSchema:
		d.schema = value
	case OptionOutputLocation:
		d.outputLocation = value
	case OptionWorkGroup:
		d.workGroup = value
	case OptionAuthType:
		switch value {
		case AuthTypeDefault, AuthTypeAccessKey, AuthTypeProfile:
			d.authType = value
		default:
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("unknown auth type '%s'", value),
			}
		}
	case OptionAccessKeyID:
		d.accessKeyID = value
	case OptionSecretKey:
		d.secretKey = value
	case OptionSessionToken:
		d.sessionToken = value
	case OptionProfileName:
		d.profileName = value
	case OptionRoleARN:
		d.roleARN = value
	case OptionRoleExternalID:
		d.roleExternalID = value
	case OptionRoleSessionName:
		d.roleSessionName = value
	case OptionRoleDuration:
		return parseDurationOption(key, value, &d.roleDuration)
	case OptionMaxAttempts:
		return parseIntOption(key, value, 1, &d.maxAttempts)
	case OptionEndpointURL:
		d.endpointURL = value
	case OptionPollInterval:
		return parseDurationOption(key, value, &d.pollInterval)
	case OptionIcebergCommitRetries:
		return parseIntOption(key, value, 0, &d.icebergCommitRetries)
	default:
		return d.DatabaseImplBase.SetOption(key, value)
	}
	return nil
}

// parseDurationOption parses a positive Go duration ("1h", "500ms") into dst.
func parseDurationOption(key, value string, dst *time.Duration) error {
	v, err := time.ParseDuration(value)
	if err != nil || v <= 0 {
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("'%s' must be a positive duration such as '1s' or '500ms', got '%s'", key, value),
		}
	}
	*dst = v
	return nil
}

// parseIntOption parses an integer of at least minValue into dst.
func parseIntOption(key, value string, minValue int, dst *int) error {
	v, err := strconv.Atoi(value)
	if err != nil || v < minValue {
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("'%s' must be an integer of at least %d, got '%s'", key, minValue, value),
		}
	}
	*dst = v
	return nil
}
