// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package internal // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awslambdareceiver/internal"

import (
	"fmt"
	"io"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-lambda-go/events"
	gojson "github.com/goccy/go-json"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	conventions "go.opentelemetry.io/otel/semconv/v1.38.0"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/xstreamencoding"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awslambdareceiver/internal/metadata"
)

// DefaultS3LogsDecoder defines the default S3 logs decoder for AWS Lambda receiver
type DefaultS3LogsDecoder struct{}

// NewLogsDecoder returns a LogsDecoder that decodes logs from a S3 object containing logs.
// It implements the decoder contract and process all data at once. Offset is full length of data in bytes.
// Decoding adds event message as log record body and adds resource attributes for owner, log group and log stream.
func (*DefaultS3LogsDecoder) NewLogsDecoder(reader io.Reader, opts ...encoding.DecoderOption) (encoding.LogsDecoder, error) {
	options := encoding.DecoderOptions{}
	for _, opt := range opts {
		opt(&options)
	}

	if options.Offset > 0 {
		// consume all and send an EOF
		_, err := io.Copy(io.Discard, reader)
		if err != nil {
			return nil, fmt.Errorf("failed to consume reader data: %w", err)
		}
		return xstreamencoding.NewLogsDecoderAdapter(
			func() (plog.Logs, error) {
				return plog.NewLogs(), io.EOF
			},
			func() int64 {
				return options.Offset
			}), nil
	}

	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName(metadata.ScopeName)

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	lr := sl.LogRecords().AppendEmpty()
	if utf8.Valid(data) {
		lr.Body().SetStr(string(data))
	} else {
		lr.Body().SetEmptyBytes().FromRaw(data)
	}

	isEOF := false
	return xstreamencoding.NewLogsDecoderAdapter(
			func() (plog.Logs, error) {
				if isEOF {
					return plog.NewLogs(), io.EOF
				}

				isEOF = true
				return logs, nil
			},
			func() int64 {
				return int64(len(data))
			}),
		nil
}

// DefaultCWLogsDecoder defines the default CloudWatch logs decoder for AWS Lambda receiver.
type DefaultCWLogsDecoder struct{}

// NewLogsDecoder returns a LogsDecoder that decodes logs from a CloudWatch logs event.
// It implements the decoder contract and process all data at once. Offset is full length of data in bytes.
// Decoding adds event message as log record body and adds resource attributes for owner, log group and log stream.
func (*DefaultCWLogsDecoder) NewLogsDecoder(reader io.Reader, opts ...encoding.DecoderOption) (encoding.LogsDecoder, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	options := encoding.DecoderOptions{}
	for _, opt := range opts {
		opt(&options)
	}

	if options.Offset > 0 {
		return xstreamencoding.NewLogsDecoderAdapter(
			func() (plog.Logs, error) {
				return plog.NewLogs(), io.EOF
			},
			func() int64 {
				return options.Offset
			}), nil
	}

	var cwLog events.CloudwatchLogsData
	err = gojson.Unmarshal(data, &cwLog)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal data from cloudwatch logs event: %w", err)
	}

	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	resourceAttrs := rl.Resource().Attributes()
	resourceAttrs.PutStr(string(conventions.CloudProviderKey), conventions.CloudProviderAWS.Value.AsString())
	resourceAttrs.PutStr(string(conventions.CloudAccountIDKey), cwLog.Owner)
	resourceAttrs.PutStr(string(conventions.AWSLogGroupNamesKey), cwLog.LogGroup)
	resourceAttrs.PutStr(string(conventions.AWSLogStreamNamesKey), cwLog.LogStream)

	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName(metadata.ScopeName)

	for _, event := range cwLog.LogEvents {
		logRecord := sl.LogRecords().AppendEmpty()
		// pcommon.Timestamp is a time specified as UNIX Epoch time in nanoseconds
		// but timestamp in cloudwatch logs are in milliseconds.
		logRecord.SetTimestamp(pcommon.Timestamp(event.Timestamp * int64(time.Millisecond)))
		logRecord.Body().SetStr(event.Message)
	}

	isEOF := false
	return xstreamencoding.NewLogsDecoderAdapter(
			func() (plog.Logs, error) {
				if isEOF {
					return plog.NewLogs(), io.EOF
				}

				isEOF = true
				return logs, nil
			},
			func() int64 {
				return int64(len(data))
			}),
		nil
}
