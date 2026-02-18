// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package internal // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awslambdareceiver/internal"

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/xstreamencoding"
)

// DecodeWrapper wraps non-streaming unmarshal functions to provide streaming decoder capabilities.
type DecodeWrapper struct {
	unmarshalLog    func(buf []byte) (plog.Logs, error)
	unmarshalMetric func(buf []byte) (pmetric.Metrics, error)
}

// NewLogsDecoderWrapper creates a DecodeWrapper for logs.
func NewLogsDecoderWrapper(unmarshalLog func(buf []byte) (plog.Logs, error)) *DecodeWrapper {
	return &DecodeWrapper{
		unmarshalLog: unmarshalLog,
	}
}

// NewMetricsDecoderWrapper creates a DecodeWrapper for metrics.
func NewMetricsDecoderWrapper(unmarshalMetric func(buf []byte) (pmetric.Metrics, error)) *DecodeWrapper {
	return &DecodeWrapper{
		unmarshalMetric: unmarshalMetric,
	}
}

func (*DecodeWrapper) Start(_ context.Context, _ component.Host) error {
	// NOOP
	return nil
}

func (l *DecodeWrapper) NewLogsDecoder(reader io.Reader, options ...encoding.DecoderOption) (encoding.LogsDecoder, error) {
	decode, offset, err := genericDecoder[plog.Logs](reader, l.unmarshalLog, plog.NewLogs, options...)
	if err != nil {
		return nil, err
	}

	return xstreamencoding.NewLogsDecoderAdapter(decode, offset), nil
}

func (l *DecodeWrapper) NewMetricsDecoder(reader io.Reader, options ...encoding.DecoderOption) (encoding.MetricsDecoder, error) {
	decode, offset, err := genericDecoder[pmetric.Metrics](reader, l.unmarshalMetric, pmetric.NewMetrics, options...)
	if err != nil {
		return nil, err
	}

	return xstreamencoding.NewMetricsDecoderAdapter(decode, offset), nil
}

func (*DecodeWrapper) Shutdown(_ context.Context) error {
	// NOOP
	return nil
}

// genericDecoder is a generic helper function to create decode and offset functions for both logs and metrics decoders based on the provided unmarshal function and reader.
func genericDecoder[T plog.Logs | pmetric.Metrics](reader io.Reader, unmarshalF func(buf []byte) (T, error), zeroF func() T, options ...encoding.DecoderOption) (func() (T, error), func() int64, error) {
	scannerHelper, err := xstreamencoding.NewScannerHelper(reader, options...)
	if err != nil {
		return nil, nil, err
	}

	buffer := bytes.NewBuffer(make([]byte, 0))

	decoderF := func() (T, error) {
		var b []byte
		var flush bool
		var isEOF bool

		for {
			b, flush, err = scannerHelper.ScanBytes()
			if err != nil {
				if !errors.Is(err, io.EOF) {
					return zeroF(), fmt.Errorf("failed to read stream data: %w", err)
				}

				isEOF = true
			}

			buffer.Write(b)
			if flush {
				var logs T
				logs, err = unmarshalF(buffer.Bytes())
				if err != nil {
					return zeroF(), fmt.Errorf("failed to unmarshal data: %w", err)
				}

				buffer.Reset()
				return logs, nil
			}

			if isEOF {
				break
			}
		}

		return zeroF(), io.EOF
	}

	return decoderF, scannerHelper.Offset, nil
}
