// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package internal

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding"
)

func TestGenericDecoder(t *testing.T) {
	t.Parallel()

	unmarshalF := func(buf []byte) (plog.Logs, error) {
		logs := plog.NewLogs()
		rl := logs.ResourceLogs().AppendEmpty()
		sl := rl.ScopeLogs().AppendEmpty()
		lr := sl.LogRecords().AppendEmpty()
		lr.Body().SetStr(string(buf))
		return logs, nil
	}

	reader := bytes.NewReader([]byte("simple string input"))

	decodeF, offsetF, err := genericDecoder[plog.Logs](reader, unmarshalF, plog.NewLogs, encoding.WithFlushItems(1))
	require.NoError(t, err)

	// First call should return logs with the input content
	logs, err := decodeF()
	require.NoError(t, err)
	require.Equal(t, 1, logs.ResourceLogs().Len())
	require.Equal(t, "simple string input", logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().Str())
	require.Equal(t, int64(19), offsetF())

	// Second call expect to return EOF with non nil
	logs, err = decodeF()
	require.NotNil(t, logs)
	require.ErrorIs(t, err, io.EOF)
}
