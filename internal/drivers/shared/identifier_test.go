package driversshared

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQuoteIdentifier(t *testing.T) {
	t.Run("PlainName", func(t *testing.T) {
		require.Equal(t, `"users"`, QuoteIdentifier("users"))
	})

	t.Run("NameWithAQuote", func(t *testing.T) {
		require.Equal(t, `"we""ird"`, QuoteIdentifier(`we"ird`))
	})

	t.Run("NameWithASpace", func(t *testing.T) {
		require.Equal(t, `"order list"`, QuoteIdentifier("order list"))
	})

	t.Run("List", func(t *testing.T) {
		require.Equal(t, []string{`"a"`, `"b"`}, QuoteIdentifiers([]string{"a", "b"}))
	})
}
