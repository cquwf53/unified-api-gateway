package config

import "testing"

func FuzzParseConfig(f *testing.F) {
	for _, seed := range [][]byte{
		[]byte("version: 1\n"),
		[]byte("version: 1\nprofiles: {}\nroutes: []\n"),
		[]byte("version: 1\nunknown: true\n"),
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		cfg, err := Parse(raw)
		if err != nil || cfg == nil {
			return
		}
		_, _ = NewSnapshot(cfg, "fuzz")
	})
}
