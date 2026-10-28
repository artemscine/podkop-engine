package option

import (
	"encoding/json"
	"testing"
)

func TestFinalMaskValidation(t *testing.T) {
	for _, test := range []struct {
		name, settings string
		valid          bool
	}{
		{"raw", `{"packets":"1-3","length":"10-20","delay":"10-20"}`, true},
		{"hello", `{"packets":"tlshello","length":20}`, true},
		{"all", `{"length":20}`, true},
		{"arrays", `{"lengths":[0,1,"3-6"],"delays":[0,"1-3"],"maxSplit":"3-6"}`, true},
		{"missing-length", `{"packets":"1-3"}`, false},
		{"last-zero", `{"lengths":[20,0]}`, false},
		{"negative-length", `{"length":-1}`, false},
		{"negative-delay", `{"length":20,"delay":-1}`, false},
		{"negative-limit", `{"length":20,"maxSplit":-1}`, false},
		{"zero-packet", `{"length":20,"packets":"0-3"}`, false},
		{"bad-packet", `{"length":20,"packets":"not-a-range"}`, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			var settings FinalMaskFragmentOptions
			if err := json.Unmarshal([]byte(test.settings), &settings); err != nil {
				t.Fatal(err)
			}
			err := settings.Validate()
			if (err == nil) != test.valid {
				t.Fatalf("valid=%v, error=%v", test.valid, err)
			}
		})
	}
}
