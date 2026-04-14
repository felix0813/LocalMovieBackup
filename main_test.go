package main

import "testing"

func TestSanitizeName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "keeps chinese and latin", in: "测试 backup_01", want: "测试-backup_01"},
		{name: "strips punctuation", in: "###测试@@", want: "测试"},
		{name: "fallback when empty", in: "   ", want: "backup"},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := sanitizeName(tc.in); got != tc.want {
				t.Fatalf("sanitizeName(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
