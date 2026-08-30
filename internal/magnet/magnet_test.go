package magnet

import (
	"reflect"
	"testing"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name    string
		uri     string
		want    *Magnet
		wantErr bool
	}{
		{
			name: "valid hex magnet",
			uri:  "magnet:?xt=urn:btih:c12fe1c06bba254a9dc9f519b335aa7c1367a88a&dn=ubuntu.iso&tr=http%3A%2F%2Ftracker.com%2Fannounce",
			want: &Magnet{
				InfoHash:    [20]byte{0xc1, 0x2f, 0xe1, 0xc0, 0x6b, 0xba, 0x25, 0x4a, 0x9d, 0xc9, 0xf5, 0x19, 0xb3, 0x35, 0xaa, 0x7c, 0x13, 0x67, 0xa8, 0x8a},
				DisplayName: "ubuntu.iso",
				Trackers:    []string{"http://tracker.com/announce"},
			},
			wantErr: false,
		},
		{
			name: "valid base32 magnet",
			uri:  "magnet:?xt=urn:btih:YEX6DQDLXISUVHOJ6UM3GNNKPQJWPKEK",
			want: &Magnet{
				InfoHash:    [20]byte{0xc1, 0x2f, 0xe1, 0xc0, 0x6b, 0xba, 0x25, 0x4a, 0x9d, 0xc9, 0xf5, 0x19, 0xb3, 0x35, 0xaa, 0x7c, 0x13, 0x67, 0xa8, 0x8a},
				DisplayName: "",
				Trackers:    nil,
			},
			wantErr: false,
		},
		{
			name:    "invalid scheme",
			uri:     "http://example.com/file.torrent",
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.uri)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Parse() got = %v, want %v", got, tt.want)
			}
		})
	}
}
