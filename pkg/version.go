package switchback

import "fmt"

const (
	VersionMajor   = 0
	VersionMinor   = 1
	VersionPatch   = 0
	VersionRelease = 0
)

func Version() string {
	return fmt.Sprintf("%d.%d.%d", VersionMajor, VersionMinor, VersionPatch)
}
