package util

import (
	"os"
	"strings"
)

var isNamespaceHosted = false
var meshID = ""

func init() {
	meshID = os.Getenv("MESH_ID")
	meshType := os.Getenv("MESH_TYPE")
	isNamespaceHosted = strings.ToLower(meshType) == "namespace_hosted"
}
func IsNamespaceHosted() bool {
	return isNamespaceHosted
}

func MeshID() string {
	return meshID
}
