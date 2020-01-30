package hdfs

import (
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/testsuites"
	"gopkg.in/check.v1"
	"os"
	"testing"
)

const (
	hdfsNameNodeKeyEnv = "HDFS_NAMENODE"
	hdfsRootPathEnv    = "HDFS_ROOT"
	maxHdfsClientEnv   = "MAX_HDFS_CLIENT"
)

// Test hooks up gocheck into the "go test" runner.
func Test(t *testing.T) { check.TestingT(t) }

func init() {
	hdfsNameNode := os.Getenv(hdfsNameNodeKeyEnv)
	hdfsRootPath := os.Getenv(hdfsRootPathEnv)

	// Skip HDFS storage driver tests if environment variable parameters are not provided
	skipHDFS := func() string {
		if hdfsNameNode == "" || hdfsRootPath == "" {
			return "The following environment variables must be set to enable these tests: HDFS_NAMENODE, HDFS_ROOT"
		}
		return ""
	}

	hdfsDriverConstructor := func() (storagedriver.StorageDriver, error) {
		config := DriverConfig{
			nameNode:   hdfsNameNode,
			rootPath:   hdfsRootPath,
			maxClients: 8,
		}
		return New(config), nil
	}

	testsuites.RegisterSuite(hdfsDriverConstructor, skipHDFS)
}
