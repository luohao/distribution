package hdfs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"

	"github.com/colinmarc/hdfs"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/docker/distribution/registry/storage/driver/factory"
	log "github.com/sirupsen/logrus"
)

const (
	driverName      = "hdfs"
	paramNameNode   = "namenode"
	paramRootDir    = "rootdirectory"
	paramMaxClients = "maxClients"

	// defaultMaxClients is the maximal value for the maxClients configuration
	defaultMaxClients = uint64(1024)
)

// DriverConfig represents all configuration options available for the
// hdfs driver
type DriverConfig struct {
	nameNode   string
	rootPath   string
	maxClients uint64
}

func init() {
	factory.Register(driverName, &hdfsDriverFactory{})
}

// hdfsDriverFactory implements the factory.StorageDriverFactory interface.
type hdfsDriverFactory struct{}

func (factory *hdfsDriverFactory) Create(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	return FromParameters(parameters)
}

type driver struct {
	config DriverConfig
}

// baseEmbed allows us to hide the Base embed.
type baseEmbed struct {
	base.Base
}

// FromParameters constructs a new Driver with a given parameters map
// Required Parameters:
// - hdfs name node URI
// - root path of the storage
func FromParameters(parameters map[string]interface{}) (*Driver, error) {
	config, err := buildConfig(parameters)
	if err != nil {
		return nil, fmt.Errorf("failed to build config: %+v", err)
	}
	return New(*config), nil
}

func buildConfig(parameters map[string]interface{}) (*DriverConfig, error) {
	nn, ok := parameters[paramNameNode]
	if !ok || fmt.Sprint(nn) == "" {
		//return nil, fmt.Errorf("no %s parameter provided", paramNameNode)
		log.Warningf("no namenode provided, will try loading from config file")
		nn = ""
	}

	root, ok := parameters[paramRootDir]
	if !ok || fmt.Sprint(root) == "" {
		return nil, fmt.Errorf("no %s parameter provided", paramRootDir)
	}

	maxClients, err := base.GetLimitFromParameter(parameters[paramMaxClients], 1, defaultMaxClients)
	if err != nil {
		return nil, fmt.Errorf("maxClients config error: %s", err.Error())
	}

	return &DriverConfig{
		nameNode:   fmt.Sprint(nn),
		rootPath:   fmt.Sprint(root),
		maxClients: maxClients,
	}, nil
}

// Driver is a storagedriver.StorageDriver implementation backed by HDFS.
type Driver struct {
	baseEmbed // embedded, hidden base driver.
}

var _ storagedriver.StorageDriver = &Driver{}

// New constructs a new Driver.
func New(config DriverConfig) *Driver {
	hdfsDriver := &driver{config: config}
	log.Infof("created HDFS driver with config %+v", config)

	return &Driver{
		baseEmbed: baseEmbed{
			Base: base.Base{
				StorageDriver: base.NewRegulator(hdfsDriver, config.maxClients),
			},
		},
	}
}

func (d *driver) Name() string {
	return driverName
}

func (d *driver) fullPath(subPath string) string {
	return path.Join(d.config.rootPath, subPath)
}

func (d *driver) newWriter(client *hdfs.Client, fullPath string, hdfsFileWriter *hdfs.FileWriter, offset int64) storagedriver.FileWriter {
	return &fileWriter{
		client: client,
		path:   fullPath,
		file:   hdfsFileWriter,
		size:   offset,
	}
}

// GetContent retrieves the content stored at "path" as a []byte.
func (d *driver) GetContent(ctx context.Context, path string) ([]byte, error) {
	rc, err := d.Reader(ctx, path, 0)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	p, err := ioutil.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// PutContent stores the []byte content at a location designated by "path".
func (d *driver) PutContent(ctx context.Context, subPath string, contents []byte) error {
	writer, err := d.Writer(ctx, subPath, false)
	if err != nil {
		return err
	}
	defer writer.Close()
	_, err = io.Copy(writer, bytes.NewReader(contents))
	if err != nil {
		writer.Cancel()
		return err
	}
	return writer.Commit()
}

// Reader retrieves an io.ReadCloser for the content stored at "path" with a
// given byte offset.
func (d *driver) Reader(ctx context.Context, subPath string, offset int64) (io.ReadCloser, error) {
	client, err := newHdfsClient(d.config.nameNode)
	if err != nil {
		return nil, err
	}
	fullPath := d.fullPath(subPath)
	file, err := client.Open(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, storagedriver.PathNotFoundError{Path: fullPath}
		}

		return nil, err
	}

	seekPos, err := file.Seek(offset, io.SeekStart)
	if err != nil {
		file.Close()
		return nil, err
	} else if seekPos < offset {
		file.Close()
		return nil, storagedriver.InvalidOffsetError{Path: fullPath, Offset: offset}
	}
	return file, nil
}

// Writer returns a FileWriter which will store the content written to it
// at the location designated by "path" after the call to Commit.
func (d *driver) Writer(ctx context.Context, subPath string, append bool) (storagedriver.FileWriter, error) {
	fullPath := d.fullPath(subPath)
	parentDir := path.Dir(fullPath)

	client, err := newHdfsClient(d.config.nameNode)
	if err != nil {
		log.Errorf("failed to create client: %v", err)
		return nil, err
	}

	if err := client.MkdirAll(parentDir, 0777); err != nil {
		log.Errorf("failed to create parent directory: %v", err)
		return nil, err
	}

	var file *hdfs.FileWriter
	var size int64

	fileInfo, err := client.Stat(fullPath)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Errorf("failed to stat file: %v", err)
			return nil, err
		}
	} else {
		// file exists
		if append {
			// if in append mode, record the current size
			size = fileInfo.Size()
		} else {
			// if not in append mode, we need to truncate the file by deleting and recreating the file
			if err := client.Remove(fullPath); err != nil {
				log.Errorf("failed to delete file: %v", err)
				return nil, err
			}
		}
	}

	if append {
		file, err = client.Append(fullPath)
		if err != nil {
			log.Errorf("failed to open file in append mode: %v", err)
			return nil, err
		}
	} else {
		file, err = client.Create(fullPath)
		if err != nil {
			if !os.IsExist(err) {
				log.Errorf("failed to create file: %v", err)
			}
			return nil, err
		}

		// explicitly set the size to zero
		size = 0
	}

	return d.newWriter(client, fullPath, file, size), nil
}

// Stat returns info about the provided path.
func (d *driver) Stat(ctx context.Context, subPath string) (storagedriver.FileInfo, error) {
	fullPath := d.fullPath(subPath)
	client, err := newHdfsClient(d.config.nameNode)
	if err != nil {
		return nil, err
	}

	fi, err := client.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, storagedriver.PathNotFoundError{Path: subPath}
		}

		return nil, err
	}

	return fromOSFileInfo(subPath, fi), nil
}

// List returns a list of the objects that are direct descendants of the given
// path.
func (d *driver) List(ctx context.Context, subPath string) ([]string, error) {
	fullPath := d.fullPath(subPath)
	client, err := newHdfsClient(d.config.nameNode)
	if err != nil {
		return nil, err
	}
	dir, err := client.Open(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, storagedriver.PathNotFoundError{Path: subPath}
		}
		return nil, err
	}
	defer dir.Close()

	fileNames, err := dir.Readdirnames(0)
	if err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(fileNames))
	for _, fileName := range fileNames {
		keys = append(keys, path.Join(subPath, fileName))
	}

	return keys, nil
}

// Move moves an object stored at sourcePath to destPath, removing the original
// object.
func (d *driver) Move(ctx context.Context, sourcePath string, destPath string) error {
	source := d.fullPath(sourcePath)
	dest := d.fullPath(destPath)

	client, err := newHdfsClient(d.config.nameNode)
	if err != nil {
		return err
	}

	if _, err := client.Stat(source); os.IsNotExist(err) {
		return storagedriver.PathNotFoundError{Path: sourcePath}
	}

	if err := client.MkdirAll(path.Dir(dest), 0755); err != nil {
		return err
	}

	err = client.Rename(source, dest)
	return err
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
func (d *driver) Delete(ctx context.Context, subPath string) error {
	fullPath := d.fullPath(subPath)
	client, err := newHdfsClient(d.config.nameNode)
	if err != nil {
		return err
	}

	_, err = client.Stat(fullPath)
	if err != nil && !os.IsNotExist(err) {
		return err
	} else if err != nil {
		return storagedriver.PathNotFoundError{Path: subPath}
	}

	err = client.Remove(fullPath)
	return err
}

// URLFor returns a URL which may be used to retrieve the content stored at the given path.
// May return an UnsupportedMethodErr in certain StorageDriver implementations.
func (d *driver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
	return "", storagedriver.ErrUnsupportedMethod{DriverName: driverName}
}

// Walk traverses a filesystem defined within driver, starting from the given path, calling f on each file
func (d *driver) Walk(ctx context.Context, path string, f storagedriver.WalkFn) error {
	return storagedriver.WalkFallback(ctx, d, path, f)
}

// fileWriter implements storagedriver.FileWriter interface
type fileWriter struct {
	client    *hdfs.Client
	file      *hdfs.FileWriter
	path      string
	size      int64
	closed    bool
	committed bool
	cancelled bool
}

func (fw *fileWriter) Write(p []byte) (int, error) {
	if fw.closed {
		return 0, fmt.Errorf("already closed")
	} else if fw.committed {
		return 0, fmt.Errorf("already committed")
	} else if fw.cancelled {
		return 0, fmt.Errorf("already cancelled")
	}

	n, err := fw.file.Write(p)
	fw.size += int64(n)
	return n, err
}

func (fw *fileWriter) Size() int64 {
	return fw.size
}

func (fw *fileWriter) Close() error {
	if fw.closed {
		return fmt.Errorf("already closed")
	}

	if err := fw.file.Flush(); err != nil {
		return err
	}

	if err := fw.file.Close(); err != nil {
		return err
	}
	fw.closed = true
	return nil
}

func (fw *fileWriter) Cancel() error {
	if fw.closed {
		return fmt.Errorf("already closed")
	}

	fw.cancelled = true
	fw.file.Close()

	return fw.client.Remove(fw.path)
}

func (fw *fileWriter) Commit() error {
	if fw.closed {
		return fmt.Errorf("already closed")
	} else if fw.committed {
		return fmt.Errorf("already committed")
	} else if fw.cancelled {
		return fmt.Errorf("already cancelled")
	}

	if err := fw.file.Flush(); err != nil {
		return err
	}

	fw.committed = true
	return nil
}

// TODO(hluo): add hdfs client pool
func newHdfsClient(nameNode string) (*hdfs.Client, error) {
	return hdfs.New(nameNode)
}

func fromOSFileInfo(path string, info os.FileInfo) storagedriver.FileInfo {
	val := storagedriver.FileInfoInternal{
		FileInfoFields: storagedriver.FileInfoFields{
			Path:    path,
			IsDir:   info.IsDir(),
			Size:    info.Size(),
			ModTime: info.ModTime(),
		},
	}
	return val
}
