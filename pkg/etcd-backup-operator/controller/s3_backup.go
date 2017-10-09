package controller

import (
	"fmt"
	"io/ioutil"
	"os"

	api "github.com/coreos/etcd-operator/pkg/apis/etcd/v1beta2"
	"github.com/coreos/etcd-operator/pkg/backup"
	"github.com/coreos/etcd-operator/pkg/backup/backend"
	"github.com/coreos/etcd-operator/pkg/backup/backupapi"
	backupS3 "github.com/coreos/etcd-operator/pkg/backup/s3"
	"github.com/coreos/etcd-operator/pkg/cluster/backupstorage"

	"github.com/aws/aws-sdk-go/aws/session"
)

const (
	tmp = "/tmp"
)

// TODO: remove this and use backend interface for other options (PV, Azure)
// handleS3 backups up etcd cluster to s3.
func (b *Backup) handleS3(clusterName string, s3 *api.S3Source) error {
	awsDir, so, err := b.setupAWSConfig(s3.AWSSecret)
	if err != nil {
		return fmt.Errorf("failed to set up aws config: (%v)", err)
	}
	defer os.RemoveAll(awsDir)

	prefix := backupapi.ToS3Prefix(s3.Prefix, b.namespace, clusterName)
	s3Dir, be, err := makeS3Backend(so, prefix, s3.S3Bucket)
	if err != nil {
		return fmt.Errorf("failed to create s3 backend: (%v)", err)
	}
	defer os.RemoveAll(s3Dir)

	bm := backup.NewBackupManager(b.kubecli, clusterName, b.namespace, nil, be)
	// this SaveSnap takes 0 as lastSnapRev to indicate that it
	// saves any snapshot with revision > 0.
	_, err = bm.SaveSnap(0)
	if err != nil {
		return fmt.Errorf("failed to save snapshot (%v)", err)
	}
	return nil
}

func (b *Backup) setupAWSConfig(secret string) (string, *session.Options, error) {
	awsDir, err := ioutil.TempDir(tmp, "")
	if err != nil {
		return "", nil, fmt.Errorf("failed to create aws config/cred dir: (%v)", err)
	}

	so, err := backupstorage.SetupAWSConfig(b.kubecli, b.namespace, secret, awsDir)
	if err != nil {
		return "", nil, fmt.Errorf("failed to setup aws config: (%v)", err)
	}

	return awsDir, so, nil
}

func makeS3Backend(so *session.Options, prefix, bucket string) (string, backend.Backend, error) {
	s3cli, err := backupS3.NewFromSessionOpt(bucket, prefix, *so)
	if err != nil {
		return "", nil, fmt.Errorf("failed to create aws cli: (%v)", err)
	}

	backupDir, err := ioutil.TempDir(tmp, "")
	if err != nil {
		return "", nil, err
	}

	return backupDir, backend.NewS3Backend(s3cli, backupDir), nil
}
