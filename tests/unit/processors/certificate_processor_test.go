package processors_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"nectar/models"
	"nectar/processors"
)

type testMetadataFetcher struct{}

func (testMetadataFetcher) QueuePoolMetadata(poolHash, pmrHash []byte, url string, hash []byte) error {
	return nil
}

func (testMetadataFetcher) QueueGovernanceMetadata(anchorHash []byte, url string, hash []byte) error {
	return nil
}

func setupCertificateProcessorDB(t *testing.T) *gorm.DB {
	t.Helper()

	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(
		&models.PoolHash{},
		&models.StakeAddress{},
		&models.StakeRegistration{},
		&models.StakeDeregistration{},
		&models.Delegation{},
	))

	return db
}

func TestNewCertificateProcessor(t *testing.T) {
	db := setupCertificateProcessorDB(t)
	require.NoError(t, db.Create(&models.PoolHash{
		HashRaw: testBytes(28, 0x51),
		View:    "pool1test",
	}).Error)

	processor := processors.NewCertificateProcessor(db, processors.NewStakeAddressCache(db))

	require.NotNil(t, processor)
	processor.SetMetadataFetcher(testMetadataFetcher{})
}

func TestProcessCertificatesNoopsForEmptyNilAndUnknownCertificates(t *testing.T) {
	db := setupCertificateProcessorDB(t)
	processor := processors.NewCertificateProcessor(db, processors.NewStakeAddressCache(db))

	assert.NoError(t, processor.ProcessCertificates(nil, db, testBytes(32, 0x52), nil))
	assert.NoError(t, processor.ProcessCertificates(nil, db, testBytes(32, 0x52), []interface{}{nil, struct{ Name string }{"unknown"}}))
}
