package snapshot

import (
	"context"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	connect "connectrpc.com/connect"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	utxocardano "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/query"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/query/queryconnect"
	"golang.org/x/net/http2"
	"gorm.io/gorm"

	"nectar/models"
)

const (
	ProtocolGRPC    = "grpc"
	ProtocolConnect = "connect"
)

type UTxORPCOptions struct {
	Endpoint    string
	Protocol    string
	BatchSize   int32
	Limit       int64
	Policy      []byte
	AssetName   []byte
	Apply       bool
	ReplaceUTxO bool
	Timeout     time.Duration
}

type UTxORPCStats struct {
	Pages          int64
	UTxOs          int64
	TxOuts         int64
	MaTxOuts       int64
	MultiAssets    int64
	Skipped        int64
	LastTipSlot    uint64
	LastTipHashHex string
	Applied        bool
}

type UTxORPCImporter struct {
	db      *gorm.DB
	options UTxORPCOptions
}

func NewUTxORPCImporter(db *gorm.DB, options UTxORPCOptions) *UTxORPCImporter {
	if options.BatchSize <= 0 {
		options.BatchSize = 1000
	}
	if options.Protocol == "" {
		options.Protocol = ProtocolGRPC
	}
	if options.Timeout <= 0 {
		options.Timeout = 30 * time.Second
	}
	return &UTxORPCImporter{
		db:      db,
		options: options,
	}
}

func (i *UTxORPCImporter) Import(ctx context.Context) (UTxORPCStats, error) {
	if strings.TrimSpace(i.options.Endpoint) == "" {
		return UTxORPCStats{}, fmt.Errorf("utxorpc endpoint is required")
	}
	if i.options.Apply && i.db == nil {
		return UTxORPCStats{}, fmt.Errorf("database handle is required when apply is enabled")
	}

	client, err := newQueryClient(i.options.Endpoint, i.options.Protocol)
	if err != nil {
		return UTxORPCStats{}, err
	}

	stats := UTxORPCStats{Applied: i.options.Apply}
	if i.options.Apply && i.options.ReplaceUTxO {
		if err := clearCurrentUTxOTables(i.db); err != nil {
			return stats, err
		}
	}

	predicate := buildPredicate(i.options.Policy, i.options.AssetName)
	token := ""
	for {
		reqCtx, cancel := context.WithTimeout(ctx, i.options.Timeout)
		resp, err := client.SearchUtxos(reqCtx, connect.NewRequest(&query.SearchUtxosRequest{
			Predicate:  predicate,
			MaxItems:   i.options.BatchSize,
			StartToken: token,
		}))
		cancel()
		if err != nil {
			return stats, fmt.Errorf("search utxos: %w", err)
		}

		items := resp.Msg.GetItems()
		if i.options.Limit > 0 {
			remaining := i.options.Limit - stats.UTxOs
			if remaining <= 0 {
				break
			}
			if int64(len(items)) > remaining {
				items = items[:remaining]
			}
		}

		pageStats, err := i.handlePage(items)
		if err != nil {
			return stats, err
		}
		stats.Pages++
		stats.UTxOs += pageStats.UTxOs
		stats.TxOuts += pageStats.TxOuts
		stats.MaTxOuts += pageStats.MaTxOuts
		stats.MultiAssets += pageStats.MultiAssets
		stats.Skipped += pageStats.Skipped

		if tip := resp.Msg.GetLedgerTip(); tip != nil {
			stats.LastTipSlot = tip.GetSlot()
			stats.LastTipHashHex = hex.EncodeToString(tip.GetHash())
		}

		if i.options.Limit > 0 && stats.UTxOs >= i.options.Limit {
			break
		}
		token = resp.Msg.GetNextToken()
		if token == "" {
			break
		}
	}

	return stats, nil
}

func (i *UTxORPCImporter) handlePage(items []*query.AnyUtxoData) (UTxORPCStats, error) {
	stats := UTxORPCStats{}
	txOuts := make([]models.TxOut, 0, len(items))
	maTxOuts := make([]models.MaTxOut, 0)
	multiAssets := make(map[string]models.MultiAsset)

	for _, item := range items {
		stats.UTxOs++
		txoRef := item.GetTxoRef()
		output := item.GetCardano()
		if txoRef == nil || output == nil || len(txoRef.GetHash()) == 0 {
			stats.Skipped++
			continue
		}

		txOut, err := convertTxOut(txoRef, output)
		if err != nil {
			stats.Skipped++
			continue
		}
		txOuts = append(txOuts, txOut)
		stats.TxOuts++

		for _, ma := range output.GetAssets() {
			policy := append([]byte{}, ma.GetPolicyId()...)
			if len(policy) == 0 {
				continue
			}
			for _, asset := range ma.GetAssets() {
				name := append([]byte{}, asset.GetName()...)
				quantity := asset.GetOutputCoin()
				if quantity == 0 {
					continue
				}
				key := hex.EncodeToString(policy) + "." + hex.EncodeToString(name)
				if _, ok := multiAssets[key]; !ok {
					multiAssets[key] = models.MultiAsset{
						Policy:      append([]byte{}, policy...),
						Name:        append([]byte{}, name...),
						Fingerprint: common.NewAssetFingerprint(policy, name).String(),
					}
				}
				maTxOuts = append(maTxOuts, models.MaTxOut{
					TxHash:   append([]byte{}, txOut.TxHash...),
					TxIndex:  txOut.Index,
					Policy:   append([]byte{}, policy...),
					Name:     append([]byte{}, name...),
					Quantity: quantity,
				})
				stats.MaTxOuts++
			}
		}
	}

	stats.MultiAssets = int64(len(multiAssets))
	if !i.options.Apply {
		return stats, nil
	}

	return stats, writeCurrentUTxOBatch(i.db, currentUTxOBatch{
		TxOuts:      txOuts,
		MaTxOuts:    maTxOuts,
		MultiAssets: multiAssets,
	})
}

func buildPredicate(policy []byte, assetName []byte) *query.UtxoPredicate {
	if len(policy) == 0 && len(assetName) == 0 {
		return nil
	}
	return &query.UtxoPredicate{
		Match: &query.AnyUtxoPattern{
			UtxoPattern: &query.AnyUtxoPattern_Cardano{
				Cardano: &utxocardano.TxOutputPattern{
					Asset: &utxocardano.AssetPattern{
						PolicyId:  append([]byte{}, policy...),
						AssetName: append([]byte{}, assetName...),
					},
				},
			},
		},
	}
}

func convertTxOut(ref *query.TxoRef, output *utxocardano.TxOutput) (models.TxOut, error) {
	rawAddress := output.GetAddress()
	address, err := common.NewAddressFromBytes(rawAddress)
	if err != nil {
		return models.TxOut{}, fmt.Errorf("decode address: %w", err)
	}
	addressString := address.String()
	addressBytes, err := address.Bytes()
	if err != nil {
		addressBytes = append([]byte{}, rawAddress...)
	}

	txOut := models.TxOut{
		TxHash:           append([]byte{}, ref.GetHash()...),
		Index:            ref.GetIndex(),
		Address:          addressString,
		AddressRaw:       addressBytes,
		AddressHasScript: addressHasScript(addressBytes),
		PaymentCred:      paymentCredential(addressBytes),
		StakeAddressHash: stakeCredential(addressBytes),
		Value:            output.GetCoin(),
	}
	if datum := output.GetDatum(); datum != nil {
		txOut.DataHash = append([]byte{}, datum.GetHash()...)
		txOut.InlineDatumHash = append([]byte{}, datum.GetHash()...)
	}
	return txOut, nil
}

func addressHasScript(addressBytes []byte) bool {
	if len(addressBytes) == 0 {
		return false
	}
	switch addressBytes[0] >> 4 {
	case 1, 2, 3, 5, 7, 15:
		return true
	default:
		return false
	}
}

func paymentCredential(addressBytes []byte) []byte {
	if len(addressBytes) < 29 {
		return nil
	}
	return append([]byte{}, addressBytes[1:29]...)
}

func stakeCredential(addressBytes []byte) []byte {
	if len(addressBytes) < 57 {
		return nil
	}
	addrType := addressBytes[0] >> 4
	if addrType > 1 {
		return nil
	}
	return append([]byte{}, addressBytes[29:57]...)
}

func clearCurrentUTxOTables(db *gorm.DB) error {
	return db.Transaction(func(tx *gorm.DB) error {
		for _, stmt := range []string{
			"DELETE FROM token_holders",
			"DELETE FROM ma_tx_outs",
			"DELETE FROM tx_outs",
		} {
			if err := tx.Exec(stmt).Error; err != nil {
				return fmt.Errorf("%s: %w", stmt, err)
			}
		}
		return nil
	})
}

func newQueryClient(endpoint string, protocol string) (queryconnect.QueryServiceClient, error) {
	endpoint = normalizeEndpoint(endpoint)
	switch strings.ToLower(strings.TrimSpace(protocol)) {
	case "", ProtocolGRPC:
		return queryconnect.NewQueryServiceClient(h2cClient(), endpoint, connect.WithGRPC()), nil
	case ProtocolConnect:
		return queryconnect.NewQueryServiceClient(http.DefaultClient, endpoint), nil
	default:
		return nil, fmt.Errorf("unsupported utxorpc protocol %q", protocol)
	}
}

func normalizeEndpoint(endpoint string) string {
	endpoint = strings.TrimRight(strings.TrimSpace(endpoint), "/")
	if strings.HasPrefix(endpoint, "http://") || strings.HasPrefix(endpoint, "https://") {
		return endpoint
	}
	return "http://" + endpoint
}

func h2cClient() *http.Client {
	return &http.Client{
		Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
				var dialer net.Dialer
				return dialer.DialContext(ctx, network, addr)
			},
		},
	}
}
