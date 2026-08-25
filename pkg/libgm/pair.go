package libgm

import (
	"context"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	"go.mau.fi/mautrix-gmessages/pkg/libgm/events"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/util"
)

func (c *Client) StartLogin() (string, error) {
	registered, err := c.RegisterPhoneRelay()
	if err != nil {
		return "", err
	}
	c.updateTachyonAuthToken(registered.GetAuthKeyData())
	c.startAuxiliaryLongPoll(context.Background())
	qr, err := c.GenerateQRCodeData(registered.GetPairingKey())
	if err != nil {
		return "", fmt.Errorf("failed to generate QR code: %w", err)
	}
	return qr, nil
}

func (c *Client) GenerateQRCodeData(pairingKey []byte) (string, error) {
	auth := c.AuthData.Snapshot()
	defer auth.ClearSecrets()
	urlData := &gmproto.URLData{
		PairingKey: pairingKey,
		AESKey:     auth.RequestCrypto.AESKey,
		HMACKey:    auth.RequestCrypto.HMACKey,
	}
	encodedURLData, err := proto.Marshal(urlData)
	if err != nil {
		return "", err
	}
	cData := base64.StdEncoding.EncodeToString(encodedURLData)
	return util.QRCodeURLBase + cData, nil
}

func (c *Client) handlePairingEvent(msg *IncomingRPCMessage) {
	switch evt := msg.Pair.Event.(type) {
	case *gmproto.RPCPairData_Paired:
		c.completePairing(evt.Paired)
	case *gmproto.RPCPairData_Revoked:
		c.triggerEvent(evt.Revoked)
	default:
		c.Logger.Debug().Type("event_type", evt).Msg("Unknown pair event type")
	}
}

func (c *Client) completePairing(data *gmproto.PairedData) {
	c.updateTachyonAuthToken(data.GetTokenData())
	c.AuthData.SetDevices(data.Browser, data.Mobile)

	if cb := c.PairCallback.Load(); cb != nil {
		(*cb)(data)
	} else {
		c.triggerEvent(&events.PairSuccessful{PhoneID: data.GetMobile().GetSourceID(), QRData: data})

		// Wait before reconnecting so the phone can save the pair data. The
		// auxiliary lifecycle owns this delay, so Disconnect can cancel and join it.
		if !c.requestAuxiliaryReconnect(2 * time.Second) {
			c.triggerEvent(&events.ListenFatalError{Error: errors.New("pairing lifecycle stopped before reconnect")})
		}
	}
}

func (c *Client) RegisterPhoneRelay() (*gmproto.RegisterPhoneRelayResponse, error) {
	auth := c.AuthData.Snapshot()
	defer auth.ClearSecrets()
	key, err := x509.MarshalPKIXPublicKey(auth.RefreshKey.GetPublicKey())
	if err != nil {
		return nil, err
	}

	payload := &gmproto.AuthenticationContainer{
		AuthMessage: &gmproto.AuthMessage{
			RequestID:     uuid.NewString(),
			Network:       util.QRNetwork,
			ConfigVersion: util.ConfigMessage,
		},
		BrowserDetails: util.BrowserDetailsMessage,
		Data: &gmproto.AuthenticationContainer_KeyData{
			KeyData: &gmproto.KeyData{
				EcdsaKeys: &gmproto.ECDSAKeys{
					Field1:        2,
					EncryptedKeys: key,
				},
			},
		},
	}
	return typedHTTPResponse[*gmproto.RegisterPhoneRelayResponse](
		c.makeProtobufHTTPRequest(util.RegisterPhoneRelayURL, payload, ContentTypeProtobuf),
	)
}

func (c *Client) RefreshPhoneRelay() (string, error) {
	auth := c.AuthData.Snapshot()
	defer auth.ClearSecrets()
	payload := &gmproto.AuthenticationContainer{
		AuthMessage: &gmproto.AuthMessage{
			RequestID:        uuid.NewString(),
			Network:          util.QRNetwork,
			TachyonAuthToken: auth.TachyonAuthToken,
			ConfigVersion:    util.ConfigMessage,
		},
	}
	res, err := typedHTTPResponse[*gmproto.RefreshPhoneRelayResponse](
		c.makeProtobufHTTPRequest(util.RefreshPhoneRelayURL, payload, ContentTypeProtobuf),
	)
	if err != nil {
		return "", err
	}
	qr, err := c.GenerateQRCodeData(res.GetPairKey())
	if err != nil {
		return "", err
	}
	return qr, nil
}

func (c *Client) GetWebEncryptionKey() (*gmproto.WebEncryptionKeyResponse, error) {
	auth := c.AuthData.Snapshot()
	defer auth.ClearSecrets()
	payload := &gmproto.AuthenticationContainer{
		AuthMessage: &gmproto.AuthMessage{
			RequestID:        uuid.NewString(),
			TachyonAuthToken: auth.TachyonAuthToken,
			ConfigVersion:    util.ConfigMessage,
		},
	}
	return typedHTTPResponse[*gmproto.WebEncryptionKeyResponse](
		c.makeProtobufHTTPRequest(util.GetWebEncryptionKeyURL, payload, ContentTypeProtobuf),
	)
}

func (c *Client) UnpairBugle() (*gmproto.RevokeRelayPairingResponse, error) {
	auth := c.AuthData.Snapshot()
	defer auth.ClearSecrets()
	if auth.TachyonAuthToken == nil || auth.Browser == nil {
		return nil, nil
	}
	payload := &gmproto.RevokeRelayPairingRequest{
		AuthMessage: &gmproto.AuthMessage{
			RequestID:        uuid.NewString(),
			TachyonAuthToken: auth.TachyonAuthToken,
			ConfigVersion:    util.ConfigMessage,
		},
		Browser: auth.Browser,
	}
	return typedHTTPResponse[*gmproto.RevokeRelayPairingResponse](
		c.makeProtobufHTTPRequest(util.RevokeRelayPairingURL, payload, ContentTypeProtobuf),
	)
}

func (c *Client) Unpair(ctx context.Context) (err error) {
	if c.AuthData.HasCookies() {
		err = c.UnpairGaia(ctx)
	} else {
		_, err = c.UnpairBugle()
	}
	return
}
