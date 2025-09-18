package eth

import (
    "context"
    "encoding/hex"
    "errors"
    "math/big"
    "strings"
)

// Precomputed function selectors
const (
    selName        = "0x06fdde03"
    selSymbol      = "0x95d89b41"
    selDecimals    = "0x313ce567"
    selTotalSupply = "0x18160ddd"
)

// parseStringOutput decodes ABI-encoded string return value
func parseStringOutput(hexData string) (string, error) {
    // Expect 0x-prefixed hex
    if len(hexData) < 2 || hexData[:2] != "0x" {
        return "", errors.New("invalid hex")
    }
    data, err := hex.DecodeString(hexData[2:])
    if err != nil { return "", err }
    if len(data) < 64 { return "", errors.New("short data") }
    // First 32 bytes = offset (ignored)
    // Next 32 bytes starting at 32 = length
    if len(data) < 64 { return "", errors.New("invalid data") }
    // length is at bytes 32..63
    length := new(big.Int).SetBytes(data[32:64]).Int64()
    if length < 0 { return "", errors.New("invalid length") }
    // data starts at byte 64
    if int64(len(data)) < 64+length { return "", errors.New("incomplete") }
    return string(data[64 : 64+length]), nil
}

// parseBytes32String tries to decode a fixed 32-byte string (bytes32), trimming trailing zeros.
func parseBytes32String(hexData string) (string, error) {
    if len(hexData) < 2 || hexData[:2] != "0x" {
        return "", errors.New("invalid hex")
    }
    data, err := hex.DecodeString(hexData[2:])
    if err != nil { return "", err }
    if len(data) < 32 { return "", errors.New("short data") }
    b := data[len(data)-32:]
    // Remove all null bytes and trailing spaces
    s := strings.Map(func(r rune) rune { if r == 0 { return -1 }; return r }, string(b))
    s = strings.TrimSpace(s)
    return s, nil
}

// parseUint256 decodes a single uint256
func parseUint256(hexData string) (*big.Int, error) {
    if len(hexData) < 2 || hexData[:2] != "0x" { return nil, errors.New("invalid hex") }
    data, err := hex.DecodeString(hexData[2:])
    if err != nil { return nil, err }
    if len(data) < 32 { return nil, errors.New("short data") }
    return new(big.Int).SetBytes(data[len(data)-32:]), nil
}

// parseUint8 decodes the last byte of uint256 as uint8
func parseUint8(hexData string) (uint8, error) {
    n, err := parseUint256(hexData)
    if err != nil { return 0, err }
    return uint8(n.Uint64() & 0xff), nil
}

type ERC20Client struct { rpc *Client }

func NewERC20Client(rpcURL string) *ERC20Client { return &ERC20Client{rpc: NewClient(rpcURL)} }

// GetCode proxies eth_getCode to check if an address is a contract.
func (e *ERC20Client) GetCode(ctx context.Context, address string) (string, error) {
    return e.rpc.EthGetCode(ctx, address)
}

func (e *ERC20Client) Name(ctx context.Context, token string) (string, error) {
    out, err := e.rpc.EthCall(ctx, token, selName)
    if err != nil { return "", err }
    if s, err := parseStringOutput(out); err == nil && s != "" {
        s = strings.Map(func(r rune) rune { if r == 0 { return -1 }; return r }, s)
        return strings.TrimSpace(s), nil
    }
    s, err := parseBytes32String(out)
    if err != nil { return "", err }
    return s, nil
}

func (e *ERC20Client) Symbol(ctx context.Context, token string) (string, error) {
    out, err := e.rpc.EthCall(ctx, token, selSymbol)
    if err != nil { return "", err }
    if s, err := parseStringOutput(out); err == nil && s != "" {
        s = strings.Map(func(r rune) rune { if r == 0 { return -1 }; return r }, s)
        return strings.TrimSpace(s), nil
    }
    s, err := parseBytes32String(out)
    if err != nil { return "", err }
    return s, nil
}

func (e *ERC20Client) Decimals(ctx context.Context, token string) (uint8, error) {
    out, err := e.rpc.EthCall(ctx, token, selDecimals)
    if err != nil { return 0, err }
    return parseUint8(out)
}

func (e *ERC20Client) TotalSupply(ctx context.Context, token string) (*big.Int, error) {
    out, err := e.rpc.EthCall(ctx, token, selTotalSupply)
    if err != nil { return nil, err }
    return parseUint256(out)
}
