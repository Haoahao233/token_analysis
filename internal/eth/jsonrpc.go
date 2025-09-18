package eth

import (
    "bytes"
    "context"
    "encoding/json"
    "io"
    "net/http"
    "time"
)

type Client struct {
    url    string
    httpc  *http.Client
}

func NewClient(url string) *Client {
    return &Client{url: url, httpc: &http.Client{Timeout: 10 * time.Second}}
}

type rpcReq struct {
    JSONRPC string      `json:"jsonrpc"`
    Method  string      `json:"method"`
    Params  interface{} `json:"params"`
    ID      int         `json:"id"`
}

type rpcResp struct {
    JSONRPC string           `json:"jsonrpc"`
    ID      int              `json:"id"`
    Result  json.RawMessage  `json:"result"`
    Error   *rpcError        `json:"error"`
}

type rpcError struct {
    Code    int    `json:"code"`
    Message string `json:"message"`
}

func (c *Client) call(ctx context.Context, method string, params interface{}, result interface{}) error {
    body, _ := json.Marshal(rpcReq{JSONRPC: "2.0", Method: method, Params: params, ID: 1})
    req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
    if err != nil { return err }
    req.Header.Set("Content-Type", "application/json")
    resp, err := c.httpc.Do(req)
    if err != nil { return err }
    defer resp.Body.Close()
    b, err := io.ReadAll(resp.Body)
    if err != nil { return err }
    var rr rpcResp
    if err := json.Unmarshal(b, &rr); err != nil { return err }
    if rr.Error != nil { return &RPCError{Code: rr.Error.Code, Message: rr.Error.Message} }
    if result != nil {
        if err := json.Unmarshal(rr.Result, result); err != nil { return err }
    }
    return nil
}

type RPCError struct {
    Code int
    Message string
}

func (e *RPCError) Error() string { return e.Message }

// EthCall performs eth_call and returns hex-encoded data string.
func (c *Client) EthCall(ctx context.Context, to string, data string) (string, error) {
    // params: [ { to, data }, "latest" ]
    var res string
    err := c.call(ctx, "eth_call", []interface{}{map[string]string{"to": to, "data": data}, "latest"}, &res)
    return res, err
}

// EthGetCode returns the runtime code at an address; empty code means non-contract.
func (c *Client) EthGetCode(ctx context.Context, address string) (string, error) {
    var res string
    if err := c.call(ctx, "eth_getCode", []interface{}{address, "latest"}, &res); err != nil {
        return "", err
    }
    return res, nil
}
