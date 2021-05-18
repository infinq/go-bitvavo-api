package bitvavo

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

var rateLimitRemaining = 1000
var rateLimitReset = 0

// Put mutex on globals to prevent race conditions
var rateLimitRemainingMu sync.Mutex
var rateLimitResetMu sync.Mutex

type TimeResponse struct {
	Action   string `json:"action"`
	Response Time   `json:"response"`
}

type Time struct {
	Time int64 `json:"time"`
}

type MarketsResponse struct {
	Action   string    `json:"action"`
	Response []Markets `json:"response"`
}

type Markets struct {
	Status               string   `json:"status"`
	Base                 string   `json:"base"`
	Quote                string   `json:"quote"`
	Market               string   `json:"market"`
	PricePrecision       int      `json:"pricePrecision"`
	MinOrderInQuoteAsset string   `json:"minOrderInQuoteAsset"`
	MinOrderInBaseAsset  string   `json:"minOrderInBaseAsset"`
	OrderTypes           []string `json:"orderTypes"`
}

type AssetsResponse struct {
	Action   string   `json:"action"`
	Response []Assets `json:"response"`
}

type Assets struct {
	Symbol               string   `json:"symbol"`
	Name                 string   `json:"name"`
	Decimals             int      `json:"decimals"`
	DepositFee           string   `json:"depositFee"`
	DepositConfirmations int      `json:"depositConfirmations"`
	DepositStatus        string   `json:"depositStatus"`
	WithdrawalFee        string   `json:"withdrawalFee"`
	WithdrawalMinAmount  string   `json:"withdrawalMinAmount"`
	WithdrawalStatus     string   `json:"withdrawalStatus"`
	Networks             []string `json:"networks"`
	Message              string   `json:"message"`
}

type BookResponse struct {
	Action   string `json:"action"`
	Response Book   `json:"response"`
}

type Book struct {
	Market string     `json:"market"`
	Nonce  int        `json:"nonce"`
	Bids   [][]string `json:"bids"`
	Asks   [][]string `json:"asks"`
}

type PublicTradesResponse struct {
	Action   string         `json:"action"`
	Response []PublicTrades `json:"response"`
}

type PublicTrades struct {
	Timestamp int64  `json:"timestamp"`
	Id        string `json:"id"`
	Amount    string `json:"amount"`
	Price     string `json:"price"`
	Side      string `json:"side"`
}

type CandlesResponse struct {
	Action   string        `json:"action"`
	Response []interface{} `json:"response"`
}

type Candles struct {
	Candles []Candle `json:"candles"`
}

type Candle struct {
	Timestamp int
	Open      string
	High      string
	Low       string
	Close     string
	Volume    string
}

type Ticker24hResponse struct {
	Action   string      `json:"action"`
	Response []Ticker24h `json:"response"`
}

type Ticker24h struct {
	Market      string `json:"market"`
	Open        string `json:"open"`
	High        string `json:"high"`
	Low         string `json:"low"`
	Last        string `json:"last"`
	Volume      string `json:"volume"`
	VolumeQuote string `json:"volumeQuote"`
	Bid         string `json:"bid"`
	Ask         string `json:"ask"`
	Timestamp   int    `json:"timestamp"`
	BidSize     string `json:"bidSize"`
	AskSize     string `json:"askSize"`
}

type TickerPriceResponse struct {
	Action   string        `json:"action"`
	Response []TickerPrice `json:"response"`
}

type TickerPrice struct {
	Market string `json:"market"`
	Price  string `json:"price"`
}

type TickerBookResponse struct {
	Action   string       `json:"action"`
	Response []TickerBook `json:"response"`
}

type TickerBook struct {
	Market string `json:"market"`
	Bid    string `json:"bid"`
	Ask    string `json:"ask"`
	BidSize string `json:"bidSize"`
	AskSize string `json:"askSize"`
}

type PlaceOrderResponse struct {
	Action   string `json:"action"`
	Response Order  `json:"response"`
}

type Order struct {
	OrderId                 string `json:"orderId"`
	Market                  string `json:"market"`
	Created                 int    `json:"created"`
	Updated                 int    `json:"updated"`
	Status                  string `json:"status"`
	Side                    string `json:"side"`
	OrderType               string `json:"orderType"`
	Amount                  string `json:"amount"`
	AmountRemaining         string `json:"amountRemaining"`
	Price                   string `json:"price"`
	AmountQuote             string `json:"amountQuote"`
	AmountQuoteRemaining    string `json:"amountQuoteRemaining"`
	OnHold                  string `json:"onHold"`
	OnHoldCurrency          string `json:"onHoldCurrency"`
	FilledAmount            string `json:"filledAmount"`
	FilledAmountQuote       string `json:"filledAmountQuote"`
	FeePaid                 string `json:"feePaid"`
	FeeCurrency             string `json:"feeCurrency"`
	Fills                   []Fill `json:"fills"`
	SelfTradePrevention     string `json:"selfTradePrevention"`
	Visible                 bool   `json:"visible"`
	DisableMarketProtection bool   `json:"disableMarketProtection"`
	TimeInForce             string `json:"timeInForce"`
	PostOnly                bool   `json:"postOnly"`
	TriggerAmount           string `json:"triggerAmount"`
	TriggerPrice            string `json:"triggerPrice"`
	TriggerType             string `json:"triggerType"`
	TriggerReference        string `json:"triggerReference"`
}

type Fill struct {
	Id          string `json:"id"`
	Timestamp   int    `json:"timestamp"`
	Amount      string `json:"amount"`
	Price       string `json:"price"`
	Taker       bool   `json:"taker"`
	Fee         string `json:"fee"`
	FeeCurrency string `json:"feeCurrency"`
	Settled     bool   `json:"settled"`
}

type GetOrderResponse struct {
	Action   string `json:"action"`
	Response Order  `json:"response"`
}

type UpdateOrderResponse struct {
	Action   string `json:"action"`
	Response Order  `json:"response"`
}

type CancelOrderResponse struct {
	Action   string      `json:"action"`
	Response CancelOrder `json:"response"`
}

type CancelOrder struct {
	OrderId string `json:"orderId"`
}

type GetOrdersResponse struct {
	Action   string  `json:"action"`
	Response []Order `json:"response"`
}

type CancelOrdersResponse struct {
	Action   string        `json:"action"`
	Response []CancelOrder `json:"response"`
}

type OrdersOpenResponse struct {
	Action   string  `json:"action"`
	Response []Order `json:"response"`
}

type TradesResponse struct {
	Action   string   `json:"action"`
	Response []Trades `json:"response"`
}

type Trades struct {
	Id          string `json:"id"`
	Timestamp   int    `json:"timestamp"`
	Market      string `json:"market"`
	Amount      string `json:"amount"`
	Side        string `json:"side"`
	Price       string `json:"price"`
	Taker       bool   `json:"taker"`
	Fee         string `json:"fee"`
	FeeCurrency string `json:"feeCurrency"`
	Settled     bool   `json:"settled"`
}

type AccountResponse struct {
	Action   string  `json:"action"`
	Response Account `json:"response"`
}

type Account struct {
	Fees  FeeObject `json:"fees"`
}

type FeeObject struct {
	Taker  string `json:"taker"`
	Maker  string `json:"maker"`
	Volume string `json:"volume"`
}

type BalanceResponse struct {
	Action   string    `json:"action"`
	Response []Balance `json:"response"`
}

type Balance struct {
	Symbol    string `json:"symbol"`
	Available string `json:"available"`
	InOrder   string `json:"inOrder"`
}

type DepositAssetsResponse struct {
	Action   string        `json:"action"`
	Response DepositAssets `json:"response"`
}

type DepositAssets struct {
	Address     string `json:"address"`
	Iban        string `json:"iban"`
	Bic         string `json:"bic"`
	Description string `json:"description"`
	PaymentId   string `json:"paymentId"`
}

type WithdrawAssetsResponse struct {
	Action   string         `json:"action"`
	Response WithdrawAssets `json:"response"`
}

type WithdrawAssets struct {
	Symbol  string `json:"symbol"`
	Amount  string `json:"amount"`
	Success bool   `json:"success"`
}

type HistoryResponse struct {
	Action   string    `json:"action"`
	Response []History `json:"response"`
}

type History struct {
	Symbol    string `json:"symbol"`
	Amount    string `json:"amount"`
	Address   string `json:"address"`
	PaymentId string `json:"paymentId"`
	Fee       string `json:"fee"`
	TxId      string `json:"txId"`
	Timestamp int    `json:"timestamp"`
	Status    string `json:"status"`
}

type SubscriptionTickerResponse struct {
	Action   string             `json:"action"`
	Response SubscriptionTicker `json:"response"`
}

type SubscriptionTicker struct {
	Event       string `json:"event"`
	Market      string `json:"market"`
	BestBid     string `json:"bestBid"`
	BestBidSize string `json:"bestBidSize"`
	BestAsk     string `json:"bestAsk"`
	BestAskSize string `json:"bestAskSize"`
	LastPrice   string `json:"lastPrice"`
}

type SubscriptionTicker24h struct {
	Event string      `json:"event"`
	Data  []Ticker24h `json:"data"`
}

type SubscriptionAccountFill struct {
	Event       string `json:"event"`
	Timestamp   int    `json:"timestamp"`
	Market      string `json:"market"`
	OrderId     string `json:"orderId"`
	FillId      string `json:"fillId"`
	Amount      string `json:"amount"`
	Price       string `json:"price"`
	Taker       bool   `json:"taker"`
	Fee         string `json:"fee"`
	FeeCurrency string `json:"feeCurrency"`
}

type SubscriptionAccountOrder struct {
	Event                string `json:"event"`
	OrderId              string `json:"orderId"`
	Market               string `json:"market"`
	Created              int    `json:"created"`
	Updated              int    `json:"updated"`
	Status               string `json:"status"`
	Side                 string `json:"side"`
	OrderType            string `json:"orderType"`
	Amount               string `json:"amount"`
	AmountRemaining      string `json:"amountRemaining"`
	AmountQuote          string `json:"amountQuote"`
	AmountQuoteRemaining string `json:"amountQuoteRemaining"`
	Price                string `json:"price"`
	OnHold               string `json:"onHold"`
	OnHoldCurrency       string `json:"onHoldCurrency"`
	TimeInForce          string `json:"timeInForce"`
	PostOnly             bool   `json:"postOnly"`
	SelfTradePrevention  string `json:"selfTradePrevention"`
	Visible              bool   `json:"visible"`
	TriggerAmount        string `json:"triggerAmount"`
	TriggerPrice         string `json:"triggerPrice"`
	TriggerType          string `json:"triggerType"`
	TriggerReference     string `json:"triggerReference"`
}

type SubscriptionCandlesResponse struct {
	Action   string             `json:"action"`
	Response SubscriptionTicker `json:"response"`
}

type SubscriptionCandles struct {
	Event    string   `json:"event"`
	Market   string   `json:"market"`
	Interval string   `json:"interval"`
	Candle   []Candle `json:"candle"`
}

type PreCandle struct {
	Event    string        `json:"event"`
	Market   string        `json:"market"`
	Interval string        `json:"interval"`
	Candle   []interface{} `json:"candle"`
}

type SubscriptionTrades struct {
	Event     string `json:"event"`
	Timestamp int64  `json:"timestamp"`
	Market    string `json:"market"`
	Id        string `json:"id"`
	Amount    string `json:"amount"`
	Price     string `json:"price"`
	Side      string `json:"side"`
}

type SubscriptionBookUpdate struct {
	Event  string     `json:"event"`
	Market string     `json:"market"`
	Nonce  int        `json:"nonce"`
	Bids   [][]string `json:"bids"`
	Asks   [][]string `json:"asks"`
}

type SubscriptionTickAccObject struct {
	Action   string   `json:"action"`
	Channels []string `json:"channels"`
}

type SubscriptionTickerObject struct {
	Action   string                         `json:"action"`
	Channels []SubscriptionTickAccSubObject `json:"channels"`
}

type SubscriptionTickAccSubObject struct {
	Name    string   `json:"name"`
	Markets []string `json:"markets"`
}

type SubscriptionTradesBookObject struct {
	Action   string                            `json:"action"`
	Channels []SubscriptionTradesBookSubObject `json:"channels"`
}

type SubscriptionTradesBookSubObject struct {
	Name    string   `json:"name"`
	Markets []string `json:"markets"`
}

type SubscriptionCandlesObject struct {
	Action   string                         `json:"action"`
	Channels []SubscriptionCandlesSubObject `json:"channels"`
}

type SubscriptionCandlesSubObject struct {
	Name     string   `json:"name"`
	Interval []string `json:"interval"`
	Markets  []string `json:"markets"`
}

type LocalBook struct {
	Book map[string]Book `json:"book"`
}

type MyError struct {
	Err         error
	CustomError CustomError
}

func (e MyError) Error() string {
	if e.Err != nil {
		errorString := e.Err.Error()
		return errorString
	} else {
		return fmt.Sprintf("Error returned by API: errorCode:%d, Message: %s", e.CustomError.Code, e.CustomError.Message)
	}
}

type CustomError struct {
	Code    int    `json:"errorCode"`
	Message string `json:"error"`
	Action  string `json:"action"`
}

type Bitvavo struct {
	ApiKey, ApiSecret string
	RestUrl           string
	WsUrl             string
	AccessWindow      int
	WS                Websocket
	reconnectTimer    int
	Debugging         bool
}

func (bitvavo Bitvavo) NewWebsocket() (*Websocket, chan MyError) {
	ws := Websocket{
		sendBuf: make(chan []byte, 10),
		Debugging : bitvavo.Debugging,
		ApiKey : bitvavo.ApiKey,
		ApiSecret : bitvavo.ApiSecret,
		WsUrl : bitvavo.WsUrl,
		AccessWindow: bitvavo.AccessWindow,
		reconnectOnError : true,
		authenticated : false,
		authenticationFailed : false,
		keepLocalBook : false,
	}
	errChannel := make(chan MyError)
	ws.errChannel = errChannel
	ws.ctx, ws.ctxCancel = context.WithCancel(context.Background())

	go ws.listen()
	go ws.listenWrite()
	go ws.ping()

	return &ws, errChannel
}

func (bitvavo Bitvavo) sendPublic(endpoint string) []byte {
	client := &http.Client{}
	req, err := http.NewRequest("GET", endpoint, bytes.NewBuffer(nil))
	if err != nil {
		errorToConsole("We caught error " + err.Error())
	}
	// Removed, do not share if not necessary.
	// A bit more secure to only add this to private messages
	//if bitvavo.ApiKey != "" {
	//	millis := time.Now().UnixNano() / 1000000
	//	timeString := strconv.FormatInt(millis, 10)
	//	sig := bitvavo.createSignature(timeString, "GET", strings.Replace(endpoint, bitvavo.RestUrl, "", 1), map[string]string{}, bitvavo.ApiSecret)
	//	req.Header.Set("Bitvavo-Access-Key", bitvavo.ApiKey)
	//	req.Header.Set("Bitvavo-Access-Signature", sig)
	//	req.Header.Set("Bitvavo-Access-Timestamp", timeString)
	//	req.Header.Set("Bitvavo-Access-Window", strconv.Itoa(bitvavo.AccessWindow))
	//}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		errorToConsole("Caught error " + err.Error())
		return []byte("caught error")
	} else {
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			errorToConsole("Caught error " + err.Error())
			return []byte("caught error")
		}
		updateRateLimit(resp.Header)
		return body
	}
}

func (bitvavo Bitvavo) sendPrivate(endpoint string, postfix string, body map[string]string, method string) []byte {
	millis := time.Now().UnixNano() / 1000000
	timeString := strconv.FormatInt(millis, 10)
	sig := createSignature(timeString, method, endpoint + postfix, body, bitvavo.ApiSecret)
	url := bitvavo.RestUrl + endpoint + postfix
	client := &http.Client{}
	byteBody := []byte{}
	if len(body) != 0 {
		bodyString, err := json.Marshal(body)
		if err != nil {
			errorToConsole("We caught error " + err.Error())
		}
		byteBody = []byte(bodyString)
	} else {
		byteBody = nil
	}
	req, err := http.NewRequest(method, url, bytes.NewBuffer(byteBody))
	req.Header.Set("Bitvavo-Access-Key", bitvavo.ApiKey)
	req.Header.Set("Bitvavo-Access-Signature", sig)
	req.Header.Set("Bitvavo-Access-Timestamp", timeString)
	req.Header.Set("Bitvavo-Access-Window", strconv.Itoa(bitvavo.AccessWindow))
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		errorToConsole("We caught an error " + err.Error())
	}
	defer resp.Body.Close()
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		errorToConsole("Caught error " + err.Error())
		return nil
	}
	updateRateLimit(resp.Header)
	return respBody
}

func checkLimit() {
	now := int(time.Nanosecond * time.Duration(time.Now().UnixNano()) / time.Millisecond)
	rateLimitResetMu.Lock()
	defer rateLimitResetMu.Unlock()
	if rateLimitReset <= now {
		rateLimitRemainingMu.Lock()
		defer rateLimitRemainingMu.Unlock()
		rateLimitRemaining = 1000
	}
}

func updateRateLimit(response http.Header) {
	for key, value := range response {
		if key == "Bitvavo-Ratelimit-Remaining" {
			rateLimitRemainingMu.Lock()
			defer rateLimitRemainingMu.Unlock()

			rateLimitRemaining, _ = strconv.Atoi(value[0])
		}
		if key == "Bitvavo-Ratelimit-ResetAt" {
			rateLimitResetMu.Lock()
			defer rateLimitResetMu.Unlock()

			rateLimitReset, _ = strconv.Atoi(value[0])
			now := int(time.Nanosecond * time.Duration(time.Now().UnixNano()) / time.Millisecond)
			var timeToWait = rateLimitReset - now
			time.AfterFunc(time.Duration(timeToWait)*time.Millisecond, checkLimit)
		}
	}
}

func (bitvavo Bitvavo) GetRemainingLimit() int {
	rateLimitRemainingMu.Lock()
	defer rateLimitRemainingMu.Unlock()
	return rateLimitRemaining
}

func (bitvavo Bitvavo) createPostfix(options map[string]string) string {
	result := []string{}
	for k := range options {
		result = append(result, k + "=" + options[k])
	}
	params := strings.Join(result, "&")
	if len(params) != 0 {
		params = "?" + params
	}
	return params
}

func handleAPIError(jsonResponse []byte) error {
	var e CustomError
	err := json.Unmarshal(jsonResponse, &e)
	if err != nil {
		errorToConsole("error casting")
		return MyError{Err: err}
	}
	if e.Code == 105 {
		rateLimitResetMu.Lock()
		defer rateLimitResetMu.Unlock()
		rateLimitRemainingMu.Lock()
		defer rateLimitRemainingMu.Unlock()

		rateLimitRemaining = 0
		rateLimitReset, _ = strconv.Atoi(strings.Split(strings.Split(e.Message, " at ")[1], ".")[0])
		now := int(time.Nanosecond * time.Duration(time.Now().UnixNano()) / time.Millisecond)
		var timeToWait = rateLimitReset - now
		time.AfterFunc(time.Duration(timeToWait)*time.Millisecond, checkLimit)
	}
	return MyError{CustomError: e}
}

func (bitvavo Bitvavo) Time() (Time, error) {
	jsonResponse := bitvavo.sendPublic(bitvavo.RestUrl + "/time")
	var t Time

	err := json.Unmarshal(jsonResponse, &t)
	if err != nil {
		return Time{}, MyError{Err: err}
	}
	if t.Time == 0.0 {
		return Time{}, handleAPIError(jsonResponse)
	}
	return t, nil
}

// options: market
func (bitvavo Bitvavo) Markets(options map[string]string) ([]Markets, error) {
	postfix := bitvavo.createPostfix(options)
	jsonResponse := bitvavo.sendPublic(bitvavo.RestUrl + "/markets" + postfix)
	t := make([]Markets, 0)
	err := json.Unmarshal(jsonResponse, &t)
	if err != nil {
		return []Markets{Markets{}}, handleAPIError(jsonResponse)
	}
	return t, nil
}

// options: symbol
func (bitvavo Bitvavo) Assets(options map[string]string) ([]Assets, error) {
	postfix := bitvavo.createPostfix(options)
	jsonResponse := bitvavo.sendPublic(bitvavo.RestUrl + "/assets" + postfix)
	t := make([]Assets, 0)
	err := json.Unmarshal(jsonResponse, &t)
	if err != nil {
		return []Assets{Assets{}}, handleAPIError(jsonResponse)
	}
	return t, nil
}

// options: depth
func (bitvavo Bitvavo) Book(symbol string, options map[string]string) (Book, error) {
	postfix := bitvavo.createPostfix(options)
	jsonResponse := bitvavo.sendPublic(bitvavo.RestUrl + "/" + symbol + "/book" + postfix)
	var t Book
	err := json.Unmarshal(jsonResponse, &t)
	if err != nil {
		return Book{}, MyError{Err: err}
	}
	if t.Market == "" {
		return Book{}, handleAPIError(jsonResponse)
	}
	return t, nil
}

// options: limit, start, end, tradeIdFrom, tradeIdTo
func (bitvavo Bitvavo) PublicTrades(symbol string, options map[string]string) ([]PublicTrades, error) {
	postfix := bitvavo.createPostfix(options)
	jsonResponse := bitvavo.sendPublic(bitvavo.RestUrl + "/" + symbol + "/trades" + postfix)
	t := make([]PublicTrades, 0)
	err := json.Unmarshal(jsonResponse, &t)
	if err != nil {
		return []PublicTrades{PublicTrades{}}, handleAPIError(jsonResponse)
	}
	return t, nil
}

// options: limit, start, end
func (bitvavo Bitvavo) Candles(symbol string, interval string, options map[string]string) ([]Candle, error) {
	options["interval"] = interval
	postfix := bitvavo.createPostfix(options)
	jsonResponse := bitvavo.sendPublic(bitvavo.RestUrl + "/" + symbol + "/candles" + postfix)
	var t []interface{}
	err := json.Unmarshal(jsonResponse, &t)
	if err != nil {
		return []Candle{Candle{}}, MyError{Err: err}
	}
	var candles []Candle
	for i := 0; i < len(t); i++ {
		entry := reflect.ValueOf(t[i])
		candles = append(candles, Candle{Timestamp: int(entry.Index(0).Interface().(float64)), Open: entry.Index(1).Interface().(string), High: entry.Index(2).Interface().(string), Low: entry.Index(3).Interface().(string), Close: entry.Index(4).Interface().(string), Volume: entry.Index(5).Interface().(string)})
	}
	return candles, nil
}

// options: market
func (bitvavo Bitvavo) TickerPrice(options map[string]string) ([]TickerPrice, error) {
	postfix := bitvavo.createPostfix(options)
	jsonResponse := bitvavo.sendPublic(bitvavo.RestUrl + "/ticker/price" + postfix)
	t := make([]TickerPrice, 0)
	err := json.Unmarshal(jsonResponse, &t)
	if err != nil {
		var t TickerPrice
		err = json.Unmarshal(jsonResponse, &t)
		if err != nil {
			return []TickerPrice{TickerPrice{}}, MyError{Err: err}
		}
		if t.Market == "" {
			return []TickerPrice{TickerPrice{}}, handleAPIError(jsonResponse)
		}
		return []TickerPrice{t}, nil
	}
	return t, nil
}

// options: market
func (bitvavo Bitvavo) TickerBook(options map[string]string) ([]TickerBook, error) {
	postfix := bitvavo.createPostfix(options)
	jsonResponse := bitvavo.sendPublic(bitvavo.RestUrl + "/ticker/book" + postfix)
	t := make([]TickerBook, 0)
	err := json.Unmarshal(jsonResponse, &t)
	if err != nil {
		var t TickerBook
		err = json.Unmarshal(jsonResponse, &t)
		if err != nil {
			return []TickerBook{TickerBook{}}, MyError{Err: err}
		}
		if t.Market == "" {
			return []TickerBook{TickerBook{}}, handleAPIError(jsonResponse)
		}
		return []TickerBook{t}, nil
	}
	return t, nil
}

// options: market
func (bitvavo Bitvavo) Ticker24h(options map[string]string) ([]Ticker24h, error) {
	postfix := bitvavo.createPostfix(options)
	jsonResponse := bitvavo.sendPublic(bitvavo.RestUrl + "/ticker/24h" + postfix)
	t := make([]Ticker24h, 0)
	err := json.Unmarshal(jsonResponse, &t)
	if err != nil {
		var t Ticker24h
		err = json.Unmarshal(jsonResponse, &t)
		if err != nil {
			return []Ticker24h{Ticker24h{}}, MyError{Err: err}
		}
		if t.Market == "" {
			return []Ticker24h{Ticker24h{}}, handleAPIError(jsonResponse)
		}
		return []Ticker24h{t}, nil
	}
	return t, nil
}

// optional body parameters: limit:(amount, price, postOnly), market:(amount, amountQuote, disableMarketProtection)
//                           stopLoss/takeProfit:(amount, amountQuote, disableMarketProtection, triggerType, triggerReference, triggerAmount)
//                           stopLossLimit/takeProfitLimit:(amount, price, postOnly, triggerType, triggerReference, triggerAmount)
//                           all orderTypes: timeInForce, selfTradePrevention, responseRequired
func (bitvavo Bitvavo) PlaceOrder(market string, side string, orderType string, body map[string]string) (Order, error) {
	body["market"] = market
	body["side"] = side
	body["orderType"] = orderType
	jsonResponse := bitvavo.sendPrivate("/order", "", body, "POST")
	var t Order
	err := json.Unmarshal(jsonResponse, &t)
	if err != nil {
		return Order{}, MyError{Err: err}
	}
	if t.OrderId == "" {
		return Order{}, handleAPIError(jsonResponse)
	}
	return t, nil
}

func (bitvavo Bitvavo) GetOrder(market string, orderId string) (Order, error) {
	options := map[string]string{"market": market, "orderId": orderId}
	postfix := bitvavo.createPostfix(options)
	jsonResponse := bitvavo.sendPrivate("/order", postfix, map[string]string{}, "GET")
	var t Order
	err := json.Unmarshal(jsonResponse, &t)
	if err != nil {
		return Order{}, MyError{Err: err}
	}
	if t.OrderId == "" {
		return Order{}, handleAPIError(jsonResponse)
	}
	return t, nil
}

// Optional body parameters: limit:(amount, amountRemaining, price, timeInForce, selfTradePrevention, postOnly)
//               untriggered stopLoss/takeProfit:(amount, amountQuote, disableMarketProtection, triggerType, triggerReference, triggerAmount)
//                           stopLossLimit/takeProfitLimit: (amount, price, postOnly, triggerType, triggerReference, triggerAmount)
func (bitvavo Bitvavo) UpdateOrder(market string, orderId string, body map[string]string) (Order, error) {
	body["market"] = market
	body["orderId"] = orderId
	jsonResponse := bitvavo.sendPrivate("/order", "", body, "PUT")
	var t Order
	err := json.Unmarshal(jsonResponse, &t)
	if err != nil {
		return Order{}, MyError{Err: err}
	}
	if t.OrderId == "" {
		return Order{}, handleAPIError(jsonResponse)
	}
	return t, nil
}

func (bitvavo Bitvavo) CancelOrder(market string, orderId string) (CancelOrder, error) {
	options := map[string]string{"market": market, "orderId": orderId}
	postfix := bitvavo.createPostfix(options)
	jsonResponse := bitvavo.sendPrivate("/order", postfix, map[string]string{}, "DELETE")
	var t CancelOrder
	err := json.Unmarshal(jsonResponse, &t)
	if err != nil {
		return CancelOrder{}, MyError{Err: err}
	}
	if t.OrderId == "" {
		return CancelOrder{}, handleAPIError(jsonResponse)
	}
	return t, nil
}

// options: limit, start, end, orderIdFrom, orderIdTo
func (bitvavo Bitvavo) GetOrders(market string, options map[string]string) ([]Order, error) {
	options["market"] = market
	postfix := bitvavo.createPostfix(options)
	jsonResponse := bitvavo.sendPrivate("/orders", postfix, map[string]string{}, "GET")
	t := make([]Order, 0)
	err := json.Unmarshal(jsonResponse, &t)
	if err != nil {
		return []Order{Order{}}, handleAPIError(jsonResponse)
	}
	return t, nil
}

// options: market
func (bitvavo Bitvavo) CancelOrders(options map[string]string) ([]CancelOrder, error) {
	postfix := bitvavo.createPostfix(options)
	jsonResponse := bitvavo.sendPrivate("/orders", postfix, map[string]string{}, "DELETE")
	t := make([]CancelOrder, 0)
	err := json.Unmarshal(jsonResponse, &t)
	if err != nil {
		return []CancelOrder{CancelOrder{}}, handleAPIError(jsonResponse)
	}
	return t, nil
}

// options: market
func (bitvavo Bitvavo) OrdersOpen(options map[string]string) ([]Order, error) {
	postfix := bitvavo.createPostfix(options)
	jsonResponse := bitvavo.sendPrivate("/ordersOpen", postfix, map[string]string{}, "GET")
	t := make([]Order, 0)
	err := json.Unmarshal(jsonResponse, &t)
	if err != nil {
		return []Order{Order{}}, handleAPIError(jsonResponse)
	}
	return t, nil
}

// options: limit, start, end, tradeIdFrom, tradeIdTo
func (bitvavo Bitvavo) Trades(market string, options map[string]string) ([]Trades, error) {
	options["market"] = market
	postfix := bitvavo.createPostfix(options)
	jsonResponse := bitvavo.sendPrivate("/trades", postfix, map[string]string{}, "GET")
	t := make([]Trades, 0)
	err := json.Unmarshal(jsonResponse, &t)
	if err != nil {
		return []Trades{Trades{}}, handleAPIError(jsonResponse)
	}
	return t, nil
}

func (bitvavo Bitvavo) Account() (Account, error) {
	jsonResponse := bitvavo.sendPrivate("/account", "", map[string]string{}, "GET")
	var t Account
	err := json.Unmarshal(jsonResponse, &t)
	if err != nil {
		return Account{}, MyError{Err: err}
	}
	return t, nil
}

// options: symbol
func (bitvavo Bitvavo) Balance(options map[string]string) ([]Balance, error) {
	postfix := bitvavo.createPostfix(options)
	jsonResponse := bitvavo.sendPrivate("/balance", postfix, map[string]string{}, "GET")
	t := make([]Balance, 0)
	err := json.Unmarshal(jsonResponse, &t)
	if err != nil {
		return []Balance{Balance{}}, handleAPIError(jsonResponse)
	}
	return t, nil
}

func (bitvavo Bitvavo) DepositAssets(symbol string) (DepositAssets, error) {
	options := map[string]string{"symbol": symbol}
	postfix := bitvavo.createPostfix(options)
	jsonResponse := bitvavo.sendPrivate("/deposit", postfix, map[string]string{}, "GET")
	var t DepositAssets
	err := json.Unmarshal(jsonResponse, &t)
	if err != nil {
		return DepositAssets{}, MyError{Err: err}
	}
	if t.Address == "" {
		return DepositAssets{}, handleAPIError(jsonResponse)
	}
	return t, nil
}

// optional body parameters: paymentId, internal, addWithdrawalFee
func (bitvavo Bitvavo) WithdrawAssets(symbol string, amount string, address string, body map[string]string) (WithdrawAssets, error) {
	body["symbol"] = symbol
	body["amount"] = amount
	body["address"] = address
	jsonResponse := bitvavo.sendPrivate("/withdrawal", "", body, "POST")
	var t WithdrawAssets
	err := json.Unmarshal(jsonResponse, &t)
	if err != nil {
		return WithdrawAssets{}, MyError{Err: err}
	}
	if t.Symbol == "" {
		return WithdrawAssets{}, handleAPIError(jsonResponse)
	}
	return t, nil
}

// options: symbol, limit, start, end
func (bitvavo Bitvavo) DepositHistory(options map[string]string) ([]History, error) {
	postfix := bitvavo.createPostfix(options)
	jsonResponse := bitvavo.sendPrivate("/depositHistory", postfix, map[string]string{}, "GET")
	t := make([]History, 0)
	err := json.Unmarshal(jsonResponse, &t)
	if err != nil {
		return []History{History{}}, handleAPIError(jsonResponse)
	}
	return t, nil
}

// options: symbol, limit, start, end
func (bitvavo Bitvavo) WithdrawalHistory(options map[string]string) ([]History, error) {
	postfix := bitvavo.createPostfix(options)
	jsonResponse := bitvavo.sendPrivate("/withdrawalHistory", postfix, map[string]string{}, "GET")
	t := make([]History, 0)
	err := json.Unmarshal(jsonResponse, &t)
	if err != nil {
		return []History{History{}}, handleAPIError(jsonResponse)
	}
	return t, nil
}

func handleError(err error) bool {
	if err != nil {
		errorToConsole(err.Error())
		return true
	}
	return false
}

func sortAndInsert(update [][]string, book [][]string, asksCompare bool) [][]string {
	for i := 0; i < len(update); i++ {
		entrySet := false
		updateEntry := update[i]
		for j := 0; j < len(book); j++ {
			bookItem := book[j]
			updatePrice, _ := strconv.ParseFloat(updateEntry[0], 64)
			bookPrice, _ := strconv.ParseFloat(bookItem[0], 64)
			if asksCompare {
				if updatePrice < bookPrice {
					book = append(book, make([]string, 2))
					copy(book[j+1:], book[j:])
					book[j] = updateEntry
					entrySet = true
					break
				}
			} else {
				if updatePrice > bookPrice {
					book = append(book, make([]string, 2))
					copy(book[j+1:], book[j:])
					book[j] = updateEntry
					entrySet = true
					break
				}
			}
			if updatePrice == bookPrice {
				updateAmount, _ := strconv.ParseFloat(updateEntry[1], 64)
				if updateAmount > 0.0 {
					book[j] = updateEntry
					entrySet = true
					break
				} else {
					book = append(book[:j], book[j+1:]...)
					entrySet = true
					break
				}
			}
		}
		if entrySet == false {
			book = append(book, updateEntry)
		}
	}
	return book
}

func addToBook(t SubscriptionBookUpdate, ws *Websocket) {
	ws.BookLock.Lock()
	defer ws.BookLock.Unlock()
	var book = ws.localBook.Book[t.Market]
	book.Bids = sortAndInsert(t.Bids, ws.localBook.Book[t.Market].Bids, false)
	book.Asks = sortAndInsert(t.Asks, ws.localBook.Book[t.Market].Asks, true)
	if book.Nonce != (t.Nonce - 1) {
		ws.SubscriptionBook(t.Market, ws.subscriptionBookOptionsFirstMap[t.Market])
		return
	}
	book.Nonce = t.Nonce
	ws.localBook.Book[t.Market] = book
	ws.subscriptionBookChannelMap[t.Market] <- ws.localBook.Book[t.Market]
}

func (bitvavo Bitvavo) DebugToConsole(message string) {
	if bitvavo.Debugging {
		fmt.Println(time.Now().Format("15:04:05") + " DEBUG: " + message)
	}
}

func (ws *Websocket) DebugToConsole(message string) {
	if ws.Debugging {
		fmt.Println(time.Now().Format("15:04:05") + " DEBUG: " + message)
	}
}

func errorToConsole(message string) {
	fmt.Println(time.Now().Format("15:04:05") + " ERROR: " + message)
}


