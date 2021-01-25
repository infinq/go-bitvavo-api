package bitvavo

//
//Credits: https://webdevelop.pro/blog/guide-creating-websocket-client-golang-using-mutex-and-channel
//

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gorilla/websocket"
	"net/url"
	"reflect"
	"sync"
	"time"
)

type Websocket struct {
	ApiKey    string
	ApiSecret string

	WsUrl     string
	Debugging bool
	BookLock  sync.Mutex
	sendLock  sync.Mutex

	conn      *websocket.Conn
	mu        sync.RWMutex
	sendBuf   chan []byte
	ctx       context.Context
	ctxCancel context.CancelFunc

	localBook                LocalBook
	reconnectOnError         bool
	authenticated            bool
	authenticationFailed     bool
	keepLocalBook            bool
	errChannel               chan MyError
	timeChannel              chan Time
	marketsChannel           chan []Markets
	assetsChannel            chan []Assets
	bookChannel              chan Book
	publicTradesChannel      chan []PublicTrades
	candlesChannel           chan []Candle
	ticker24hChannel         chan []Ticker24h
	tickerPriceChannel       chan []TickerPrice
	tickerBookChannel        chan []TickerBook
	placeOrderChannel        chan Order
	getOrderChannel          chan Order
	updateOrderChannel       chan Order
	cancelOrderChannel       chan CancelOrder
	getOrdersChannel         chan []Order
	cancelOrdersChannel      chan []CancelOrder
	ordersOpenChannel        chan []Order
	tradesChannel            chan []Trades
	accountChannel           chan Account
	balanceChannel           chan []Balance
	depositAssetsChannel     chan DepositAssets
	withdrawAssetsChannel    chan WithdrawAssets
	depositHistoryChannel    chan []History
	withdrawalHistoryChannel chan []History

	subscriptionTickerChannelMap map[string]chan SubscriptionTicker
	subscriptionTickerOptionsMap map[string]SubscriptionTickerObject

	subscriptionTicker24hChannelMap map[string]chan Ticker24h
	subscriptionTicker24hOptionsMap map[string]SubscriptionTickerObject

	subscriptionAccountFillChannelMap  map[string]chan SubscriptionAccountFill
	subscriptionAccountOrderChannelMap map[string]chan SubscriptionAccountOrder
	subscriptionAccountOptionsMap      map[string]SubscriptionTickerObject

	subscriptionCandlesOptionsMap map[string]map[string]SubscriptionCandlesObject
	subscriptionCandlesChannelMap map[string]map[string]chan SubscriptionCandles

	subscriptionTradesChannelMap map[string]chan SubscriptionTrades
	subscriptionTradesOptionsMap map[string]SubscriptionTradesBookObject

	subscriptionBookUpdateChannelMap map[string]chan SubscriptionBookUpdate
	subscriptionBookUpdateOptionsMap map[string]SubscriptionTradesBookObject

	subscriptionBookChannelMap       map[string]chan Book
	subscriptionBookOptionsFirstMap  map[string]map[string]string
	subscriptionBookOptionsSecondMap map[string]SubscriptionTradesBookObject

}

func (bitvavo Bitvavo) NewWebsocket() (*Websocket, chan MyError) {
	ws := Websocket{
		sendBuf: make(chan []byte, 10),
		Debugging : bitvavo.Debugging,
		ApiKey : bitvavo.ApiKey,
		ApiSecret : bitvavo.ApiSecret,
		WsUrl : bitvavo.WsUrl,
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

	//go bitvavo.handleMessage(&ws)
	return &ws, errChannel
}



func (ws *Websocket) listen() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ws.ctx.Done():
			return
		case <-ticker.C:
			for {
				wsConn := ws.Connect()
				if wsConn == nil {
					return
				}
				_, message, err := wsConn.ReadMessage()

				if err != nil {
					errorToConsole("Cannot read websocket message")
					fmt.Printf("Close ws")
					ws.closeWs()
					return
				}
				//if handleError(err) {
				//	if ws.reconnectOnError {
				//		err = ws.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
				//		//bitvavo.reconnect(ws)
				//		return
				//
				//	}
				//	//return
				//}
				ws.handleMessage(message)

				//fmt.Printf("websocket msg: %x\n", bytMsg)
			}
		}
	}
}


// Write data to the websocket server
func (ws *Websocket) Write(data []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
	defer cancel()

	for {
		select {
		case ws.sendBuf <- data:
			return nil
		case <-ctx.Done():
			return fmt.Errorf("context canceled")
		}
	}
}
func (ws *Websocket) listenWrite() {
	for data := range ws.sendBuf {
		wsConn := ws.Connect()
		if wsConn == nil {
			err := fmt.Errorf("conn.ws is nil")
			errorToConsole(fmt.Sprintf("No websocket connection: %s", err))
			continue
		}
		ws.mu.Lock()
		if err := wsConn.WriteMessage(
			websocket.TextMessage,
			data,
		); err != nil {
			errorToConsole("WebSocket Write Error")
		}
		ws.mu.Unlock()
		//fmt.Printf("send: %s", data)
	}
}
// Close will send close message and shutdown websocket connection
func (ws *Websocket) Stop() {
	ws.ctxCancel()
	ws.closeWs()
}

// Close will send close message and shutdown websocket connection
func (ws *Websocket) closeWs() {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if ws.conn != nil {
		fmt.Printf("Close websocket\r\n")
		ws.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		ws.conn.Close()
		ws.conn = nil
	}
	//ws.mu.Unlock()
}

func (ws *Websocket) ping() {
	fmt.Printf("Starting ping/pong\r\n")
	pingPeriod := 1 * time.Second
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			wsConn := ws.Connect()
			if wsConn == nil {
				continue
			}
			fmt.Printf(">")
			ws.mu.Lock()
			if err := ws.conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(pingPeriod/2)); err != nil {
				fmt.Printf("/")
				ws.closeWs()
			}
			ws.mu.Unlock()
		case <-ws.ctx.Done():
			return
		}
	}
}

func (ws *Websocket) Connect() *websocket.Conn {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if ws.conn != nil {
		return ws.conn
	}
	// if no connection retry every second
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for ; ; <-ticker.C {
		select {
		case <-ws.ctx.Done():
			return nil
		default:
			fmt.Print("Create connection")
			uri, _ := url.Parse(ws.WsUrl)
			wsConn, _, err := websocket.DefaultDialer.Dial(uri.String(), nil)
			if err != nil {
				fmt.Printf("X")
				continue
			}
			wsConn.SetPongHandler(func(msg string) error {
				fmt.Printf("<")
				return nil
			})
			//TODO: Authenticate is necessary
			if ws.ApiKey != "" {
				errorToConsole("TODO: authenticate here")
				//now := time.Now()
				//nanos := now.UnixNano()
				//millis := nanos / 1000000
				//timestamp := strconv.FormatInt(millis, 10)
				////
				//authenticate := map[string]string{"action": "authenticate", "key": ws.ApiKey, "signature": bitvavo.createSignature(timestamp, "GET", "/websocket", map[string]string{}, ws.ApiSecret), "timestamp": timestamp, "window": strconv.Itoa(bitvavo.AccessWindow)}
				//myMessage, _ := json.Marshal(authenticate)
				//ws.DebugToConsole("SENDING: " + string(myMessage))
				//ws.conn.WriteMessage(websocket.TextMessage, []byte(myMessage))
			}
			//fmt.Printf("connected to websocket to %s", ws.WsUrl)
			ws.conn = wsConn
			return ws.conn
		}
	}
}


func (ws *Websocket) Close() {
	ws.reconnectOnError = false
	ws.mu.Unlock()
	defer ws.mu.Lock()
	ws.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
}

func (ws *Websocket) Time() chan Time {
	ws.timeChannel = make(chan Time, 100)
	myMessage, _ := json.Marshal(map[string]string{"action": "getTime"})
	ws.Write([]byte(myMessage))
	return ws.timeChannel
}

// options: market
func (ws *Websocket) Markets(options map[string]string) chan []Markets {
	ws.marketsChannel = make(chan []Markets, 100)
	options["action"] = "getMarkets"
	myMessage, _ := json.Marshal(options)
	ws.Write([]byte(myMessage))
	return ws.marketsChannel
}

// options: symbol
func (ws *Websocket) Assets(options map[string]string) chan []Assets {
	ws.assetsChannel = make(chan []Assets, 100)
	options["action"] = "getAssets"
	myMessage, _ := json.Marshal(options)
	ws.Write([]byte(myMessage))
	return ws.assetsChannel
}

// options: depth
func (ws *Websocket) Book(market string, options map[string]string) chan Book {
	ws.bookChannel = make(chan Book, 100)
	options["market"] = market
	options["action"] = "getBook"
	myMessage, _ := json.Marshal(options)
	ws.Write([]byte(myMessage))
	return ws.bookChannel
}

// options: limit, start, end, tradeIdFrom, tradeIdTo
func (ws *Websocket) PublicTrades(market string, options map[string]string) chan []PublicTrades {
	ws.publicTradesChannel = make(chan []PublicTrades, 100)
	options["market"] = market
	options["action"] = "getTrades"
	myMessage, _ := json.Marshal(options)
	ws.Write([]byte(myMessage))
	return ws.publicTradesChannel
}

// options: limit, start, end
func (ws *Websocket) Candles(market string, interval string, options map[string]string) chan []Candle {
	ws.candlesChannel = make(chan []Candle, 100)
	options["market"] = market
	options["interval"] = interval
	options["action"] = "getCandles"
	myMessage, _ := json.Marshal(options)
	ws.Write([]byte(myMessage))
	return ws.candlesChannel
}

// options: market
func (ws *Websocket) Ticker24h(options map[string]string) chan []Ticker24h {
	ws.ticker24hChannel = make(chan []Ticker24h, 100)
	options["action"] = "getTicker24h"
	myMessage, _ := json.Marshal(options)
	ws.Write([]byte(myMessage))
	return ws.ticker24hChannel
}

// options: market
func (ws *Websocket) TickerPrice(options map[string]string) chan []TickerPrice {
	ws.tickerPriceChannel = make(chan []TickerPrice, 100)
	options["action"] = "getTickerPrice"
	myMessage, _ := json.Marshal(options)
	ws.Write([]byte(myMessage))
	return ws.tickerPriceChannel
}

// options: market
func (ws *Websocket) TickerBook(options map[string]string) chan []TickerBook {
	ws.tickerBookChannel = make(chan []TickerBook, 100)
	options["action"] = "getTickerBook"
	myMessage, _ := json.Marshal(options)
	ws.Write([]byte(myMessage))
	return ws.tickerBookChannel
}

func (ws *Websocket) sendPrivate(msg []byte) {
	if ws.ApiKey == "" {
		errorToConsole("You did not set the API key, but requested a private function.")
		return
	}
	if ws.authenticated == true {
		ws.DebugToConsole("SENDING: " + string(msg))
		ws.Write(msg)
	} else {
		if ws.authenticationFailed == false {
			ws.DebugToConsole("Waiting 100 milliseconds till authenticated is received")
			time.Sleep(100 * time.Millisecond)
			ws.sendPrivate(msg)
		} else {
			errorToConsole("Authentication is required for sending this message, but authentication failed.")
		}
	}
}

// optional body parameters: limit:(amount, price, postOnly), market:(amount, amountQuote, disableMarketProtection)
//                           stopLoss/takeProfit:(amount, amountQuote, disableMarketProtection, triggerType, triggerReference, triggerAmount)
//                           stopLossLimit/takeProfitLimit:(amount, price, postOnly, triggerType, triggerReference, triggerAmount)
//                           all orderTypes: timeInForce, selfTradePrevention, responseRequired
func (ws *Websocket) PlaceOrder(market string, side string, orderType string, body map[string]string) chan Order {
	body["market"] = market
	body["side"] = side
	body["orderType"] = orderType
	ws.placeOrderChannel = make(chan Order, 100)
	body["action"] = "privateCreateOrder"
	myMessage, _ := json.Marshal(body)
	go ws.sendPrivate(myMessage)
	return ws.placeOrderChannel
}

func (ws *Websocket) GetOrder(market string, orderId string) chan Order {
	ws.getOrderChannel = make(chan Order, 100)
	options := map[string]string{"action": "privateGetOrder", "market": market, "orderId": orderId}
	myMessage, _ := json.Marshal(options)
	go ws.sendPrivate(myMessage)
	return ws.getOrderChannel
}

// Optional body parameters: limit:(amount, amountRemaining, price, timeInForce, selfTradePrevention, postOnly)
//               untriggered stopLoss/takeProfit:(amount, amountQuote, disableMarketProtection, triggerType, triggerReference, triggerAmount)
//                           stopLossLimit/takeProfitLimit: (amount, price, postOnly, triggerType, triggerReference, triggerAmount)
func (ws *Websocket) UpdateOrder(market string, orderId string, body map[string]string) chan Order {
	ws.updateOrderChannel = make(chan Order, 100)
	body["market"] = market
	body["orderId"] = orderId
	body["action"] = "privateUpdateOrder"
	myMessage, _ := json.Marshal(body)
	go ws.sendPrivate(myMessage)
	return ws.updateOrderChannel
}

func (ws *Websocket) CancelOrder(market string, orderId string) chan CancelOrder {
	ws.cancelOrderChannel = make(chan CancelOrder, 100)
	options := map[string]string{"action": "privateCancelOrder", "market": market, "orderId": orderId}
	myMessage, _ := json.Marshal(options)
	go ws.sendPrivate(myMessage)
	return ws.cancelOrderChannel
}

// options: limit, start, end, orderIdFrom, orderIdTo
func (ws *Websocket) GetOrders(market string, options map[string]string) chan []Order {
	ws.getOrdersChannel = make(chan []Order, 100)
	options["action"] = "privateGetOrders"
	options["market"] = market
	myMessage, _ := json.Marshal(options)
	go ws.sendPrivate(myMessage)
	return ws.getOrdersChannel
}

func (ws *Websocket) CancelOrders(options map[string]string) chan []CancelOrder {
	ws.cancelOrdersChannel = make(chan []CancelOrder, 100)
	options["action"] = "privateCancelOrders"
	myMessage, _ := json.Marshal(options)
	go ws.sendPrivate(myMessage)
	return ws.cancelOrdersChannel
}

// options: market
func (ws *Websocket) OrdersOpen(options map[string]string) chan []Order {
	ws.ordersOpenChannel = make(chan []Order, 100)
	options["action"] = "privateGetOrdersOpen"
	myMessage, _ := json.Marshal(options)
	go ws.sendPrivate(myMessage)
	return ws.ordersOpenChannel
}

// options: limit, start, end, tradeIdFrom, tradeIdTo
func (ws *Websocket) Trades(market string, options map[string]string) chan []Trades {
	ws.tradesChannel = make(chan []Trades, 100)
	options["action"] = "privateGetTrades"
	options["market"] = market
	myMessage, _ := json.Marshal(options)
	go ws.sendPrivate(myMessage)
	return ws.tradesChannel
}

func (ws *Websocket) Account() chan Account {
	ws.accountChannel = make(chan Account, 100)
	options := map[string]string{"action": "privateGetAccount"}
	myMessage, _ := json.Marshal(options)
	go ws.sendPrivate(myMessage)
	return ws.accountChannel
}

// options: symbol
func (ws *Websocket) Balance(options map[string]string) chan []Balance {
	ws.balanceChannel = make(chan []Balance, 100)
	options["action"] = "privateGetBalance"
	myMessage, _ := json.Marshal(options)
	go ws.sendPrivate(myMessage)
	return ws.balanceChannel
}

func (ws *Websocket) DepositAssets(symbol string) chan DepositAssets {
	ws.depositAssetsChannel = make(chan DepositAssets, 100)
	options := map[string]string{"action": "privateDepositAssets", "symbol": symbol}
	myMessage, _ := json.Marshal(options)
	go ws.sendPrivate(myMessage)
	return ws.depositAssetsChannel
}

// optional body parameters: paymentId, internal, addWithdrawalFee
func (ws *Websocket) WithdrawAssets(symbol string, amount string, address string, body map[string]string) chan WithdrawAssets {
	ws.withdrawAssetsChannel = make(chan WithdrawAssets, 100)
	body["symbol"] = symbol
	body["amount"] = amount
	body["address"] = address
	body["action"] = "privateWithdrawAssets"
	myMessage, _ := json.Marshal(body)
	go ws.sendPrivate(myMessage)
	return ws.withdrawAssetsChannel
}

// options: symbol, limit, start, end
func (ws *Websocket) DepositHistory(options map[string]string) chan []History {
	ws.depositHistoryChannel = make(chan []History, 100)
	options["action"] = "privateGetDepositHistory"
	myMessage, _ := json.Marshal(options)
	go ws.sendPrivate(myMessage)
	return ws.depositHistoryChannel
}

// options: symbol, limit, start, end
func (ws *Websocket) WithdrawalHistory(options map[string]string) chan []History {
	ws.withdrawalHistoryChannel = make(chan []History, 100)
	options["action"] = "privateGetWithdrawalHistory"
	myMessage, _ := json.Marshal(options)
	go ws.sendPrivate(myMessage)
	return ws.withdrawalHistoryChannel
}

func (ws *Websocket) SubscriptionTicker(market string) chan SubscriptionTicker {
	options := SubscriptionTickerObject{Action: "subscribe", Channels: []SubscriptionTickAccSubObject{SubscriptionTickAccSubObject{Name: "ticker", Markets: []string{market}}}}
	if ws.subscriptionTickerChannelMap == nil {
		ws.subscriptionTickerChannelMap = map[string]chan SubscriptionTicker{}
	}
	if ws.subscriptionTickerOptionsMap == nil {
		ws.subscriptionTickerOptionsMap = map[string]SubscriptionTickerObject{}
	}
	ws.subscriptionTickerChannelMap[market] = make(chan SubscriptionTicker, 100)
	ws.subscriptionTickerOptionsMap[market] = options

	myMessage, _ := json.Marshal(options)
	ws.Write([]byte(myMessage))
	return ws.subscriptionTickerChannelMap[market]
}

func (ws *Websocket) SubscriptionTicker24h(market string) chan Ticker24h {
	options := SubscriptionTickerObject{Action: "subscribe", Channels: []SubscriptionTickAccSubObject{SubscriptionTickAccSubObject{Name: "ticker24h", Markets: []string{market}}}}
	if ws.subscriptionTicker24hChannelMap == nil {
		ws.subscriptionTicker24hChannelMap = map[string]chan Ticker24h{}
	}
	if ws.subscriptionTicker24hOptionsMap == nil {
		ws.subscriptionTicker24hOptionsMap = map[string]SubscriptionTickerObject{}
	}
	ws.subscriptionTicker24hChannelMap[market] = make(chan Ticker24h, 100)
	ws.subscriptionTicker24hOptionsMap[market] = options

	myMessage, _ := json.Marshal(options)
	ws.Write([]byte(myMessage))
	return ws.subscriptionTicker24hChannelMap[market]
}

func (ws *Websocket) SubscriptionAccount(market string) (chan SubscriptionAccountOrder, chan SubscriptionAccountFill) {
	options := SubscriptionTickerObject{Action: "subscribe", Channels: []SubscriptionTickAccSubObject{SubscriptionTickAccSubObject{Name: "account", Markets: []string{market}}}}
	if ws.subscriptionAccountOrderChannelMap == nil {
		ws.subscriptionAccountOrderChannelMap = map[string]chan SubscriptionAccountOrder{}
	}
	if ws.subscriptionAccountFillChannelMap == nil {
		ws.subscriptionAccountFillChannelMap = map[string]chan SubscriptionAccountFill{}
	}
	ws.subscriptionAccountOrderChannelMap[market] = make(chan SubscriptionAccountOrder, 100)
	ws.subscriptionAccountFillChannelMap[market] = make(chan SubscriptionAccountFill, 100)

	if ws.subscriptionAccountOptionsMap == nil {
		ws.subscriptionAccountOptionsMap = map[string]SubscriptionTickerObject{}
	}
	ws.subscriptionAccountOptionsMap[market] = options

	myMessage, _ := json.Marshal(options)

	ws.sendPrivate(myMessage)
	return ws.subscriptionAccountOrderChannelMap[market], ws.subscriptionAccountFillChannelMap[market]
}

func (ws *Websocket) SubscriptionCandles(market string, interval string) chan SubscriptionCandles {
	options := SubscriptionCandlesObject{Action: "subscribe", Channels: []SubscriptionCandlesSubObject{SubscriptionCandlesSubObject{Name: "candles", Interval: []string{interval}, Markets: []string{market}}}}
	if ws.subscriptionCandlesChannelMap == nil {
		ws.subscriptionCandlesChannelMap = map[string]map[string]chan SubscriptionCandles{}
	}
	if ws.subscriptionCandlesChannelMap[market] == nil {
		ws.subscriptionCandlesChannelMap[market] = map[string]chan SubscriptionCandles{}
	}
	if ws.subscriptionCandlesOptionsMap == nil {
		ws.subscriptionCandlesOptionsMap = map[string]map[string]SubscriptionCandlesObject{}
	}
	if ws.subscriptionCandlesOptionsMap[market] == nil {
		ws.subscriptionCandlesOptionsMap[market] = map[string]SubscriptionCandlesObject{}
	}
	ws.subscriptionCandlesChannelMap[market][interval] = make(chan SubscriptionCandles, 100)
	ws.subscriptionCandlesOptionsMap[market][interval] = options

	myMessage, _ := json.Marshal(options)

	ws.Write([]byte(myMessage))
	return ws.subscriptionCandlesChannelMap[market][interval]
}

func (ws *Websocket) SubscriptionTrades(market string) chan SubscriptionTrades {
	options := SubscriptionTradesBookObject{Action: "subscribe", Channels: []SubscriptionTradesBookSubObject{SubscriptionTradesBookSubObject{Name: "trades", Markets: []string{market}}}}
	if ws.subscriptionTradesChannelMap == nil {
		ws.subscriptionTradesChannelMap = map[string]chan SubscriptionTrades{}
	}
	if ws.subscriptionTradesOptionsMap == nil {
		ws.subscriptionTradesOptionsMap = map[string]SubscriptionTradesBookObject{}
	}
	ws.subscriptionTradesChannelMap[market] = make(chan SubscriptionTrades, 100)
	ws.subscriptionTradesOptionsMap[market] = options

	myMessage, _ := json.Marshal(options)
	ws.Write([]byte(myMessage))
	return ws.subscriptionTradesChannelMap[market]
}

func (ws *Websocket) SubscriptionBookUpdate(market string) chan SubscriptionBookUpdate {
	options := SubscriptionTradesBookObject{Action: "subscribe", Channels: []SubscriptionTradesBookSubObject{SubscriptionTradesBookSubObject{Name: "book", Markets: []string{market}}}}
	if ws.subscriptionBookUpdateChannelMap == nil {
		ws.subscriptionBookUpdateChannelMap = map[string]chan SubscriptionBookUpdate{}
	}
	if ws.subscriptionBookUpdateOptionsMap == nil {
		ws.subscriptionBookUpdateOptionsMap = map[string]SubscriptionTradesBookObject{}
	}
	ws.subscriptionBookUpdateChannelMap[market] = make(chan SubscriptionBookUpdate, 100)
	ws.subscriptionBookUpdateOptionsMap[market] = options
	myMessage, _ := json.Marshal(options)
	ws.Write([]byte(myMessage))
	return ws.subscriptionBookUpdateChannelMap[market]
}

func (ws *Websocket) SubscriptionBook(market string, options map[string]string) chan Book {
	ws.keepLocalBook = true
	options["action"] = "getBook"
	options["market"] = market
	secondOptions := SubscriptionTradesBookObject{Action: "subscribe", Channels: []SubscriptionTradesBookSubObject{SubscriptionTradesBookSubObject{Name: "book", Markets: []string{market}}}}

	if ws.subscriptionBookChannelMap == nil {
		ws.subscriptionBookChannelMap = map[string]chan Book{}
	}
	if ws.subscriptionBookOptionsFirstMap == nil {
		ws.subscriptionBookOptionsFirstMap = map[string]map[string]string{}
	}
	if ws.subscriptionBookOptionsSecondMap == nil {
		ws.subscriptionBookOptionsSecondMap = map[string]SubscriptionTradesBookObject{}
	}
	if ws.localBook.Book == nil {
		ws.localBook.Book = map[string]Book{}
	}
	ws.subscriptionBookChannelMap[market] = make(chan Book, 100)
	ws.subscriptionBookOptionsFirstMap[market] = options
	ws.subscriptionBookOptionsSecondMap[market] = secondOptions

	myMessage, _ := json.Marshal(options)
	ws.Write([]byte(myMessage))
	mySecondMessage, _ := json.Marshal(secondOptions)
	ws.Write([]byte(mySecondMessage))
	return ws.subscriptionBookChannelMap[market]
}

//TODO add error to return
func (ws *Websocket) handleMessage(message []byte) {


	//ws.DebugToConsole("FULL RESPONSE: " + string(message))
	var x map[string]interface{}
	err := json.Unmarshal(message, &x)
	if handleError(err) {
		errorToConsole("We are returning, this should not happen...")
		return
	}
	if _, ok := x["error"]; ok {
		if x["action"] == "authenticate" {
			//ws.authenticationFailed = true
		}
		var t CustomError
		err = json.Unmarshal(message, &t)
		if handleError(err) {
			errorToConsole("Something failed during reception of the authentication response.")
			return
		}
		//ws.errChannel <- MyError{CustomError: t}
	}
	if x["event"] == "authenticate" {
		//ws.authenticated = true
	} else if x["event"] == "book" {
		var t SubscriptionBookUpdate
		err = json.Unmarshal(message, &t)
		if handleError(err) {
			return
		}
		fmt.Printf(".")
		market, _ := x["market"].(string)
		if ws.subscriptionBookUpdateChannelMap[market] != nil {
			ws.subscriptionBookUpdateChannelMap[market] <- t
		}
		if ws.keepLocalBook {
			addToBook(t, ws)
		}
	} else if x["event"] == "trade" {
		var t SubscriptionTrades
		err = json.Unmarshal(message, &t)
		if handleError(err) {
			return
		}
		fmt.Printf("+")
		market, _ := x["market"].(string)
		if ws.subscriptionTradesChannelMap[market] != nil {
			ws.subscriptionTradesChannelMap[market] <- t
		}

	} else if x["event"] == "fill" {
		var t SubscriptionAccountFill
		err = json.Unmarshal(message, &t)
		if handleError(err) {
			return
		}
		market, _ := x["market"].(string)
		if ws.subscriptionAccountFillChannelMap[market] != nil {
			ws.subscriptionAccountFillChannelMap[market] <- t
		}
	} else if x["event"] == "order" {
		var t SubscriptionAccountOrder
		err = json.Unmarshal(message, &t)
		if handleError(err) {
			return
		}
		market, _ := x["market"].(string)
		if ws.subscriptionAccountOrderChannelMap[market] != nil {
			ws.subscriptionAccountOrderChannelMap[market] <- t
		}
	} else if x["event"] == "ticker" {
		var t SubscriptionTicker
		err = json.Unmarshal(message, &t)
		if handleError(err) {
			return
		}
		market, _ := x["market"].(string)
		ws.subscriptionTickerChannelMap[market] <- t
	} else if x["event"] == "ticker24h" {
		var t SubscriptionTicker24h
		err = json.Unmarshal(message, &t)
		if handleError(err) {
			return
		}
		for i := 0; i < len(t.Data); i++ {
			ws.subscriptionTicker24hChannelMap[t.Data[i].Market] <- t.Data[i]
		}
	} else if x["event"] == "candle" {
		var t PreCandle
		err := json.Unmarshal(message, &t)
		if err != nil {
			return
		}
		fmt.Printf("=")
		var candles []Candle
		for i := 0; i < len(t.Candle); i++ {
			entry := reflect.ValueOf(t.Candle[i])
			candles = append(candles, Candle{Timestamp: int(entry.Index(0).Interface().(float64)), Open: entry.Index(1).Interface().(string), High: entry.Index(2).Interface().(string), Low: entry.Index(3).Interface().(string), Close: entry.Index(4).Interface().(string), Volume: entry.Index(5).Interface().(string)})
		}
		market, _ := x["market"].(string)
		interval, _ := x["interval"].(string)
		ws.subscriptionCandlesChannelMap[market][interval] <- SubscriptionCandles{Event: t.Event, Market: t.Market, Interval: t.Interval, Candle: candles}
	}
	if x["action"] == "getTime" {
		var t TimeResponse
		err = json.Unmarshal(message, &t)
		if handleError(err) {
			return
		}
		ws.timeChannel <- t.Response

	} else if x["action"] == "getMarkets" {
		var t MarketsResponse
		err = json.Unmarshal(message, &t)
		if handleError(err) {
			return
		}
		ws.marketsChannel <- t.Response
	} else if x["action"] == "getAssets" {
		var t AssetsResponse
		err = json.Unmarshal(message, &t)
		if handleError(err) {
			return
		}
		ws.assetsChannel <- t.Response
	} else if x["action"] == "getBook" {
		var t BookResponse
		err = json.Unmarshal(message, &t)
		if handleError(err) {
			return
		}
		if ws.bookChannel != nil {
			ws.bookChannel <- t.Response
		}
		if ws.keepLocalBook {
			ws.localBook.Book[t.Response.Market] = t.Response
			ws.subscriptionBookChannelMap[t.Response.Market] <- ws.localBook.Book[t.Response.Market]
		}
	} else if x["action"] == "getTrades" {
		var t PublicTradesResponse
		err = json.Unmarshal(message, &t)
		if handleError(err) {
			return
		}
		ws.publicTradesChannel <- t.Response
	} else if x["action"] == "getCandles" {
		var t CandlesResponse
		err = json.Unmarshal(message, &t)
		if handleError(err) {
			return
		}
		var candles []Candle
		for i := 0; i < len(t.Response); i++ {
			entry := reflect.ValueOf(t.Response[i])
			candles = append(candles, Candle{Timestamp: int(entry.Index(0).Interface().(float64)), Open: entry.Index(1).Interface().(string), High: entry.Index(2).Interface().(string), Low: entry.Index(3).Interface().(string), Close: entry.Index(4).Interface().(string), Volume: entry.Index(5).Interface().(string)})
		}
		ws.candlesChannel <- candles
	} else if x["action"] == "getTicker24h" {
		var t Ticker24hResponse
		err = json.Unmarshal(message, &t)
		if handleError(err) {
			return
		}
		ws.ticker24hChannel <- t.Response
	} else if x["action"] == "getTickerPrice" {
		var t TickerPriceResponse
		err = json.Unmarshal(message, &t)
		if handleError(err) {
			return
		}
		ws.tickerPriceChannel <- t.Response
	} else if x["action"] == "getTickerBook" {
		var t TickerBookResponse
		err = json.Unmarshal(message, &t)
		if handleError(err) {
			return
		}
		ws.tickerBookChannel <- t.Response
	} else if x["action"] == "privateCreateOrder" {
		var t PlaceOrderResponse
		err = json.Unmarshal(message, &t)
		if handleError(err) {
			return
		}
		ws.placeOrderChannel <- t.Response
	} else if x["action"] == "privateUpdateOrder" {
		var t UpdateOrderResponse
		err = json.Unmarshal(message, &t)
		if handleError(err) {
			return
		}
		if t.Response.OrderId != "" {
			ws.updateOrderChannel <- t.Response
		}
	} else if x["action"] == "privateGetOrder" {
		var t GetOrderResponse
		err = json.Unmarshal(message, &t)
		if handleError(err) {
			return
		}
		ws.getOrderChannel <- t.Response
	} else if x["action"] == "privateCancelOrder" {
		var t CancelOrderResponse
		err = json.Unmarshal(message, &t)
		if handleError(err) {
			return
		}
		ws.cancelOrderChannel <- t.Response
	} else if x["action"] == "privateGetOrders" {
		var t GetOrdersResponse
		err = json.Unmarshal(message, &t)
		if handleError(err) {
			return
		}
		ws.getOrdersChannel <- t.Response
	} else if x["action"] == "privateCancelOrders" {
		var t CancelOrdersResponse
		err = json.Unmarshal(message, &t)
		if handleError(err) {
			return
		}
		ws.cancelOrdersChannel <- t.Response
	} else if x["action"] == "privateGetOrdersOpen" {
		var t OrdersOpenResponse
		err = json.Unmarshal(message, &t)
		if handleError(err) {
			return
		}
		ws.ordersOpenChannel <- t.Response
	} else if x["action"] == "privateGetTrades" {
		var t TradesResponse
		err = json.Unmarshal(message, &t)
		if handleError(err) {
			return
		}
		ws.tradesChannel <- t.Response
	} else if x["action"] == "privateGetAccount" {
		var t AccountResponse
		err = json.Unmarshal(message, &t)
		if handleError(err) {
			return
		}
		ws.accountChannel <- t.Response
	} else if x["action"] == "privateGetBalance" {
		var t BalanceResponse
		err = json.Unmarshal(message, &t)
		if handleError(err) {
			return
		}
		ws.balanceChannel <- t.Response
	} else if x["action"] == "privateDepositAssets" {
		var t DepositAssetsResponse
		err = json.Unmarshal(message, &t)
		if handleError(err) {
			return
		}
		ws.depositAssetsChannel <- t.Response
	} else if x["action"] == "privateWithdrawAssets" {
		var t WithdrawAssetsResponse
		err = json.Unmarshal(message, &t)
		if handleError(err) {
			return
		}
		ws.withdrawAssetsChannel <- t.Response
	} else if x["action"] == "privateGetDepositHistory" {
		var t HistoryResponse
		err = json.Unmarshal(message, &t)
		if handleError(err) {
			return
		}
		ws.depositHistoryChannel <- t.Response
	} else if x["action"] == "privateGetWithdrawalHistory" {
		var t HistoryResponse
		err = json.Unmarshal(message, &t)
		if handleError(err) {
			return
		}
		ws.withdrawalHistoryChannel <- t.Response
	}
}
