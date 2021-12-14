package main

import (
	bytes2 "bytes"
	"context"
	"crypto/ecdsa"
	"fmt"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"

	"io/ioutil"
	"log"
	"math/big"
	"time"
)


const (
	RpcUrl          = "https://exchaintestrpc.okex.org"
	ChainId int64   = 65 //  oec

	//RpcUrl          = "https://exchainrpc.okex.org"
	//ChainId int64   = 66 //  oec
	GasPrice int64  = 100000000 // 0.1 gwei
	GasLimit uint64 = 3000000
)

var (
	zeroToken  = big.NewInt(0)
	oneToken  = str2bigInt("1")
	manyTokens = str2bigInt("10000000000000")
	zeroPointOneToken = str2bigInt("0.1")
	emptyAddress = common.BigToAddress(big.NewInt(0))
)

func init() {
}


type Contract struct {
	name     string
	address  string
	addr     common.Address
	abi      abi.ABI
}

func NewContract(name, address, abiFile string) *Contract {
	c := &Contract{
		name: name,
		address: address,
		addr: common.HexToAddress(address),
	}

	abiByte, err := ioutil.ReadFile(abiFile)
	if err != nil {
		log.Fatal(err)
	}
	c.abi, err = abi.JSON(bytes2.NewReader(abiByte))
	if err != nil {
		log.Fatal(err)
	}

	return c
}

func str2bigInt(input string) *big.Int{
	return sdk.MustNewDecFromStr(input).Int
}

func uint256Output(client *ethclient.Client, c *Contract, name string, args ...interface{}) (*big.Int) {

	value := ReadContract(client, c, name, args...)
	ret := value[0].(*big.Int)

	arg0 := ""
	if len(args) > 0 {
		if value, ok := args[0].(common.Address); ok {
			arg0 = value.String()
		}
	}

	decRet := sdk.NewDecFromBigIntWithPrec(ret, sdk.Precision)

	fmt.Printf("	<%s[%s(%s)]> uint256 output: %s\n", c.name, name, arg0, decRet)
	return ret
}

func WriteContract(client *ethclient.Client,
	contract *Contract,
	fromAddress common.Address,
	privateKey *ecdsa.PrivateKey,
	amount *big.Int,
	sleep time.Duration,
	name string,
	args ...interface{}) {
	// 0. get the value of nonce, based on address
	nonce, err := client.PendingNonceAt(context.Background(), fromAddress)
	if err != nil {
		log.Fatalf("failed to fetch the value of nonce from network: %+v", err)
	}

	// 0.5 get the gasPrice
	gasPrice := big.NewInt(GasPrice)

	fmt.Printf(
		"==================================================\n"+
			"write [%s<%s>]: \n"+
			"	msg sender: <%s>\n"+
			"	contract address: <%s>\n"+
			"	abi: <%s %s>\n"+
			"==================================================\n",
		contract.name,
		name,
		fromAddress.Hex(),
		contract.address,
		name, args)

	data, err := contract.abi.Pack(name, args...)
	if err != nil {
		log.Fatal(err)
	}

	if amount == nil {
		amount = big.NewInt(0)
	}
	unsignedTx := types.NewTransaction(nonce, contract.addr, amount, GasLimit, gasPrice, data)

	// 2. sign unsignedTx -> rawTx
	signedTx, err := types.SignTx(unsignedTx, types.NewEIP155Signer(big.NewInt(ChainId)), privateKey)
	if err != nil {
		log.Fatalf("failed to sign the unsignedTx offline: %+v", err)
	}

	// 3. send rawTx
	err = client.SendTransaction(context.Background(), signedTx)
	if err != nil {
		log.Fatal(err)
	}

	time.Sleep(time.Second * sleep)
}


func TransferOKT(client *ethclient.Client,
	fromAddress common.Address,
	toAddress common.Address,
	amount *big.Int,
	privateKey *ecdsa.PrivateKey,
	sleep time.Duration) {
	// 0. get the value of nonce, based on address
	nonce, err := client.PendingNonceAt(context.Background(), fromAddress)
	if err != nil {
		log.Fatalf("failed to fetch the value of nonce from network: %+v", err)
	}

	// 0.5 get the gasPrice
	gasPrice := big.NewInt(GasPrice)

	fmt.Printf(
		"==================================================\n"+
			"Transfer OKT: \n"+
			"	from  : <%s>\n"+
			"	to    : <%s>\n"+
			"	amount: <%s>\n"+
			"==================================================\n",
		fromAddress,
		toAddress,
		sdk.NewDecFromBigIntWithPrec(amount, sdk.Precision),
		)

	unsignedTx := types.NewTransaction(nonce, toAddress, amount, GasLimit, gasPrice, nil)

	// 2. sign unsignedTx -> rawTx
	signedTx, err := types.SignTx(unsignedTx, types.NewEIP155Signer(big.NewInt(ChainId)), privateKey)
	if err != nil {
		log.Fatalf("failed to sign the unsignedTx offline: %+v", err)
	}

	// 3. send rawTx
	err = client.SendTransaction(context.Background(), signedTx)
	if err != nil {
		log.Fatal(err)
	}

	if sleep > 0 {
		time.Sleep(time.Second * sleep)
	}
}



func ReadContract(client *ethclient.Client, contract *Contract, name string, args ...interface{}) []interface{} {
	data, err := contract.abi.Pack(name, args ...)
	if err != nil {
		log.Fatal(err)
	}

	msg := ethereum.CallMsg{
		To:   &contract.addr,
		Data: data,
	}

	output, err := client.CallContract(context.Background(), msg, nil)
	if err != nil {
		panic(err)
	}

	ret, err := contract.abi.Unpack(name, output)
	if err != nil {
		panic(err)
	}
	return ret
}

func initKey(key string) (*ecdsa.PrivateKey, common.Address){
	privateKey, err := crypto.HexToECDSA(key)
	if err != nil {
		log.Fatalf("failed to switch unencrypted private key -> secp256k1 private key: %+v", err)
	}
	pubkey := privateKey.Public()
	pubkeyECDSA, ok := pubkey.(*ecdsa.PublicKey)
	if !ok {
		log.Fatalln("failed to switch secp256k1 private key -> pubkey")
	}
	senderAddress := crypto.PubkeyToAddress(*pubkeyECDSA)

	return privateKey, senderAddress
}



//func multiple(m *big.Int, n int64) *big.Int {
//	t := big.NewInt(0)
//	return t.Mul(m, big.NewInt(n))
//}

//
//func decToken(m int64, n int64) *big.Int {
//	multiple(oneToken, m)
//	multiple(zeroPointOneToken, n)
//
//	t := big.NewInt(0)
//	t.Add(multiple(oneToken, m), multiple(zeroPointOneToken, n))
//
//	return t
//}
//
//func decToken2(m int64, n1 int64, n2 int64) *big.Int {
//	multiple(oneToken, m)
//	multiple(zeroPointOneToken, n1)
//
//	t := big.NewInt(0)
//	t.Add(multiple(oneToken, m), multiple(zeroPointOneToken, n1))
//	t.Add(t, multiple(big.NewInt(1e16), n2))
//
//	return t
//}


