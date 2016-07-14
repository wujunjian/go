package main

import (
    "crypto/rand"
    "crypto/rsa"
    //"crypto/sha1"
    "crypto/sha256"
    "crypto/x509"
    "encoding/pem"
    "flag"
    "io/ioutil"
    "log"
    "fmt"
)

// Command-line flags
var (
    keyFile = flag.String("key", "server.key", "Path to RSA private key")
    inFile  = flag.String("in", "pri_in.txt", "Path to input file")
    outFile = flag.String("out", "pri_out.txt", "Path to output file")
    //label   = flag.String("label", "", "Label to use (filename by default)")
    decrypt = flag.Bool("decrypt", true, "Decrypt instead of encrypting")
)



//openssl genrsa -des3 -out server.key 1024     //˽Կ
//openssl rsa -in server.key -pubout -out public_key.pem  //˽Կ
//rsaexample2 -in=pub_out.txt -out=in_out.txt -decrypt=true

func main() {
    flag.Parse()

    // Read the input file
    in, err := ioutil.ReadFile(*inFile)
    if err != nil {
        log.Fatalf("input file: %s", err)
    }
    fmt.Println(len(in), in)

    // Read the private key
    pemData, err := ioutil.ReadFile(*keyFile)
    if err != nil {
        log.Fatalf("read key file: %s", err)
    }

    // Extract the PEM-encoded data block
    block, _ := pem.Decode(pemData)
    if block == nil {
        log.Fatalf("bad key data: %s", "not PEM-encoded")
    }
    if got, want := block.Type, "RSA PRIVATE KEY"; got != want {
        log.Fatalf("unknown key type %q, want %q", got, want)
    }

    // Decode the RSA private key
    priv, err := x509.ParsePKCS1PrivateKey(block.Bytes)
    if err != nil {
        log.Fatalf("bad private key: %s", err)
    }

    label := []byte("orders")
    var out []byte
    if *decrypt {
        // Decrypt the data
        out, err = rsa.DecryptOAEP(sha256.New(), rand.Reader, priv, in, label)
        if err != nil {
            log.Fatalf("decrypt: %s", err)
        }
    } else {
        out, err = rsa.EncryptOAEP(sha256.New(), rand.Reader, &priv.PublicKey, in, label)
        if err != nil {
            log.Fatalf("encrypt: %s", err)
        }
        //fmt.Println(priv.PublicKey)
        //fmt.Println(priv)
    }

    // Write data to output file
    if err := ioutil.WriteFile(*outFile, out, 0600); err != nil {
        log.Fatalf("write output: %s", err)
    }
}