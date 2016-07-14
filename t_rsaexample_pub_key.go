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
    "os"
    "io"
)

// Command-line flags
var (
    keyFile = flag.String("key", "public_key.pem", "Path to RSA public key")
    inFile  = flag.String("in", "pub_in.txt", "Path to input file")
    outFile = flag.String("out", "pub_out.txt", "Path to output file")
    //label   = flag.String("label", "", "Label to use (filename by default)")
    decrypt = flag.Bool("decrypt", false, "Decrypt instead of encrypting")
)

//只有加密可用
//
func main() {
    flag.Parse()

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
    if got, want := block.Type, "PUBLIC KEY"; got != want {
        log.Fatalf("unknown key type %q, want %q", got, want)
    }

    // Decode the RSA public key
    pub, err := x509.ParsePKIXPublicKey(block.Bytes)
    if err != nil {
        log.Fatalf("bad public key: %s", err)
    }

    label := []byte("orders")
    var out []byte
    
    
    inputFile, inputError := os.Open(*inFile)
    if inputError != nil {
        log.Fatalf("input file: %s", inputError)
    }
    defer inputFile.Close()
    
    outputFile, outputError := os.OpenFile(*outFile,os.O_TRUNC|os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
    if outputError != nil {
        log.Fatalf("output file: %s", outputError)
    }
    defer outputFile.Close()
    
    in := make([]byte,62)
    for {
        rnum, rerr := inputFile.Read(in)
        if rerr == io.EOF {
            break
        }
        fmt.Println(rnum,in)
        if(rnum != len(in)) {
            in=in[0:rnum]
        }
        
        if *decrypt {

        } else {
            out, err = rsa.EncryptOAEP(sha256.New(), rand.Reader, (pub.(*rsa.PublicKey)), in, label)
            if err != nil {
                log.Fatalf("encrypt: %s", err)
            }
            //fmt.Println(priv.PublicKey)
            //fmt.Println(priv)
        }

        wnum, werr:=outputFile.Write(out)
        if werr != nil {
            log.Fatalf("Write: %s", werr, "written num", wnum)
        }
    }
}