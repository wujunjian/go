package main

import (
	"fmt"
    "os"
	"os/exec"
	"runtime"
	"bufio"
	"time"
	"io"
    "strings"
)

var IpMap []string
var IpFile string
var pingCmt string
var cmdArgument string
var quit chan int
    

func main() {

    if len(os.Args) > 3 {
        IpFile = os.Args[1]
        pingCmt = os.Args[2]
        cmdArgument = os.Args[3]
    } else if len(os.Args) == 3 {
        IpFile = os.Args[1]
        pingCmt = os.Args[2]
    } else {
        fmt.Println("argument : 1.IpFile 2.cmd 3.cmdArgument")
        return
    }
    
    quit = make(chan int)
    
	runtime.GOMAXPROCS(runtime.NumCPU())
    initIpFile(IpFile)
    //fmt.Println(IpMap)
    for i := 0; i < len(IpMap); i++ {
        go ToPing(i)
    }
    

    for i := 0; i < len(IpMap); i++ {
        <- quit
    }
    fmt.Println("ping err, check cmd")
}

func ToPing(num int) {
    pingStr := pingCmt + " " + cmdArgument + " " + IpMap[num];
    fmt.Println(pingStr)
    cmd := exec.Command("/bin/sh", "-c", pingStr)
    
    stdout, _ := cmd.StdoutPipe()
    
    cmd.Start()
    var success int
    var fal int
    inputReader := bufio.NewReader(stdout)
    for {
        inputString, readerError := inputReader.ReadString('\n')
        if readerError == io.EOF {
            time.Sleep(1*time.Second)
            quit<-0
            break
        }
        inputString = strings.Replace(inputString, string('\n'), "", -1)
        if strings.Contains(inputString, "bytes") {
            success++
        } else {
            fal++
        }
        if fal > 10 {
            fmt.Printf("\033[%d;%dH\033[2K\r\033[31m[%s] [s:%d|f:%d], %s", num+1, 3, IpMap[num], success, fal, inputString)
        } else {
            fmt.Printf("\033[%d;%dH\033[2K\r\033[32m[%s] [s:%d|f:%d], %s", num+1, 3, IpMap[num], success, fal, inputString)
        }
    }
}

func initIpFile(IpFile string) {
    inputFile, inputError := os.Open(IpFile)
    if inputError != nil {
        fmt.Println("An error occurred on opening the inputfile : ", inputError)
        return
    }
    defer inputFile.Close()
    inputReader := bufio.NewReader(inputFile)
    for {
        inputString, readerError := inputReader.ReadString('\n')
        if readerError == io.EOF {
            return
        }
        inputString = strings.Replace(inputString, string('\n'), "", -1)
        if len(inputString) == 0 {
            continue
        }
        IpMap = append(IpMap, inputString)
    }
    
}


