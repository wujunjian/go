package main

import (
    "bufio"
    "fmt"
    "io"
    "os"
)

func main() {
    var task_ch []chan string
    
    var ch_num int = 30
    
    //fmt.Println(cap(task_ch), len(task_ch))
    initChansofString(&task_ch, ch_num)
    //fmt.Println(cap(task_ch), len(task_ch))
    
    go FileToChans("D:\\GO\\MoneyCooDaemon.go", task_ch)   //TODO 
    
    for i:=0;i<ch_num;i++ {
       go process(task_ch[i])
    }
   
    var quit chan int
    <- quit
}

func process(ch chan string) {
    for {
        strlog,ok := <- ch
        if ok==false {
            fmt.Println("task queue empty!")
        }
        fmt.Print(strlog)
    }
}


func initChansofString(ch *[]chan string, num int) {
    for i:=0;i<num;i++ {
        *ch = append(*ch,make(chan string,300000))
    }
}

func FileToChans(file string, ch []chan string) {
    fmt.Println("FileToChans Begin : ", file)
    inputFile, inputError := os.Open(file)
    if inputError != nil {
        fmt.Println("An error occurred on opening the inputfile : ", inputError)
        return
    }

    defer inputFile.Close()
    inputReader := bufio.NewReader(inputFile)
    lineCounter := 0
    for {
        inputString, readerError := inputReader.ReadString('\n')
        //inputString, readerError := inputReader.ReadBytes('\n')
        if readerError == io.EOF {
            return
        }
        lineCounter++
        var line string 
        line = fmt.Sprintf("%d : %s", lineCounter, inputString)
        //fmt.Print(line)
        ch[lineCounter%len(ch)]<-line
    }
}