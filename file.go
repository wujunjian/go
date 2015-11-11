package main

import (
	"fmt"
    "os"
	"os/exec"
	"runtime"
	"bufio"
	"hash/crc32"
	"time"
	"io"
    "log"
    "strings"
    "compress/gzip"
    "sync"
)

var sep = '\x02'
var sep_str = string(sep)

var logger_task *log.Logger
var logger_err *log.Logger

var task_ch []chan string
var quit chan int
var count int

type strSlice []string
var BasePkgMap map[string]strSlice
//var BasePkgMapLocker sync.RWMutex

type UserId map[string]int
var FindUserId map[string]UserId
var FindUserIdLocker sync.RWMutex

var OpenedFile map[string]*os.File
var OpenedFileLocker sync.RWMutex

func main() {

    var TaskFile string
    var BaseFile string
    if len(os.Args) > 2 {
        TaskFile = os.Args[1]
        BaseFile = os.Args[2]
    } else {
        fmt.Println("argument : 1.AnalysisFileName, 2.BasePkgFile")
        return
    }
    
    fmt.Println("Start time :", time.Now().Unix())
	runtime.GOMAXPROCS(runtime.NumCPU())
    initBasePkgMap(BaseFile)
    FindUserId = make(map[string]UserId)
    OpenedFile = make(map[string]*os.File)
    quit = make(chan int)
    //initmylog()
	initTaskCh()

    for i := 0; i < 24; i++ {
        tmpTaskFile:=fmt.Sprintf("%s-%02d", TaskFile, i)
        go fillTask(tmpTaskFile)
    }
    //go fillTaskByOpen(TaskFile)
	for i := 0; i < count; i++ {
        go processTask(i)
    }

    for i := 0; i < count+24; i++ {
        <- quit
        
        if i == 23 {
            for j := 0; j < count; j++ {
                close(task_ch[j])
            }
        }
    }
    fmt.Println("Finish time :", time.Now().Unix())
}

func initBasePkgMap(BaseFile string) {
    inputFile, inputError := os.Open(BaseFile)
    if inputError != nil {
        fmt.Println("An error occurred on opening the inputfile : ", inputError)
        return
    }
    BasePkgMap = make(map[string]strSlice)
    var PkgType string = ""
    defer inputFile.Close()
    inputReader := bufio.NewReader(inputFile)
    for {
        inputString, readerError := inputReader.ReadString('\n')
        if readerError == io.EOF {
            return
        }
        inputString = strings.Replace(inputString, string('\n'), "", -1)
        if strings.Contains(inputString, "[") && strings.Contains(inputString, "]") {
            inputString = strings.Replace(inputString, string("["), "", -1)
            inputString = strings.Replace(inputString, string("]"), "", -1)
            PkgType = inputString
        } else if len(inputString)==0 {
            PkgType = ""
            //fmt.Println("Empty Line")
        } else {
            //fmt.Println(PkgType, inputString)
            BasePkgMap[PkgType] = append(BasePkgMap[PkgType],inputString);
        }
    }
    
}

func initmylog() {
    task_log_file := "./task.log"
    tasklogfile,err := os.OpenFile(task_log_file,os.O_RDWR|os.O_CREATE,0)
    if err!=nil {
        fmt.Printf("%s\r\n",err.Error())
        os.Exit(-1)    
    }
    err_log_file := "./err.log"
    errlogfile,err := os.OpenFile(err_log_file,os.O_RDWR|os.O_CREATE,0)
    if err!=nil {
        fmt.Printf("%s\r\n",err.Error())
        os.Exit(-1)
    }
    logger_task = log.New(tasklogfile,"",0)
    logger_err = log.New(errlogfile,"",0)
}

func processTask(idx int){
	for {
		str_log,ok := <- task_ch[idx]
		if ok==false {
			fmt.Println("task queue is closed!")
            quit<-0
            return
		}

        excute(str_log)
	}
}

func excute(str_log string) {
    var strarr []string = strings.Split(str_log,sep_str)
    
    if len(strarr) < 42 {
        return
    }
    ExchangeUserId := strarr[8]
    AdPlaceType := strarr[14]
    AppPkgName := strarr[25]
    OSYSTEM := strarr[41]
    
    if AdPlaceType != "2" || len(ExchangeUserId) == 0 || len(AppPkgName) == 0 || !strings.Contains(OSYSTEM, "ios"){
        return
    }

    //fmt.Printf("ExchangeUserId:[%s],AdPlaceType:[%s],AppPkgName:[%s], OSYSTEM;[%s]\n", ExchangeUserId, AdPlaceType, AppPkgName, OSYSTEM)

    
    var IsFind bool = false
    for key,value:=range BasePkgMap {
        //fmt.Println(key)
        for _,PkgName:=range value {
            if PkgName == AppPkgName {
                IsFind = true
                break;
            }
        }
        
        if IsFind == true {
            //TODO Write to file
            //fmt.Println("find the Pkg :", key,ExchangeUserId,AdPlaceType,AppPkgName,OSYSTEM)
            
            //if IsDuplicate(key,ExchangeUserId) {
            //    //fmt.Printf("the UserID[%s] is already write to the file.\n", ExchangeUserId)
            //    break
            //}
            fileName := key + ".UserId"
            WriteToFile(fileName, ExchangeUserId)
            break
        }
    }
}

func IsDuplicate(key, UserId string) bool {
    
    var result bool
    FindUserIdLocker.RLock()
    //for _,value:=range FindUserId[key]{
    //    if value == UserId {
    //        FindUserIdLocker.RUnlock()
    //        return true
    //    }
    //}
    if FindUserId[key] == nil {
        FindUserId[key] = make(map[string]int)
        result = false
    }else {
        if FindUserId[key][UserId] == 0 {
            FindUserId[key][UserId]=1
            result = false
        } else {
            FindUserId[key][UserId]++
            result = true
        }
    }
    FindUserIdLocker.RUnlock()

    //FindUserIdLocker.Lock()
    //FindUserId[key] = append(FindUserId[key], UserId)
    //FindUserIdLocker.Unlock()
    return result
}
func WriteToFile(FileName, Content string){

    var outputFile *os.File
    
    OpenedFileLocker.RLock()
    for key,value := range OpenedFile {
        if key==FileName {
            outputFile = value
            break
        }
    }
    OpenedFileLocker. RUnlock()

    if outputFile == nil {
        tmpFileName := fmt.Sprintf("../UserId/%s.%d",FileName,time.Now().Unix())
        tmpoutputFile, outputError := os.OpenFile(tmpFileName,os.O_RDWR|os.O_APPEND|os.O_CREATE,0)
        if outputError != nil {
            fmt.Println("An error occurred on opening the outputFile : ", outputError)
            return
        }
        outputFile = tmpoutputFile
        
        //outputFile.WriteString("\n\n" + time.Now().String() + "\n")
        
        OpenedFileLocker.Lock()
        OpenedFile[FileName]=outputFile
        OpenedFileLocker.Unlock()
    }
    outputFile.WriteString(Content+"\n")
    //defer outputFile.Close()
}

func fillTaskByOpen(TaskFile string) {

    gzFile, gzFileError := os.Open(TaskFile)
    if gzFileError != nil {
        fmt.Println("An error occurred on opening the gzFileError : ", gzFileError)
        return
    }
    
    defer gzFile.Close()
    gzReader,gzReaderr := gzip.NewReader(gzFile)
    
    if gzReaderr != nil {
        fmt.Println("An error occurred on NewReader gzReaderr : ", gzReaderr)
        return
    }
    
    p := make([]byte, 10)
    for {

        num, err := gzReader.Read(p)
        //inputString, readerError := gzReader.ReadString('\n')
        if err == io.EOF {
            time.Sleep(1*time.Second)
            fmt.Println("AnalysisFile Finished !")
            for i := 0; i < count; i++ {
                close(task_ch[i])
            }
            break
            //continue
        }
        fmt.Println(num, string(p[:10]))
        time.Sleep(1*time.Second)
        
        task_ch[getCrc("test")%uint32(count)] <- "test"
    }
    
    quit<-0
}

func fillTask(TaskFile string){
    var tmpTaskFile string
	if len(TaskFile)==0 {
        tmpTaskFile="argument : AnalysisFileName"
    } else {
        tmpTaskFile=TaskFile
    }
    cmdStr := "zcat " + tmpTaskFile + "*.gz"
    fmt.Println(cmdStr)
    
    cmd := exec.Command("/bin/sh", "-c", cmdStr)
	stdout, _ := cmd.StdoutPipe()
	cmd.Start()
	inputReader := bufio.NewReader(stdout)
    for {
        inputString, readerError := inputReader.ReadString('\n')
        if readerError == io.EOF {
            time.Sleep(1*time.Second)
            fmt.Println("AnalysisFile Finished !")
            //for i := 0; i < count; i++ {
            //    close(task_ch[i])
            //}
            break
            //continue
        }
        task_ch[getCrc(inputString)%uint32(count)] <- inputString
    }
    
    quit<-0
}

func getCrc(key string) uint32 {  
    if len(key) < 64 {
        var scratch [64]byte  
        copy(scratch[:], key)   
        return crc32.ChecksumIEEE(scratch[:len(key)])  
    }  
    return crc32.ChecksumIEEE([]byte(key))  
}

func initTaskCh() {
	count = 32
	for i := 0; i < count; i++ {
		task_ch = append(task_ch,make(chan string,300000))
	}
}
