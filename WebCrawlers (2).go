package main

import (
    "fmt"
    "net/http"
    "os"
    "runtime"
    "strings"
    "time"
    //"bufio"
    "io/ioutil"
    "log"
    "regexp"
    "sync"
    //"bloomfilter"
)

var ToDoUrl_ch chan string
var DownHtml_ch chan string
var ToDoUrlNum int
var DownHtmlNum int

var CrawledUrl map[string]int

var logger_task *log.Logger
var logger_err *log.Logger

var RootUrl string
var Regular []string
var Domain string

var quit chan int

var CrawledUrlLocker sync.RWMutex

func initTaskCh() {
	for i := 0; i < ToDoUrlNum; i++ {
		ToDoUrl_ch = make(chan string,10000000)
	}
    
    for i := 0; i < DownHtmlNum; i++ {
		DownHtml_ch = make(chan string,300000)
	}
}

func GetUrl() {

    for {
        Url,ok := <- ToDoUrl_ch
        if ok==false {
            logger_err.Println("ToDoUrl_ch closed")
            return
        }
        
        fmt.Println("request : ", Url)
        resp, err := http.Get(Url)
        if err!=nil {
            continue
            //log.Fatal(err)
            fmt.Println("err ", Url)
        }

        robots, err := ioutil.ReadAll(resp.Body)
        resp.Body.Close()
        if err != nil {
            continue
            //log.Fatal(err)
        }
        resphtml := string(robots)
        //fmt.Println(resphtml)
        
        if len(resphtml) < 100 {
            continue
        }

        DownHtml_ch<-resphtml
    }
}

func ParseHtmlWithRegExp() {
    for {
        html,ok := <- DownHtml_ch
        if ok==false {
            logger_err.Println("DownHtml_ch closed")
            return
        }
        
        //fmt.Println(html)

        //reg := regexp.MustCompile(`http://[^"| |;]*"??`)
        reg := regexp.MustCompile(`href="[^"| |;]*"??`)
        NormalUrl := reg.FindAllString(html, -1)
        var tmpUrl string
        for i:=0; i<len(NormalUrl);i++ {
            tmpUrl = strings.Replace(NormalUrl[i], "href=\"", "", -1)
            if !strings.Contains(tmpUrl, "http") {
                tmpUrl = RootUrl + tmpUrl;
            }

            if strings.Contains(tmpUrl, ".jpg") || 
               strings.Contains(tmpUrl, ".JPG") ||
               strings.Contains(tmpUrl, ".png") ||
               strings.Contains(tmpUrl, "img") ||
               strings.Contains(tmpUrl, ".js") ||
               strings.Contains(tmpUrl, "javascript") ||
               !strings.Contains(tmpUrl, Domain){
                continue
            }
            
            if strings.Contains(tmpUrl, "#")  {
                tmp := strings.Split(tmpUrl, "#")
                tmpUrl = tmp[0]
            }
            if strings.Contains(tmpUrl, "html?") {
                tmp := strings.Split(tmpUrl, "html?")
                tmpUrl = tmp[0] + "html"
            }
            
            CrawledUrlLocker.RLock()
            num := CrawledUrl[tmpUrl]
            CrawledUrlLocker.RUnlock()
            if num == 0 {
                fmt.Println("New Url : ", tmpUrl)
                CrawledUrlLocker.Lock()
                CrawledUrl[tmpUrl] = 1
                CrawledUrlLocker.Unlock()
            } else {
                //fmt.Println("Crawled Url : ", tmpUrl)
                continue
            }
            var contains bool
            for i:=0;i<len(Regular);i++ {
                contains = strings.Contains(tmpUrl, Regular[i])
                if contains {
                    logger_task.Println(tmpUrl)
                    break
                }
            }
            ToDoUrl_ch<-tmpUrl
        }
    }
    

}

func initmylog() {
    task_log_file := Domain+".item.log"
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

func polling() {
    var second int
    for {
        UrlNum := len(ToDoUrl_ch)
        HtmlNum := len(DownHtml_ch)
        if UrlNum == 0 && HtmlNum == 0 {
            time.Sleep(1000 * time.Millisecond)
            second++
            if second == 10 {
                break;
            }
        } else {
            second = 0;
        }
        fmt.Println("chan : ", UrlNum, HtmlNum)
        time.Sleep(3000 * time.Millisecond)
    }
    close(ToDoUrl_ch)
    close(DownHtml_ch)
    time.Sleep(3000 * time.Millisecond)
    quit<-0
}

func main() {
    if len(os.Args) < 4 {
        fmt.Println("Argument: \n\t1. HomePage\n \t2. domain\n \t3. Regular\n")
        return
    }
    RootUrl = os.Args[1]
    Domain  = os.Args[2]
    tmpRegular := os.Args[3]
    fmt.Println(RootUrl, Domain, tmpRegular)
    
    tmpRegularArr := strings.Split(tmpRegular, ".")
    
    for i:=0;i<len(tmpRegularArr); i++ {
        Regular = append(Regular, tmpRegularArr[i])
    }

    cpunum := runtime.NumCPU()
    runtime.GOMAXPROCS(cpunum)

    ToDoUrlNum = 12
    DownHtmlNum = 4
    CrawledUrl = make(map[string]int)
    quit = make(chan int)
    
    initTaskCh()
    initmylog()
    ToDoUrl_ch<-RootUrl
    
    for i:=0;i<ToDoUrlNum;i++ {
        go GetUrl()
    }
    
    for i:=0;i<DownHtmlNum;i++ {
        go ParseHtmlWithRegExp()
    }
    
    go polling()

    <-quit
    fmt.Println("Quit")
}
