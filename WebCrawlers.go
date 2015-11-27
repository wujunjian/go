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
)

var ToDoUrl_ch chan string
var DownHtml_ch chan string
var ToDoUrlNum int
var DownHtmlNum int

var CrawledUrl map[string]int

var logger_task *log.Logger
var logger_err *log.Logger

var RootUrl string
var Regular string
var Domain string

var quit chan int

func initTaskCh() {
	for i := 0; i < ToDoUrlNum; i++ {
		ToDoUrl_ch = make(chan string,300000)
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
        
        //time.Sleep(100 * time.Millisecond)
        resp, err := http.Get(Url)
        if err!=nil {
            continue
            //log.Fatal(err)
            fmt.Println("err ", Url)
        }
        
        fmt.Println("request : ", Url)
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

func ParseHtml() {
    for {
        html,ok := <- DownHtml_ch
        if ok==false {
            logger_err.Println("DownHtml_ch closed")
            return
        }
        
        //fmt.Println(html)
        href := strings.Split(html, "href=\"")
        
        for i:=0;i<len(href);i++ {
        
            if !strings.Contains(href[i], "http") || strings.Contains(href[i], "<!DOCTYPE") {
                continue
            }

            NormalUrl := strings.Split(href[i], "\"")
            if CrawledUrl[NormalUrl[0]] == 0 {
                CrawledUrl[NormalUrl[0]] = 1
                fmt.Println("New Url : ", NormalUrl[0])
            } else {
                //fmt.Println("Crawled Url : ", NormalUrl[0])
                continue
            }

            if strings.Contains(NormalUrl[0], Regular) {
                logger_task.Println(NormalUrl[0])
            } else {
                //fmt.Println("other Url: ", NormalUrl[0])
            }
            ToDoUrl_ch<-NormalUrl[0]
        }
    }
    
    //reg := regexp.MustCompile(`[a-z]+`)
    //fmt.Printf("%q\n", reg.FindAllString(text, -1))
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

            if CrawledUrl[tmpUrl] == 0 {
                CrawledUrl[tmpUrl] = 1
                fmt.Println("New Url : ", tmpUrl)
            } else {
                //fmt.Println("Crawled Url : ", tmpUrl)
                continue
            }
            
            if strings.Contains(tmpUrl, Regular) {
                logger_task.Println(tmpUrl)
            } else {
                //fmt.Println("other Url: ", tmpUrl)
            }
            ToDoUrl_ch<-tmpUrl
        }
    }
    

}

func initmylog() {
    task_log_file := "./item.log"
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
    quit<-0
}

func main() {
    if len(os.Args) < 4 {
        fmt.Println("Argument: \n\t1. HomePage\n \t2. domain\n \t3. Regular\n")
        return
    }
    RootUrl = os.Args[1]
    Domain  = os.Args[2]
    Regular = os.Args[3]
    fmt.Println(RootUrl, Domain, Regular)
    
    runtime.GOMAXPROCS(runtime.NumCPU())

    ToDoUrlNum = 8
    DownHtmlNum = 8
    CrawledUrl = make(map[string]int)
    initTaskCh()
    initmylog()
    ToDoUrl_ch<-RootUrl
    
    for i:=0;i<ToDoUrlNum;i++ {
        go GetUrl()
    }
    
    for i:=0;i<DownHtmlNum;i++ {
        //go ParseHtml()
        go ParseHtmlWithRegExp()
    }
    
    go polling()

    
    <-quit
}
