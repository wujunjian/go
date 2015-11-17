package main

import (
	"fmt"
	"strings"
	"time"
	//"math/rand"
	"bufio"
	"io"
	"os"
	"path/filepath"
	"flag"
	"hash/crc32"
	"crypto/md5"
  "encoding/hex"
	"github.com/garyburd/redigo/redis"
	"encoding/json"
	"strconv"
	"log"
	"runtime"
)
var logger *log.Logger
var logger_time *log.Logger
var nowtime int64 = time.Now().Unix()
var timeout_date int64 =nowtime+7776000
func getMd5(key string) string {
	h := md5.New()
    h.Write([]byte(key))
    return hex.EncodeToString(h.Sum(nil))
}

func getCrc(key string) uint32 {  
    if len(key) < 64 {  
        var scratch [64]byte  
        copy(scratch[:], key)   
        return crc32.ChecksumIEEE(scratch[:len(key)])  
    }  
    return crc32.ChecksumIEEE([]byte(key))  
}

func newPool(server string) *redis.Pool {
    return &redis.Pool{
        MaxIdle: 16,
        MaxActive: 128, // max number of connections
        Dial: func() (redis.Conn, error) {
            c, err := redis.Dial("tcp", server)
            if err != nil {
                //panic(err.Error())
                fmt.Println(err.Error())
            }
            return c, err
        },
    }
}

func getFilelist(path string, ch *[]string) {
        err := filepath.Walk(path, func(path string, f os.FileInfo, err error) error {
                if ( f == nil ) {return err}
                if f.IsDir() {return nil}
                //println("xxx:",path)
                if ((!strings.Contains(path, "data.log"))&&(!strings.Contains(path, "time.log"))){
                	*ch = append(*ch,path)
                }
                return nil
        })
        if err != nil {
                fmt.Printf("filepath.Walk() returned %v\n", err)
        }
}

func fillTaskCh(tagfile string,tch *chan string, q_ch *chan int){
		inputFile, inputError := os.Open(tagfile)
		if inputError != nil {
        fmt.Printf("An error occurred on opening the inputfile\n")
        return
    }
    defer inputFile.Close()
    inputReader := bufio.NewReader(inputFile)
    for {
    	inputString, readerError := inputReader.ReadString('\n')
    	if readerError == io.EOF {
    				close(*tch)
    				*q_ch <- 0
            return
      }
      *tch <- inputString
    }
}

func updateBeheUser(strdata string, threadid int, tagname string, redispool []*redis.Pool, redisnum int) {
	sep := '\x02'
	sep_str := string(sep)
	var strarr []string = strings.Split(strdata," ")
	n := len(strarr)
	if n <= 0 {
		return
	}
	var bhuid string = strarr[0]
	bhuid = strings.Replace(bhuid, "data_", "", 1)
	//fmt.Println("ofl_map_"+bhuid)
	var redis_idx int = int(getCrc(bhuid))
	redis_idx = redis_idx%redisnum
	// 从连接池里面获得一个连接
  c := redispool[redis_idx].Get()
  // 连接完关闭，其实没有关闭，是放回池里，也就是队列里面，等待下一个重用
  defer c.Close()
  
  var str_json string
  str_json, err := redis.String(c.Do("GET", "ofl_map_"+bhuid))
  if err != nil{
  	if err.Error() != "redigo: nil returned" {
  		fmt.Printf("Redis %d err: %s  %s\n",redis_idx,err.Error(),str_json)
  		return
  	}
  }
  
  if len(str_json) == 0 {
  	//fmt.Println("record is NULL")
  	exuid_obj :=make(map[string]interface{})
  	if n > 20 {
  		n =20
  	}
  	for i :=1;i<(n-1);i++ {
  		var item []string = strings.Split(strarr[i],"^^")
  		if len(item)<2 {
  			continue
  		}
  		
  		exid ,errc :=strconv.Atoi(item[0])
  		if errc != nil{
  			exuid_obj[item[1]] = -1
  		}else{
  			exuid_obj[item[1]] = exid
  		}
  		//exuid_obj[strarr[i]] = 0
  	}
  	var tag_arr [1]string
  	tag_arr[0]=tagname+sep_str+strconv.Itoa(int(timeout_date))
  	
  	record_obj :=make(map[string]interface{})
  	record_obj["exuid"]=exuid_obj
  	record_obj["tag"]=tag_arr
  	record_obj_str, err := json.Marshal(record_obj)
    if err != nil {
        fmt.Println(err)
        return
    }
  	logger.Println(string(record_obj_str))
  	c.Do("SETEX","ofl_map_"+bhuid,7776000,string(record_obj_str))
  }else{
  	//fmt.Println("record is ",str_json)
  	var dat map[string]interface{}
  	err := json.Unmarshal([]byte(str_json), &dat)
  	if err != nil {
  			fmt.Println(err)
        return
  	}
  	var newdat_map map[string]interface{}
  	if dat["exuid"]!=nil {
  		newdat_map = dat["exuid"].(map[string]interface{})
  		if len(newdat_map)>20 {
  			return
  		}
  	}else{
  		newdat_map = make(map[string]interface{})
  	}
  	for i :=1;i<(n-1);i++ {
  		var item []string = strings.Split(strarr[i],"^^")
  		if len(item)<2 {
  			continue
  		}
  		
  		exid ,errc :=strconv.Atoi(item[0])
  		if errc != nil{
  			newdat_map[item[1]] = -1
  		}else{
  			newdat_map[item[1]] = exid
  		}
  	}
  	
  	
  	var newdat []string
  	if dat["tag"] != nil {
	  	nowdat := dat["tag"].([]interface{})
	  	for x :=0;x<len(nowdat);x++ {
	  		var tmptag string = nowdat[x].(string)
	  		var item []string = strings.Split(tmptag,sep_str)
	  		if len(item)<2 {
	  			continue
	  		}
	  		
	  		tmout ,errc :=strconv.Atoi(item[1])
	  		if errc != nil {
	  			fmt.Println(errc)
	  		}
	  		if(int64(tmout) < nowtime) {
	  			
	  			continue
	  		}
	  		if(tagname!=item[0]){
	  			newdat = append(newdat,nowdat[x].(string))
	  		}
	  	}
  	}
  	newdat = append(newdat,(tagname+sep_str+strconv.Itoa(int(timeout_date))))
  	
  	record_obj :=make(map[string]interface{})
  	record_obj["exuid"]=newdat_map
  	record_obj["tag"]=newdat
  	if dat["dm_tag"] != nil {
  		record_obj["dm_tag"]=dat["dm_tag"].(map[string]interface{})
  	}
  	record_obj_str, err := json.Marshal(record_obj)
    if err != nil {
        fmt.Println(err)
        return
    }
  	logger.Println(string(record_obj_str))
  	c.Do("SETEX","ofl_map_"+bhuid,7776000,string(record_obj_str))
  	//fmt.Println("record err: ",xxx,err)
  	//fmt.Println("record2 is ",dat["exuid"])
  }
	//fmt.Printf("Thread %d: (%d)%s--%s    %s\n",threadid,n,tagname,bhuid,str_json)
	/*for i :=0; i<n; i++ {
		
	}*/
	//time.Sleep(1)
}

func doTaskCh(tch *chan string, q_ch *chan int, threadid int, tagname string, redispool []*redis.Pool, redisnum int){
	/*var finish int = 0;
	for {
		
		select {
			case v :=<- *tch:{
				updateBeheUser(v,threadid,tagname,redispool,redisnum)
			}
			default:{
				//fmt.Println("    .",finish)
				time.Sleep(10 * time.Millisecond)
				finish++
				if finish ==3{
					*q_ch <- 0
					//fmt.Printf("Thread %d break\n",threadid)
					return
				}
			}
		}
	}*/
	//--------------------------------------------------
  var item []string = strings.Split(tagname,"/")
  n := len(item)
  var item_time []string = strings.Split(item[n-1],"@")
	n_time := len(item_time)
	var final_tag string = ""
	if n_time > 1 {
		x_timeout, _ :=strconv.Atoi(item_time[n-1])
		timeout_date = nowtime+int64(x_timeout)
		final_tag = item_time[n-2]
	}else{
		final_tag = item[n-1]
	}
	
	for {
		v,ok := <- *tch
		if ok==false {
			*q_ch <- 0
			return
		}
		updateBeheUser(v,threadid,final_tag,redispool,redisnum)
	}
}

func doWork(tagfile string, redispool []*redis.Pool) {
	redis_num := len(redispool)
	count :=32
	
	task_ch :=make(chan string,30000)
	var quit chan int
	quit = make(chan int)
	go fillTaskCh(tagfile,&task_ch,&quit)
	for i := 0; i < count; i++ {
      go doTaskCh(&task_ch,&quit,i,tagfile,redispool,redis_num)
  }

  for i := 0; i < count+1; i++ {
      <- quit
  }
}

func main() {
	runtime.GOMAXPROCS(8)
	flag.Parse()
  root := flag.Arg(0)
	data_log_file := root+"/data.log"
	logfile,err := os.OpenFile(data_log_file,os.O_RDWR|os.O_CREATE,0)
	if err!=nil {
	 fmt.Printf("%s\r\n",err.Error())
	 os.Exit(-1)
	}
	time_log_file := root+"/time.log"
	logfile_time,err := os.OpenFile(time_log_file,os.O_RDWR|os.O_CREATE,0)
	if err!=nil {
	 fmt.Printf("%s\r\n",err.Error())
	 os.Exit(-1)
	}
	defer logfile.Close()
	defer logfile_time.Close()
	logger = log.New(logfile,"",0)
  logger_time = log.New(logfile_time,"",0)
	start_time := time.Now().Unix()
	// 生成连接池
	pools := []*redis.Pool{newPool("192.168.1.179:6377"),newPool("192.168.1.180:6377"),newPool("192.168.1.181:6377"),newPool("192.168.1.183:6377")}
	//pools := []*redis.Pool{newPool("127.0.0.1:6379"),newPool("127.0.0.1:6379"),newPool("127.0.0.1:6379"),newPool("127.0.0.1:6379")}
	
  var mych []string
  getFilelist(root,&mych)
  n :=len(mych)
  for i :=0; i<n; i++ {
  	//fmt.Println("ss=",mych[i])
  	doWork(mych[i],pools)
  }
  end_time := time.Now().Unix()     
	logger_time.Println("use time:",(end_time-start_time))
}
