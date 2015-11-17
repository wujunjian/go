package main

import (
	"fmt"
	"flag"
	//"path/filepath"
	"github.com/garyburd/redigo/redis"
	"bufio"
	"io"
	"os"
	//"time"
	"encoding/json"
	"strings"
	"runtime"
	"hash/crc32"
	/*"crypto/md5"
  "encoding/hex"
	*/
	//"strconv"
)

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

func fillTaskCh(useridfile string,tch *chan string, q_ch *chan int){
		inputFile, inputError := os.Open(useridfile)
		if inputError != nil {
        fmt.Printf("An error occurred on opening the inputfile\n")
        close(*tch)
    		*q_ch <- 0
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

func doTaskCh(tch *chan string, q_ch *chan int, threadid int, redispool []*redis.Pool, redisnum int){
	sep := '\x02'
	sep_str := string(sep)
	for {
		v,ok := <- *tch
		if ok==false {
			*q_ch <- 0
			return
		}
		
		//time.Sleep(10)
		//fmt.Printf("Thread %d, data: %s\n",threadid,v)
		
		
		var dat map[string]interface{}
  	err := json.Unmarshal([]byte(v), &dat)
  	if err != nil {
  			fmt.Println(err)
        continue
  	}
  	
  	var newdat_map map[string]interface{}
  	if dat["exuid"] == nil {
  		continue
  	}
  	newdat_map = dat["exuid"].(map[string]interface{})
  	
  	
  	
  	var newdat string
  	if dat["tag"] == nil {
  		continue
  	}
  	nowdat := dat["tag"].([]interface{})
  	for x :=0;x<len(nowdat);x++ {
  		var tmptag string = nowdat[x].(string)
  		var item []string = strings.Split(tmptag,sep_str)
  		if len(item)<2 {
  			continue
  		}
  		newdat = newdat+item[0]+","
  	}
  	//fmt.Println("newdat:",newdat)
  	inum := 0
  	for key := range newdat_map {
  			inum++
  			if inum >20 {
  				break
  			}
        //fmt.Println("SETEX","rt_"+key,7776000,newdat)
        var redis_idx int = int(getCrc(key))
				redis_idx = redis_idx%redisnum
				// 从连接池里面获得一个连接
			  c := redispool[redis_idx].Get()
			  /*var str_tag string
			  str_tag, err := redis.String(c.Do("GET", "rt_"+key))
			  if err != nil{
			  	if err.Error() != "redigo: nil returned" {
			  		fmt.Printf("Redis %d err: %s  %s\n",redis_idx,err.Error(),str_tag)
			  		c.Close()
			  		continue
			  	}
			  }*/
			  //if len(str_tag) == 0 {
			  	c.Do("SETEX","rt_"+key,7776000,newdat)
			  	//fmt.Println("SETEX","rt_"+key,7776000,newdat)
			  //} else {
			  	//c.Do("SETEX","rt_"+key,7776000,str_tag+newdat)
			  	//fmt.Println("SETEX","rt_"+key,7776000,str_tag+newdat)
			  //}
			  //c.Do("SETEX","rt_"+key,7776000,newdat)
			  // 连接完关闭，其实没有关闭，是放回池里，也就是队列里面，等待下一个重用
			  c.Close()
    }
  	
  	//c.Do("SETEX","ofl_map_"+bhuid,7776000,string(record_obj_str))
	}
}

func doWork(useridfile string, redispool []*redis.Pool) {
	redis_num := len(redispool)
	count :=32
	
	task_ch :=make(chan string,30000)
	var quit chan int
	quit = make(chan int)
	go fillTaskCh(useridfile,&task_ch,&quit)
	for i := 0; i < count; i++ {
      go doTaskCh(&task_ch,&quit,i,redispool,redis_num)
  }

  for i := 0; i < count+1; i++ {
      <- quit
  }
}

func main() {
	runtime.GOMAXPROCS(8)
	// 生成连接池
	//pools := []*redis.Pool{newPool("127.0.0.1:6372"),newPool("127.0.0.1:6372"),newPool("127.0.0.1:6372"),newPool("127.0.0.1:6372")}
	pools := []*redis.Pool{newPool("192.168.1.157:6379"),newPool("192.168.1.150:6379"),newPool("192.168.1.149:6379"),newPool("192.168.1.136:6379"),newPool("192.168.1.154:6379"),newPool("192.168.1.206:6379"),newPool("192.168.1.148:6379"),newPool("192.168.1.158:6379"),newPool("192.168.1.151:6379"),newPool("192.168.1.130:6379"),newPool("192.168.1.153:6379"),newPool("192.168.1.135:6379"),newPool("192.168.1.147:6379"),newPool("192.168.1.132:6379"),newPool("192.168.1.131:6379"),newPool("192.168.1.162:6379"),newPool("192.168.1.163:6379"),newPool("192.168.1.164:6379"),newPool("192.168.1.165:6379"),newPool("192.168.1.178:6379"),newPool("192.168.1.159:6379"),newPool("192.168.1.189:6379"),newPool("192.168.1.190:6379"),newPool("192.168.1.191:6379"),newPool("192.168.1.192:6379"),newPool("192.168.1.193:6379"),newPool("192.168.1.194:6379"),newPool("192.168.1.195:6379"),newPool("192.168.1.196:6379"),newPool("192.168.1.197:6379"),newPool("192.168.1.198:6379"),newPool("192.168.1.199:6379"),newPool("192.168.1.200:6379"),newPool("192.168.1.201:6379"),newPool("192.168.1.202:6379"),newPool("192.168.1.203:6379"),newPool("192.168.1.204:6379"),newPool("192.168.1.205:6379"),newPool("192.168.1.206:6379"),newPool("192.168.1.207:6379"),newPool("192.168.1.208:6379")}
	flag.Parse()
  root := flag.Arg(0)
  if len(root) <=0 {
  	return
  }
  fmt.Println(root);
  doWork(root,pools)

}