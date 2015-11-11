package main

import (
	"fmt"
	"net/http"
	"os"
	"runtime"
	"strings"
	"time"
	"encoding/base64"
	"log"
	"strconv"
	"database/sql"
	"github.com/garyburd/redigo/redis"
  _ "github.com/go-sql-driver/mysql"
)

var logger_task *log.Logger
var logger_err *log.Logger
var db *sql.DB
var pools_redis []*redis.Pool
var task_ch []chan string
var count int

func newPool(server string) *redis.Pool {
    return &redis.Pool{
        MaxIdle: 32,
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

func initmysql() {
	var err error
	db, err = sql.Open("mysql", "real_budget:UeEyuS5czxB9he6W@(192.168.1.212:3306)/?charset=utf8")
	//db, err = sql.Open("mysql", "storm:9vG7WpECbszvFjye@(192.168.1.155:3306)/esp?charset=utf8")
	if err!=nil {
		fmt.Println("Connect Mysql err:",err)
		return
	}else{
		fmt.Println("Connect Mysql OK!")
	}
	db.SetMaxOpenConns(32)
	db.SetMaxIdleConns(16)
	db.Ping()
}

func paraURI(str_uri string, para_map *map[string]string) {
	ipos := strings.Index(str_uri, "?")
	var str_para []byte
	if ipos >=0 {
		str_para = []byte(str_uri)[ipos+1:]
	}
	var strarr []string = strings.Split(string(str_para),"&")
	n := len(strarr)
	
	for i :=0;i<n;i++ {
		var str_para_filed []byte
		var str_para_x []byte
		xpos := strings.Index(strarr[i],"=")
		if xpos >= 0 {
			str_para_filed = []byte(strarr[i])[0:xpos]
			str_para_x = []byte(strarr[i])[xpos+1:]
			(*para_map)[string(str_para_filed)]=string(str_para_x)
		}
  }
}

func addTask(writer http.ResponseWriter, req *http.Request) { 
	start_time := time.Now().UnixNano()

	prara_map := make(map[string]string)
	paraURI(req.URL.String(),&prara_map)
	str_tsk_list_b64 := prara_map["tsk"]
	hash_value,_ :=strconv.Atoi(prara_map["hv"])
	if hash_value < 0 {
		hash_value=-1*hash_value
	}
	var strarr []string = strings.Split(str_tsk_list_b64,",")
	n := len(strarr)
	idx := hash_value%count
	for i :=0;i<n;i++ {
  	task_key,_ := base64.StdEncoding.DecodeString(strarr[i])
  	task_ch[idx] <- string(task_key)
  }
  end_time := time.Now().UnixNano()
  fmt.Fprintln(writer, "ok use time:",(end_time-start_time)/1000000,"ms")
  return
}

func processTask(idx int){
	for {
		str_task_key,ok := <- task_ch[idx]
		if ok==false {
			fmt.Println("task queue empty!")
		}
		
		Excute(str_task_key,idx)
	}
}


/*func getTaskInfo(str_task_key string) (map[string]string),bool {
	redis_conn := pools_redis[0].Get()
	defer redis_conn.Close()
	task_rec,err:=redis.StringMap(redis_conn.Do("HGETALL",str_task_key))
	if err!=nil {
		fmt.Println("getTaskInfo Err:",click_rec,"HGETALL",str_task_key)
		return task_rec, false
	}
	return task_rec, true
}*/

func genSQL(str_task_key string) (string,string,bool,string,string) {
	var strarr []string = strings.Split(str_task_key,"^")
	if len(strarr) < 5 {
		return "","",false,"",""
	}
	if (len(strarr[2]) <= 0)||(len(strarr[3]) <= 0)||(len(strarr[4]) <= 0) {
			return "","",false,"",""
		}
	/*task_rec,isok:=getTaskInfo(str_task_key)
	if isok ==false{
		return "","",false,"",""
	}*/
	redis_conn := pools_redis[0].Get()
	defer redis_conn.Close()
	task_rec,err:=redis.StringMap(redis_conn.Do("HGETALL",str_task_key))
	if err!=nil {
		fmt.Println("getTaskInfo Err:",err,"HGETALL",str_task_key)
		return "","",false,"",""
	}
	
	view:=task_rec["view"]
	if len(view)<=0 {
		view = "0"
	}
	
	click:=task_rec["click"]
	if len(click)<=0 {
		click = "0"
	}
	
	userClick:=task_rec["userClick"]
	if len(userClick)<=0 {
		userClick = "0"
	}
	
	reach:=task_rec["arrive"]
	if len(reach)<=0 {
		reach = "0"
	}
	
	userReach:=task_rec["userReach"]
	if len(userReach)<=0 {
		userReach = "0"
	}
	
	action:=task_rec["action"]
	if len(action)<=0 {
		action = "0"
	}
	
	realSpend:=task_rec["realprice"]
	if len(realSpend)<=0 {
		realSpend = "0"
	}
	
	spend:=task_rec["private_price"]
	if len(spend)<=0 {
		spend = "0"
	}
	
	advSpend:=task_rec["open_price"]
	if len(advSpend)<=0 {
		advSpend = "0"
	}
	
	cSpend:=task_rec["click_price"]
	if len(cSpend)<=0 {
		cSpend = "0"
	}
	
	reportSpend:=advSpend
	algorithm_type:=task_rec["algorithm_type"]
	if algorithm_type=="2" {
		reportSpend = cSpend
	}
	
	if(strarr[0]=="TF"&&len(strarr)==6){
		var date_arr []string = strings.Split(strarr[1],"-")
		if len(date_arr)!=3{
			return "","",false,"",""
		}
		
		del_sql :="delete from again_dsp_report_time.order_hour_"+date_arr[0]+"_"+date_arr[1]+" where adAccountId = "+strarr[2]+" and campaignId = "+strarr[3]+" and orderId = "+strarr[4]+" and reportDate = '"+strarr[1]+"' and hour = '"+strarr[5]+"'"
		insert_sql :="insert into again_dsp_report_time.order_hour_"+date_arr[0]+"_"+date_arr[1]+" (reportDate,adAccountId,campaignId,orderId,hour,view,click,userClick,reach,userReach,action,realSpend,spend,advSpend,cSpend,reportSpend) values('"+strarr[1]+"','"+strarr[2]+"','"+strarr[3]+"','"+strarr[4]+"','"+strarr[5]+"','"+view+"','"+click+"','"+userClick+"','"+reach+"','"+userReach+"','"+action+"','"+realSpend+"','"+spend+"','"+advSpend+"','"+cSpend+"','"+reportSpend+"')"
		return del_sql,insert_sql,true,"",""
	}else if(strarr[0]=="TF"&&len(strarr)==5){
		var date_arr []string = strings.Split(strarr[1],"-")
		if len(date_arr)!=3{
			return "","",false,"",""
		}
		del_sql :="delete from again_dsp_report_time.order_day_"+date_arr[0]+"_"+date_arr[1]+" where adAccountId = "+strarr[2]+" and campaignId = "+strarr[3]+" and orderId = "+strarr[4]+" and reportDate = '"+strarr[1]+"'"
		insert_sql :="insert into again_dsp_report_time.order_day_"+date_arr[0]+"_"+date_arr[1]+" (reportDate,adAccountId,campaignId,orderId,view,click,userClick,reach,userReach,action,realSpend,spend,advSpend,cSpend,reportSpend) values('"+strarr[1]+"','"+strarr[2]+"','"+strarr[3]+"','"+strarr[4]+"','"+view+"','"+click+"','"+userClick+"','"+reach+"','"+userReach+"','"+action+"','"+realSpend+"','"+spend+"','"+advSpend+"','"+cSpend+"','"+reportSpend+"')"
		return del_sql,insert_sql,true,"",""
	}else if(strarr[0]=="BROWER"&&len(strarr)==6){
		var date_arr []string = strings.Split(strarr[1],"-")
		if len(date_arr)!=3{
			return "","",false,"",""
		}
		del_sql :="delete from again_dsp_report_browser.order_browser_"+date_arr[0]+"_"+date_arr[1]+" where adAccountId = "+strarr[2]+" and campaignId = "+strarr[3]+" and orderId = "+strarr[4]+" and reportDate = '"+strarr[1]+"' and browserId = '"+strarr[5]+"'"
		insert_sql :="insert into again_dsp_report_browser.order_browser_"+date_arr[0]+"_"+date_arr[1]+" (reportDate,adAccountId,campaignId,orderId,browserId,view,click,userClick,reach,userReach,action,realSpend,spend,advSpend,cSpend,reportSpend) values('"+strarr[1]+"','"+strarr[2]+"','"+strarr[3]+"','"+strarr[4]+"','"+strarr[5]+"','"+view+"','"+click+"','"+userClick+"','"+reach+"','"+userReach+"','"+action+"','"+realSpend+"','"+spend+"','"+advSpend+"','"+cSpend+"','"+reportSpend+"')"
		return del_sql,insert_sql,true,"",""
	}else if(strarr[0]=="OPERA"&&len(strarr)==6){
		var date_arr []string = strings.Split(strarr[1],"-")
		if len(date_arr)!=3{
			return "","",false,"",""
		}
		del_sql :="delete from again_dsp_report_system.order_system_"+date_arr[0]+"_"+date_arr[1]+" where adAccountId = "+strarr[2]+" and campaignId = "+strarr[3]+" and orderId = "+strarr[4]+" and reportDate = '"+strarr[1]+"' and osId = '"+strarr[5]+"'"
		insert_sql :="insert into again_dsp_report_system.order_system_"+date_arr[0]+"_"+date_arr[1]+" (reportDate,adAccountId,campaignId,orderId,osId,view,click,userClick,reach,userReach,action,realSpend,spend,advSpend,cSpend,reportSpend) values('"+strarr[1]+"','"+strarr[2]+"','"+strarr[3]+"','"+strarr[4]+"','"+strarr[5]+"','"+view+"','"+click+"','"+userClick+"','"+reach+"','"+userReach+"','"+action+"','"+realSpend+"','"+spend+"','"+advSpend+"','"+cSpend+"','"+reportSpend+"')"
		return del_sql,insert_sql,true,"",""
	}else if(strarr[0]=="PLACE"&&len(strarr)==6){
		var date_arr []string = strings.Split(strarr[1],"-")
		if len(date_arr)!=3{
			return "","",false,"",""
		}
		cityId:=strarr[5]
		provinceId:="0"
		areaId:="0"
		countryId:="0"
		if len(cityId)==15 {
			provinceId = string([]byte(cityId)[0:9])+"100100"
			areaId = string([]byte(cityId)[0:6])+"100100100"
			countryId = string([]byte(cityId)[0:3])+"100100100100"
			if(cityId==provinceId&&cityId!="137101101100100"&&cityId!="137101102100100"&&cityId!="137103101100100"&&cityId!="137105101100100"&&cityId!="137107103100100"&&cityId!="137107102100100"&&cityId!="137107101100100"){
				cityId = string([]byte(cityId)[0:9])+"100199"
			}
		}else{
			cityId="0"
		}
		
		del_sql :="delete from again_dsp_report_area.order_area_"+date_arr[0]+"_"+date_arr[1]+" where adAccountId = "+strarr[2]+" and campaignId = "+strarr[3]+" and orderId = "+strarr[4]+" and reportDate = '"+strarr[1]+"' and cityId = '"+cityId+"'"
		insert_sql :="insert into again_dsp_report_area.order_area_"+date_arr[0]+"_"+date_arr[1]+" (reportDate,adAccountId,campaignId,orderId,cityId,provinceId,countryId,areaId,view,click,userClick,reach,userReach,action,realSpend,spend,advSpend,cSpend,reportSpend) values('"+strarr[1]+"','"+strarr[2]+"','"+strarr[3]+"','"+strarr[4]+"','"+cityId+"','"+provinceId+"','"+countryId+"','"+areaId+"','"+view+"','"+click+"','"+userClick+"','"+reach+"','"+userReach+"','"+action+"','"+realSpend+"','"+spend+"','"+advSpend+"','"+cSpend+"','"+reportSpend+"')"
		return del_sql,insert_sql,true,"",""
	}else if(strarr[0]=="EXANGE"&&len(strarr)==6){
		var date_arr []string = strings.Split(strarr[1],"-")
		if len(date_arr)!=3{
			return "","",false,"",""
		}
		del_sql :="delete from again_dsp_report_source.order_source_"+date_arr[0]+"_"+date_arr[1]+" where adAccountId = "+strarr[2]+" and campaignId = "+strarr[3]+" and orderId = "+strarr[4]+" and reportDate = '"+strarr[1]+"' and sourceId = '"+strarr[5]+"'"
		insert_sql :="insert into again_dsp_report_source.order_source_"+date_arr[0]+"_"+date_arr[1]+" (reportDate,adAccountId,campaignId,orderId,sourceId,view,click,userClick,reach,userReach,action,realSpend,spend,advSpend,cSpend,reportSpend) values('"+strarr[1]+"','"+strarr[2]+"','"+strarr[3]+"','"+strarr[4]+"','"+strarr[5]+"','"+view+"','"+click+"','"+userClick+"','"+reach+"','"+userReach+"','"+action+"','"+realSpend+"','"+spend+"','"+advSpend+"','"+cSpend+"','"+reportSpend+"')"
		return del_sql,insert_sql,true,"",""
	}else if(strarr[0]=="CHANNEL"&&len(strarr)==8){
		var date_arr []string = strings.Split(strarr[1],"-")
		if len(date_arr)!=3{
			return "","",false,"",""
		}
		del_sql :="delete from again_dsp_report_channel.order_channel_"+date_arr[0]+"_"+date_arr[1]+" where adAccountId = "+strarr[2]+" and campaignId = "+strarr[3]+" and orderId = "+strarr[4]+" and reportDate = '"+strarr[1]+"' and parentChannelId = '"+strarr[5]+"' and channelId = '"+strarr[6]+"' and sourceId = '"+strarr[7]+"'"
		insert_sql :="insert into again_dsp_report_channel.order_channel_"+date_arr[0]+"_"+date_arr[1]+" (reportDate,adAccountId,campaignId,orderId,sourceId,parentChannelId,channelId,view,click,userClick,reach,userReach,action,realSpend,spend,advSpend,cSpend,reportSpend) values('"+strarr[1]+"','"+strarr[2]+"','"+strarr[3]+"','"+strarr[4]+"','"+strarr[7]+"','"+strarr[5]+"','"+strarr[6]+"','"+view+"','"+click+"','"+userClick+"','"+reach+"','"+userReach+"','"+action+"','"+realSpend+"','"+spend+"','"+advSpend+"','"+cSpend+"','"+reportSpend+"')"
		return del_sql,insert_sql,true,"",""
	}else if(strarr[0]=="CONTENT"&&len(strarr)==7){
		var date_arr []string = strings.Split(strarr[1],"-")
		if len(date_arr)!=3{
			return "","",false,"",""
		}
		media_type:=strarr[5]
		xpos := strings.Index(strarr[5],"_")
		if xpos >= 0 {
			media_type = string([]byte(strarr[5])[xpos+1:])
		}
		if len(media_type) <= 0 {
			media_type = "0"
		}
		del_sql :="delete from again_dsp_report_page.order_page_"+date_arr[0]+"_"+date_arr[1]+" where adAccountId = "+strarr[2]+" and campaignId = "+strarr[3]+" and orderId = "+strarr[4]+" and reportDate = '"+strarr[1]+"' and pId = '"+media_type+"' and sourceId = '"+strarr[6]+"'"
		insert_sql :="insert into again_dsp_report_page.order_page_"+date_arr[0]+"_"+date_arr[1]+" (reportDate,adAccountId,campaignId,orderId,pId,sourceId,view,click,userClick,reach,userReach,action,realSpend,spend,advSpend,cSpend,reportSpend) values('"+strarr[1]+"','"+strarr[2]+"','"+strarr[3]+"','"+strarr[4]+"','"+media_type+"','"+strarr[6]+"','"+view+"','"+click+"','"+userClick+"','"+reach+"','"+userReach+"','"+action+"','"+realSpend+"','"+spend+"','"+advSpend+"','"+cSpend+"','"+reportSpend+"')"
		return del_sql,insert_sql,true,"",""
	}else if(strarr[0]=="MATERIAL"&&len(strarr)==6){
		var date_arr []string = strings.Split(strarr[1],"-")
		if len(date_arr)!=3{
			return "","",false,"",""
		}
		del_sql :="delete from again_dsp_report_material.order_material_"+date_arr[0]+"_"+date_arr[1]+" where adAccountId = "+strarr[2]+" and campaignId = "+strarr[3]+" and orderId = "+strarr[4]+" and reportDate = '"+strarr[1]+"' and materialId = '"+strarr[5]+"'"
		insert_sql :="insert into again_dsp_report_material.order_material_"+date_arr[0]+"_"+date_arr[1]+" (reportDate,adAccountId,campaignId,orderId,materialId,view,click,userClick,reach,userReach,action,realSpend,spend,advSpend,cSpend,reportSpend) values('"+strarr[1]+"','"+strarr[2]+"','"+strarr[3]+"','"+strarr[4]+"','"+strarr[5]+"','"+view+"','"+click+"','"+userClick+"','"+reach+"','"+userReach+"','"+action+"','"+realSpend+"','"+spend+"','"+advSpend+"','"+cSpend+"','"+reportSpend+"')"
		return del_sql,insert_sql,true,"",""
	}else if(strarr[0]=="DOMAINCATE"&&len(strarr)==7){
		var date_arr []string = strings.Split(strarr[1],"-")
		if len(date_arr)!=3{
			return "","",false,"",""
		}
		del_sql :="delete from again_dsp_report_dcategory.order_category_"+date_arr[0]+"_"+date_arr[1]+" where adAccountId = "+strarr[2]+" and campaignId = "+strarr[3]+" and orderId = "+strarr[4]+" and reportDate = '"+strarr[1]+"' and parentId = "+strarr[5]+" and cid = '"+strarr[6]+"'"
		insert_sql :="insert into again_dsp_report_dcategory.order_category_"+date_arr[0]+"_"+date_arr[1]+" (reportDate,adAccountId,campaignId,orderId,parentId,cid,view,click,userClick,reach,userReach,action,realSpend,spend,advSpend,cSpend,reportSpend) values('"+strarr[1]+"','"+strarr[2]+"','"+strarr[3]+"','"+strarr[4]+"','"+strarr[5]+"','"+strarr[6]+"','"+view+"','"+click+"','"+userClick+"','"+reach+"','"+userReach+"','"+action+"','"+realSpend+"','"+spend+"','"+advSpend+"','"+cSpend+"','"+reportSpend+"')"
		return del_sql,insert_sql,true,"",""
	}else if(strarr[0]=="CROWD"&&len(strarr)==6){
		var date_arr []string = strings.Split(strarr[1],"-")
		if len(date_arr)!=3{
			return "","",false,"",""
		}
		cid,sourceid,_:=parseUserCate(strarr[5])
		del_sql :="delete from again_dsp_report_crowd.order_crowd_"+date_arr[0]+"_"+date_arr[1]+" where adAccountId = "+strarr[2]+" and campaignId = "+strarr[3]+" and orderId = "+strarr[4]+" and reportDate = '"+strarr[1]+"' and sourceId = '"+sourceid+"' and cid = '"+cid+"'"
		insert_sql :="insert into again_dsp_report_crowd.order_crowd_"+date_arr[0]+"_"+date_arr[1]+" (reportDate,adAccountId,campaignId,orderId,sourceId,cid,view,click,userClick,reach,userReach,action,realSpend,spend,advSpend,cSpend,reportSpend) values('"+strarr[1]+"','"+strarr[2]+"','"+strarr[3]+"','"+strarr[4]+"','"+sourceid+"','"+cid+"','"+view+"','"+click+"','"+userClick+"','"+reach+"','"+userReach+"','"+action+"','"+realSpend+"','"+spend+"','"+advSpend+"','"+cSpend+"','"+reportSpend+"')"
		return del_sql,insert_sql,true,"",""
		
		/*else if parseok == 2 {
			del_sql :="delete from again_dsp_report_gender.order_gender_"+date_arr[0]+"_"+date_arr[1]+" where adAccountId = "+strarr[2]+" and campaignId = "+strarr[3]+" and orderId = "+strarr[4]+" and reportDate = '"+strarr[1]+"' and sourceId = '"+sourceid+"' and gid = '"+cid+"'"
			insert_sql :="insert into again_dsp_report_gender.order_gender_"+date_arr[0]+"_"+date_arr[1]+" (reportDate,adAccountId,campaignId,orderId,sourceId,gid,view,click,userClick,reach,userReach,action,realSpend,spend,advSpend,cSpend,reportSpend) values('"+strarr[1]+"','"+strarr[2]+"','"+strarr[3]+"','"+strarr[4]+"','"+sourceid+"','"+cid+"','"+view+"','"+click+"','"+userClick+"','"+reach+"','"+userReach+"','"+action+"','"+realSpend+"','"+spend+"','"+advSpend+"','"+cSpend+"','"+reportSpend+"')"
			return del_sql,insert_sql,true,"",""
		}else if parseok == 3 {
			del_sql :="delete from again_dsp_report_gender.order_gender_"+date_arr[0]+"_"+date_arr[1]+" where adAccountId = "+strarr[2]+" and campaignId = "+strarr[3]+" and orderId = "+strarr[4]+" and reportDate = '"+strarr[1]+"' and sourceId = '"+sourceid+"' and gid = '"+cid+"'"
			insert_sql :="insert into again_dsp_report_gender.order_gender_"+date_arr[0]+"_"+date_arr[1]+" (reportDate,adAccountId,campaignId,orderId,sourceId,gid,view,click,userClick,reach,userReach,action,realSpend,spend,advSpend,cSpend,reportSpend) values('"+strarr[1]+"','"+strarr[2]+"','"+strarr[3]+"','"+strarr[4]+"','"+sourceid+"','"+cid+"','"+view+"','"+click+"','"+userClick+"','"+reach+"','"+userReach+"','"+action+"','"+realSpend+"','"+spend+"','"+advSpend+"','"+cSpend+"','"+reportSpend+"')"
			del_sql2 :="delete from again_dsp_report_crowd.order_crowd_"+date_arr[0]+"_"+date_arr[1]+" where adAccountId = "+strarr[2]+" and campaignId = "+strarr[3]+" and orderId = "+strarr[4]+" and reportDate = '"+strarr[1]+"' and sourceId = '"+sourceid+"' and cid = '"+cid+"'"
			insert_sql2 :="insert into again_dsp_report_crowd.order_crowd_"+date_arr[0]+"_"+date_arr[1]+" (reportDate,adAccountId,campaignId,orderId,sourceId,cid,view,click,userClick,reach,userReach,action,realSpend,spend,advSpend,cSpend,reportSpend) values('"+strarr[1]+"','"+strarr[2]+"','"+strarr[3]+"','"+strarr[4]+"','"+sourceid+"','"+cid+"','"+view+"','"+click+"','"+userClick+"','"+reach+"','"+userReach+"','"+action+"','"+realSpend+"','"+spend+"','"+advSpend+"','"+cSpend+"','"+reportSpend+"')"
			return del_sql,insert_sql,true,del_sql2,insert_sql2
		}else{
			return "","",false,"",""
		}
		*/
	}else if(strarr[0]=="GENDER"&&len(strarr)==6){
		var date_arr []string = strings.Split(strarr[1],"-")
		if len(date_arr)!=3{
			return "","",false,"",""
		}
		cid,sourceid,_:=parseUserCate(strarr[5])
		del_sql :="delete from again_dsp_report_gender.order_gender_"+date_arr[0]+"_"+date_arr[1]+" where adAccountId = "+strarr[2]+" and campaignId = "+strarr[3]+" and orderId = "+strarr[4]+" and reportDate = '"+strarr[1]+"' and sourceId = '"+sourceid+"' and gid = '"+cid+"'"
		insert_sql :="insert into again_dsp_report_gender.order_gender_"+date_arr[0]+"_"+date_arr[1]+" (reportDate,adAccountId,campaignId,orderId,sourceId,gid,view,click,userClick,reach,userReach,action,realSpend,spend,advSpend,cSpend,reportSpend) values('"+strarr[1]+"','"+strarr[2]+"','"+strarr[3]+"','"+strarr[4]+"','"+sourceid+"','"+cid+"','"+view+"','"+click+"','"+userClick+"','"+reach+"','"+userReach+"','"+action+"','"+realSpend+"','"+spend+"','"+advSpend+"','"+cSpend+"','"+reportSpend+"')"
		return del_sql,insert_sql,true,"",""
		
		/*if parseok == 1 {
			del_sql :="delete from again_dsp_report_crowd.order_crowd_"+date_arr[0]+"_"+date_arr[1]+" where adAccountId = "+strarr[2]+" and campaignId = "+strarr[3]+" and orderId = "+strarr[4]+" and reportDate = '"+strarr[1]+"' and sourceId = '"+sourceid+"' and cid = '"+cid+"'"
			insert_sql :="insert into again_dsp_report_crowd.order_crowd_"+date_arr[0]+"_"+date_arr[1]+" (reportDate,adAccountId,campaignId,orderId,sourceId,cid,view,click,userClick,reach,userReach,action,realSpend,spend,advSpend,cSpend,reportSpend) values('"+strarr[1]+"','"+strarr[2]+"','"+strarr[3]+"','"+strarr[4]+"','"+sourceid+"','"+cid+"','"+view+"','"+click+"','"+userClick+"','"+reach+"','"+userReach+"','"+action+"','"+realSpend+"','"+spend+"','"+advSpend+"','"+cSpend+"','"+reportSpend+"')"
			return del_sql,insert_sql,true,"",""
		}else */
		
		
		
		/*else if parseok == 3 {
			del_sql :="delete from again_dsp_report_gender.order_gender_"+date_arr[0]+"_"+date_arr[1]+" where adAccountId = "+strarr[2]+" and campaignId = "+strarr[3]+" and orderId = "+strarr[4]+" and reportDate = '"+strarr[1]+"' and sourceId = '"+sourceid+"' and gid = '"+cid+"'"
			insert_sql :="insert into again_dsp_report_gender.order_gender_"+date_arr[0]+"_"+date_arr[1]+" (reportDate,adAccountId,campaignId,orderId,sourceId,gid,view,click,userClick,reach,userReach,action,realSpend,spend,advSpend,cSpend,reportSpend) values('"+strarr[1]+"','"+strarr[2]+"','"+strarr[3]+"','"+strarr[4]+"','"+sourceid+"','"+cid+"','"+view+"','"+click+"','"+userClick+"','"+reach+"','"+userReach+"','"+action+"','"+realSpend+"','"+spend+"','"+advSpend+"','"+cSpend+"','"+reportSpend+"')"
			del_sql2 :="delete from again_dsp_report_crowd.order_crowd_"+date_arr[0]+"_"+date_arr[1]+" where adAccountId = "+strarr[2]+" and campaignId = "+strarr[3]+" and orderId = "+strarr[4]+" and reportDate = '"+strarr[1]+"' and sourceId = '"+sourceid+"' and cid = '"+cid+"'"
			insert_sql2 :="insert into again_dsp_report_crowd.order_crowd_"+date_arr[0]+"_"+date_arr[1]+" (reportDate,adAccountId,campaignId,orderId,sourceId,cid,view,click,userClick,reach,userReach,action,realSpend,spend,advSpend,cSpend,reportSpend) values('"+strarr[1]+"','"+strarr[2]+"','"+strarr[3]+"','"+strarr[4]+"','"+sourceid+"','"+cid+"','"+view+"','"+click+"','"+userClick+"','"+reach+"','"+userReach+"','"+action+"','"+realSpend+"','"+spend+"','"+advSpend+"','"+cSpend+"','"+reportSpend+"')"
			return del_sql,insert_sql,true,del_sql2,insert_sql2
		}else{
			return "","",false,"",""
		}*/
	}
	return "","",false,"",""
}

func parseUserCate(uctag string) (string,string,int) {
	if len(uctag) <= 0 {
		return "","",0
	}
	cid:=""
	sourceid:=""
	res:=0
	var date_arr []string = strings.Split(uctag,"_")
	if len(date_arr) <=1 {
		return "","",0
	}
	if date_arr[0]=="bh" {
		sourceid = "0"
		res=1
		cid=string([]byte(uctag)[3:])
		if cid == "0" {
			res=3
		}
		if strings.HasPrefix(cid, "gd") {
			cid=string([]byte(cid)[3:])
			res=2
		}
	}else if date_arr[0]=="bd" {
		sourceid = "11"
		res=1
		cid=string([]byte(uctag)[3:])
		if strings.HasPrefix(cid, "gd") {
			if cid == "gd_m"{
				cid="10001"
			}else if cid == "gd_f"{
				cid="10002"
			}else{
				cid="10000"
			}
			res=2
		}
		/*else if strings.HasPrefix(cid, "ag") {
			cid=string([]byte(cid)[3:])
		}*/
	}else if date_arr[0]=="mz" {
		sourceid = "7"
		res=1
		cid=string([]byte(uctag)[3:])
		if strings.HasPrefix(cid, "gd") {
			if cid == "gd_m"{
				cid="10001"
			}else if cid == "gd_f"{
				cid="10002"
			}else{
				cid="10000"
			}
			res=2
		}
		/*else if strings.HasPrefix(cid, "ag") {
			cid=string([]byte(cid)[3:])
		}else if strings.HasPrefix(cid, "slr") {
			cid=string([]byte(cid)[4:])
		}*/
	}else if date_arr[0]=="bc" {
		sourceid = "39"
		res=1
		cid=string([]byte(uctag)[3:])
		if strings.HasPrefix(cid, "gd") {
			if cid == "gd_m"{
				cid="10001"
			}else if cid == "gd_f"{
				cid="10002"
			}else{
				cid="10000"
			}
			res=2
		}
		/*else if strings.HasPrefix(cid, "ag") {
			cid=string([]byte(cid)[3:])
		}*/
	}else if date_arr[0]=="ab" {
		sourceid = "43"
		cid=string([]byte(uctag)[3:])
	}else if date_arr[0]=="tb" {
		sourceid = "3"
		cid=string([]byte(uctag)[3:])
	}else if date_arr[0]=="xf" {
		sourceid = "45"
		res=1
		cid=string([]byte(uctag)[3:])
		if strings.HasPrefix(cid, "gd") {
			if cid == "gd_m"{
				cid="10001"
			}else if cid == "gd_f"{
				cid="10002"
			}else{
				cid="10000"
			}
			res=2
		}
	}else{
		return "","",0
	}
	return cid,sourceid,res
}

func excuteSql(str_sql string,idx int,str_task_key string) {
		if len(str_sql)<=0 {
			return
		}
		stmt,err:=db.Prepare(str_sql)
		if err!=nil {
			logger_err.Println("Thread id:",idx,",Task:",str_task_key,",Sql:",str_sql,",Err:",err)
			return
		}
		defer stmt.Close();
		res,err_r:=stmt.Exec()
		if err_r!=nil {
			logger_err.Println("Thread id:",idx,",Task:",str_task_key,",Sql:",str_sql,",Err:",err_r)
			return
		}
		logger_task.Println("Thread id:",idx,",Task:",str_task_key,",Sql:",str_sql,",Res:",res)
		return
}

func Excute(str_task_key string,idx int) {
		str_sql_del,str_sql_insert,isok,str_sql_del2,str_sql_insert2 := genSQL(str_task_key)
		if isok == false{
			return
		}
		
		//fmt.Println("Thread id:",idx,",Task:",str_task_key,",SqlDel:",str_sql_del)
		//fmt.Println("Thread id:",idx,",Task:",str_task_key,",SqlIns:",str_sql_insert)
		excuteSql(str_sql_del,idx,str_task_key)
		excuteSql(str_sql_insert,idx,str_task_key)
		if len(str_sql_del2)>0 && len(str_sql_insert2)>0 {
			excuteSql(str_sql_del2,idx,str_task_key)
			excuteSql(str_sql_insert2,idx,str_task_key)
		}
		return
}

func main() {
	pools_redis = append(pools_redis,newPool("192.168.1.185:6373"),newPool("192.168.1.185:6373"))
	runtime.GOMAXPROCS(runtime.NumCPU())
	initmylog()
	initmysql()
	count = 32
	
	for i := 0; i < count; i++ {
		task_ch = append(task_ch,make(chan string,300000))
	}
	
	for i := 0; i < count; i++ {
      go processTask(i)
  }
	http.Handle("/set", http.HandlerFunc(addTask));
	
	err := http.ListenAndServe(":8041",nil); 
	if err != nil {
		fmt.Println("Err", err)
	}
}