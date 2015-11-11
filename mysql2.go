package main

import (
	"fmt"
	"sync"
	"os/exec"
	"runtime"
	"bufio"
	"hash/crc32"
	"time"
	"crypto/md5"
	"encoding/hex"
	"encoding/base64"
	"io"
	"strings"
  "strconv"
  "net/http"
	"io/ioutil"
	"net"
  "github.com/garyburd/redigo/redis"
)

type reportInfo struct {
	order_id		string
	camp_id			string
	advert_id		string
	algorithm_type string
	
	true_cost 	int64
	sys_cost  	int64
	adv_cost  	int64
	click_cost 	int64
	view_num  	int64
	click_num  	int64
	arrive_num	int64
	action_num	int64
	
	user_click_num int64
	user_arrive_num int64
	
	two_jump_num int64
	alive_time int64
	alive_num int64
	m_lock sync.Mutex
}


type RecoredSet struct {
	m_record map[string]*reportInfo
	m_record_lock sync.RWMutex
}

/*type ClickInfo struct {
	ad_id				string
	order_id		string
	camp_id			string
	advert_id		string
	
	browser			string
	os					string
	slotid			string
	areaid			string
	domain			string
	esd					string
	exchangeid	string
	cs					string
	channel			string
	media_type	string
	
	str_day			string
	str_hour		string

}*/


var sep = '\x02'
var sep_str = string(sep)

var g_recored []RecoredSet
var shift_g_recored_lock sync.RWMutex
var rec_idx int
var recordset_size int

var task_ch []chan string
var count int

var pools_redis []*redis.Pool
var pools_redis_click []*redis.Pool
var pools_redis_sid []*redis.Pool
var pools_redis_dcate []*redis.Pool


var domain_cate_map []map[string]string
var idx_domain_cate_map int
var count_domain_cate_map int

var conn_http *http.Client = &http.Client{

    Transport: &http.Transport{
        Dial: func(netw, addr string) (net.Conn, error) {
            conn_http, err := net.DialTimeout(netw, addr, time.Second*1)
            if err != nil {
                fmt.Println("dail timeout", err)
                return nil, err
            }
            conn_http.SetDeadline(time.Now().Add(time.Second * 15))
            return conn_http, nil

        },
        MaxIdleConnsPerHost:   64,
        ResponseHeaderTimeout: time.Millisecond * 40,
        DisableKeepAlives: false,
    },
}

func requestHttp(str_task_key string) string {
		hv:=getCrc(str_task_key)
		str_hv:=strconv.Itoa(int(hv))
		str_b64:=base64.StdEncoding.EncodeToString([]byte(str_task_key))
		str_url:="http://mysql.behe.com:8041/set?hv="+str_hv+"&tsk="+str_b64
		resp, err := conn_http.Get(str_url)
    if err != nil {
        fmt.Println("Err:",err)
        return ""
    }
 
    defer resp.Body.Close()
    body, err := ioutil.ReadAll(resp.Body)
    if err != nil {
        fmt.Println("Err:",err)
        return ""
    }
    return string(body)
}

func initDomainCateMap() {
	domain_cate_map = append(domain_cate_map,make(map[string]string))
	domain_cate_map = append(domain_cate_map,make(map[string]string))
	idx_domain_cate_map=0
	count_domain_cate_map=2
	loadDoaminCateMap(idx_domain_cate_map)
	fmt.Println("LoadDomainMap size:",len(domain_cate_map[idx_domain_cate_map]))
}

func loadDoaminCateMap(idx int) {
	redis_conn_dcate := pools_redis_dcate[0].Get()
	defer redis_conn_dcate.Close()
	res,err:=redis.Values(redis_conn_dcate.Do("LRANGE","domain_category",0,-1))
	if err!=nil {
		fmt.Println("loadDoaminCateMap Err:",res,"LRANGE","domain_category",0,-1)
		return
	}
	
	domain_cate_map[idx]=make(map[string]string)
	for i, data := range res {
		var strarr []string = strings.Split(string(data.([]byte)),",")
		if len(strarr) == 3 {
			domain_cate_map[idx][strarr[0]]=strarr[1]+"^"+strarr[2]
		}else{
			fmt.Println("err len:",i)
		}
	}
	
}

func reloadDomainCateMap(){
	for{
		time.Sleep(86400*time.Second)
		loadDoaminCateMap((idx_domain_cate_map+1)%count_domain_cate_map)
		idx_domain_cate_map=(idx_domain_cate_map+1)%count_domain_cate_map
	}
}

func getDomainCate(str_domain string) string {
	res:=domain_cate_map[idx_domain_cate_map][str_domain]
	if len(res) <= 0{
		res = "0^0"
	}
	return res
}
/*
//------for arrive and action----------
var g_click_map []map[string]([]ClickInfo)
var click_map_idx int
var click_map_num int
var shift_g_click_map_lock sync.RWMutex
//-------------------------------------
func initClickMap(){
	g_click_map = append(g_click_map,make(map[string]([]ClickInfo)))
}
*/

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

func newPool(server string) *redis.Pool {
    return &redis.Pool{
        MaxIdle: 32,
        MaxActive: 64, // max number of connections
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

func getMd5(key string) string {
	h := md5.New()
    h.Write([]byte(key))
    return hex.EncodeToString(h.Sum(nil))
}

func initRecoredSet(){
	var v RecoredSet
	var v1 RecoredSet
	v.m_record=make(map[string]*reportInfo)
	v1.m_record=make(map[string]*reportInfo)
	g_recored = append(g_recored,v)
	g_recored = append(g_recored,v1)
	rec_idx=0
	recordset_size=2
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

func fillTask(){
	//cmd := exec.Command("cat", "count.log.2015-09-23-10")
	cmd := exec.Command("cat", "dsp_pipe_count")
	stdout, _ := cmd.StdoutPipe()
	cmd.Start()
	inputReader := bufio.NewReader(stdout)
  for {
    inputString, readerError := inputReader.ReadString('\n')
    if readerError == io.EOF {
    				time.Sleep(1*time.Second)
            continue
    }
    task_ch[getCrc(inputString)%uint32(count)] <- inputString
  }
}

func processTask(idx int){
	for {
		str_log,ok := <- task_ch[idx]
		if ok==false {
			fmt.Println("task queue empty!")
		}
		Excute(str_log,idx)
	}
}

func Excute(str_log string,idx int) {
	str_log = strings.Replace(str_log, "\n", "", 1)
	str_log = strings.Replace(str_log, "\r", "", 1)
	var log_arr []string = strings.Split(str_log,sep_str)
	if len(log_arr)>2 {
			if (log_arr[1] == "2")&&(len(log_arr)>=43) {
				getad_process(&log_arr)
			}else if (log_arr[1] == "3")&&(len(log_arr)>=47) {
				view_process(&log_arr)
			}else if (log_arr[1] == "4")&&(len(log_arr)>=46) {
				click_process(&log_arr)
			}else if (log_arr[1] == "5")&&(len(log_arr)>=46) {
				winnotic_process(&log_arr)
			}else if (log_arr[1] == "6")&&(len(log_arr)>=9) {
				arrive_process(&log_arr)
			}
	}
}

func getDomain(strurl string) string{
		res:="-1"
    if(len(strurl)<=7){
    	return res
    }
    ipos := strings.Index(strurl, "http://")
    var str_para []byte
		if ipos >=0 {
			str_para = []byte(strurl)[ipos+7:]
		}
		xpos := strings.Index(string(str_para), "/")
		var str_para_x []byte
		if xpos >=0 {
			str_para_x = str_para[0:xpos]
		}
		res = string(str_para_x)
		return res
}

func getad_process(strarr *[]string){
	adid:=(*strarr)[3]
	oid :=(*strarr)[4]
	cid :=(*strarr)[40]
	advid :=(*strarr)[41]
	browser := (*strarr)[14]
	os := (*strarr)[15]
	if len(browser) <= 0{
		browser="0"
	}
	if len(os) <= 0 {
		os="0"
	}
	slotid := (*strarr)[16]
	if len(slotid) <= 0 {
		slotid="-1"
	}
	areaid := (*strarr)[11]
	domain := getDomain((*strarr)[19])
	esd := getMd5(slotid+"_"+domain)
	exchangeid := (*strarr)[9]
	//cs := (*strarr)[25]
	//channel := (*strarr)[24]
	media_type := (*strarr)[7]
	domain_category := getDomainCate(domain)
	if len(domain_category)<=0 {
		domain_category="0^0"
	}
	timestamp ,_ :=strconv.Atoi((*strarr)[2])
	str_today:=time.Unix(int64(timestamp),0).Format("2006-01-02")
	str_hour:=strconv.Itoa(time.Unix(int64(timestamp),0).Hour())
	
	var user_cate_list []string = strings.Split((*strarr)[39],",")
	bid_time,_ := strconv.Atoi((*strarr)[23])
	user_cate := "bh_0"
	user_gender := "bh_gd_10000"
	if len((*strarr)[39]) > 2 {
		user_cate = user_cate_list[bid_time%len(user_cate_list)]
		if strings.Contains(user_cate, "gd_") {
			user_gender = user_cate
			user_cate = "bh_0"
		}
	}
	if len(user_cate) <= 0 {
		user_cate= "bh_0"
		user_gender= "bh_gd_10000"
	}
	
	advcost ,_ :=strconv.Atoi((*strarr)[18])
	agentcost ,_ :=strconv.Atoi((*strarr)[17])
	realcost ,_ :=strconv.Atoi((*strarr)[12])
	algorithm_type :=(*strarr)[42]
	
	if(realcost>0) {
		var tmprec *reportInfo
		shift_g_recored_lock.RLock()
		//时间报表 小时-------------------
		tf_h_key:="TF^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+str_hour
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_h_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_h_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_h_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_h_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		tmprec.true_cost+=int64(realcost)
		tmprec.sys_cost+=int64(agentcost)
		tmprec.adv_cost+=int64(advcost)
		tmprec.m_lock.Unlock()
		//时间报表 日---------------------
		tf_d_key:="TF^"+str_today+"^"+advid+"^"+cid+"^"+oid
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_d_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_d_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_d_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_d_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		tmprec.true_cost+=int64(realcost)
		tmprec.sys_cost+=int64(agentcost)
		tmprec.adv_cost+=int64(advcost)
		tmprec.m_lock.Unlock()
		//浏览器报表-----------------------
		tf_br_key:="BROWER^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+browser
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_br_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_br_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_br_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_br_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		tmprec.true_cost+=int64(realcost)
		tmprec.sys_cost+=int64(agentcost)
		tmprec.adv_cost+=int64(advcost)
		tmprec.m_lock.Unlock()
		//操作系统报表-----------------------
		tf_os_key:="OPERA^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+os
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_os_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_os_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_os_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()

		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_os_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		tmprec.true_cost+=int64(realcost)
		tmprec.sys_cost+=int64(agentcost)
		tmprec.adv_cost+=int64(advcost)
		tmprec.m_lock.Unlock()
		//广告位报表-----------------------
		tf_esd_key:="MEDIAADID^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+esd+"^"+slotid+"^"+domain
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_esd_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_esd_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_esd_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_esd_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		tmprec.true_cost+=int64(realcost)
		tmprec.sys_cost+=int64(agentcost)
		tmprec.adv_cost+=int64(advcost)
		tmprec.m_lock.Unlock()
		//地域报表-----------------------
		tf_area_key:="PLACE^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+areaid
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_area_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_area_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_area_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_area_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		tmprec.true_cost+=int64(realcost)
		tmprec.sys_cost+=int64(agentcost)
		tmprec.adv_cost+=int64(advcost)
		tmprec.m_lock.Unlock()
		//域名报表-----------------------
		tf_domain_key:="MEDIA^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+domain
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_domain_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_domain_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_domain_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_domain_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		tmprec.true_cost+=int64(realcost)
		tmprec.sys_cost+=int64(agentcost)
		tmprec.adv_cost+=int64(advcost)
		tmprec.m_lock.Unlock()
		//ExchangeID报表-----------------------
		tf_exid_key:="EXANGE^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+exchangeid
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_exid_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_exid_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_exid_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_exid_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		tmprec.true_cost+=int64(realcost)
		tmprec.sys_cost+=int64(agentcost)
		tmprec.adv_cost+=int64(advcost)
		tmprec.m_lock.Unlock()
		//频道报表-----------------------
		/*tf_ch_key:="CHANNEL^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+channel+"^"+cs+"^"+exchangeid
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_ch_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_ch_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_ch_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_ch_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		tmprec.true_cost+=int64(realcost)
		tmprec.sys_cost+=int64(agentcost)
		tmprec.adv_cost+=int64(advcost)
		tmprec.m_lock.Unlock()*/
		//媒体分类报表-----------------------
		tf_mt_key:="CONTENT^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+media_type+"^"+exchangeid
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_mt_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_mt_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_mt_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_mt_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		tmprec.true_cost+=int64(realcost)
		tmprec.sys_cost+=int64(agentcost)
		tmprec.adv_cost+=int64(advcost)
		tmprec.m_lock.Unlock()
		//素材报表-----------------------
		tf_material_key:="MATERIAL^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+adid
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_material_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_material_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_material_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_material_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		tmprec.true_cost+=int64(realcost)
		tmprec.sys_cost+=int64(agentcost)
		tmprec.adv_cost+=int64(advcost)
		tmprec.m_lock.Unlock()
		//域名分类-------------------
		tf_dc_key:="DOMAINCATE^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+domain_category
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_dc_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_dc_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_dc_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_dc_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		tmprec.true_cost+=int64(realcost)
		tmprec.sys_cost+=int64(agentcost)
		tmprec.adv_cost+=int64(advcost)
		tmprec.m_lock.Unlock()
		//人群报表-------------------
		if len(user_cate) > 0 {
			tf_crowd_key:="CROWD^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+user_cate
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_crowd_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
			if(tmprec==nil){
				var x reportInfo
				g_recored[rec_idx].m_record_lock.Lock()
				tmprec=g_recored[rec_idx].m_record[tf_crowd_key]
				if(tmprec==nil){
					g_recored[rec_idx].m_record[tf_crowd_key]=&x
					tmprec=&x
				}
				g_recored[rec_idx].m_record_lock.Unlock()
			}else{
				g_recored[rec_idx].m_record_lock.RLock()
				tmprec=g_recored[rec_idx].m_record[tf_crowd_key]
				g_recored[rec_idx].m_record_lock.RUnlock()
			}
			tmprec.m_lock.Lock()
			tmprec.order_id=oid
			tmprec.camp_id=cid
			tmprec.advert_id=advid
			tmprec.algorithm_type=algorithm_type
			
			tmprec.true_cost+=int64(realcost)
			tmprec.sys_cost+=int64(agentcost)
			tmprec.adv_cost+=int64(advcost)
			tmprec.m_lock.Unlock()
		}
		//性别报表-------------------
		if len(user_gender) > 0 {
			tf_gender_key:="GENDER^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+user_gender
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_gender_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
			if(tmprec==nil){
				var x reportInfo
				g_recored[rec_idx].m_record_lock.Lock()
				tmprec=g_recored[rec_idx].m_record[tf_gender_key]
				if(tmprec==nil){
					g_recored[rec_idx].m_record[tf_gender_key]=&x
					tmprec=&x
				}
				g_recored[rec_idx].m_record_lock.Unlock()
			}else{
				g_recored[rec_idx].m_record_lock.RLock()
				tmprec=g_recored[rec_idx].m_record[tf_gender_key]
				g_recored[rec_idx].m_record_lock.RUnlock()
			}
			tmprec.m_lock.Lock()
			tmprec.order_id=oid
			tmprec.camp_id=cid
			tmprec.advert_id=advid
			tmprec.algorithm_type=algorithm_type
			
			tmprec.true_cost+=int64(realcost)
			tmprec.sys_cost+=int64(agentcost)
			tmprec.adv_cost+=int64(advcost)
			tmprec.m_lock.Unlock()
		}
		//---------------------------------
		shift_g_recored_lock.RUnlock()
	}
}

func view_process(strarr *[]string){
	adid:=(*strarr)[3]
	oid :=(*strarr)[4]
	cid :=(*strarr)[44]
	advid :=(*strarr)[45]
	browser := (*strarr)[12]
	os := (*strarr)[13]
	if len(browser) <= 0{
		browser="0"
	}
	if len(os) <= 0 {
		os="0"
	}
	slotid := (*strarr)[16]
	if len(slotid) <= 0 {
		slotid="-1"
	}
	areaid := (*strarr)[11]
	domain := getDomain((*strarr)[15])
	esd := getMd5(slotid+"_"+domain)
	exchangeid := (*strarr)[5]
	cs := (*strarr)[25]
	channel := (*strarr)[24]
	media_type := (*strarr)[8]
	domain_category := getDomainCate(domain)
	if len(domain_category)<=0 {
		domain_category="0^0"
	}
	timestamp ,_ :=strconv.Atoi((*strarr)[2])
	str_today:=time.Unix(int64(timestamp),0).Format("2006-01-02")
	str_hour:=strconv.Itoa(time.Unix(int64(timestamp),0).Hour())
	
	var user_cate_list []string = strings.Split((*strarr)[43],",")
	bid_time,_ := strconv.Atoi((*strarr)[23])
	user_cate := "bh_0"
	user_gender := "bh_gd_10000"
	if len((*strarr)[43]) > 2 {
		user_cate = user_cate_list[bid_time%len(user_cate_list)]
		if strings.Contains(user_cate, "gd_") {
			user_gender = user_cate
			user_cate = "bh_0"
		}
	}
	if len(user_cate) <= 0 {
		user_cate= "bh_0"
		user_gender= "bh_gd_10000"
	}
	
	advcost ,_ :=strconv.Atoi((*strarr)[20])
	agentcost ,_ :=strconv.Atoi((*strarr)[19])
	realcost ,_ :=strconv.Atoi((*strarr)[18])
	//algorithm_type ,_ :=strconv.Atoi((*strarr)[46])
	algorithm_type :=(*strarr)[46]
	
	
	{
		var tmprec *reportInfo
		shift_g_recored_lock.RLock()
		//时间报表 小时-------------------
		tf_h_key:="TF^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+str_hour
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_h_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_h_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_h_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_h_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		tmprec.true_cost+=int64(realcost)
		tmprec.sys_cost+=int64(agentcost)
		tmprec.adv_cost+=int64(advcost)
		tmprec.view_num++
		tmprec.m_lock.Unlock()
		//时间报表 日---------------------
		tf_d_key:="TF^"+str_today+"^"+advid+"^"+cid+"^"+oid
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_d_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_d_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_d_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_d_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		tmprec.true_cost+=int64(realcost)
		tmprec.sys_cost+=int64(agentcost)
		tmprec.adv_cost+=int64(advcost)
		tmprec.view_num++
		tmprec.m_lock.Unlock()
		//浏览器报表-----------------------
		tf_br_key:="BROWER^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+browser
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_br_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_br_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_br_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_br_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		tmprec.true_cost+=int64(realcost)
		tmprec.sys_cost+=int64(agentcost)
		tmprec.adv_cost+=int64(advcost)
		tmprec.view_num++
		tmprec.m_lock.Unlock()
		//操作系统报表-----------------------
		tf_os_key:="OPERA^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+os
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_os_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_os_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_os_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()

		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_os_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		tmprec.true_cost+=int64(realcost)
		tmprec.sys_cost+=int64(agentcost)
		tmprec.adv_cost+=int64(advcost)
		tmprec.view_num++
		tmprec.m_lock.Unlock()
		//广告位报表-----------------------
		tf_esd_key:="MEDIAADID^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+esd+"^"+slotid+"^"+domain
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_esd_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_esd_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_esd_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_esd_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		tmprec.true_cost+=int64(realcost)
		tmprec.sys_cost+=int64(agentcost)
		tmprec.adv_cost+=int64(advcost)
		tmprec.view_num++
		tmprec.m_lock.Unlock()
		//地域报表-----------------------
		tf_area_key:="PLACE^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+areaid
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_area_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_area_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_area_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_area_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		tmprec.true_cost+=int64(realcost)
		tmprec.sys_cost+=int64(agentcost)
		tmprec.adv_cost+=int64(advcost)
		tmprec.view_num++
		tmprec.m_lock.Unlock()
		//域名报表-----------------------
		tf_domain_key:="MEDIA^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+domain
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_domain_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_domain_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_domain_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_domain_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		tmprec.true_cost+=int64(realcost)
		tmprec.sys_cost+=int64(agentcost)
		tmprec.adv_cost+=int64(advcost)
		tmprec.view_num++
		tmprec.m_lock.Unlock()
		//ExchangeID报表-----------------------
		tf_exid_key:="EXANGE^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+exchangeid
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_exid_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_exid_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_exid_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_exid_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		tmprec.true_cost+=int64(realcost)
		tmprec.sys_cost+=int64(agentcost)
		tmprec.adv_cost+=int64(advcost)
		tmprec.view_num++
		tmprec.m_lock.Unlock()
		//频道报表-----------------------
		if(len(channel)>0||len(cs)>0){
			if len(channel)==0{
				channel="-1"
			}
			if len(cs)==0{
				cs="-1"
			}	
			tf_ch_key:="CHANNEL^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+channel+"^"+cs+"^"+exchangeid
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_ch_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
			if(tmprec==nil){
				var x reportInfo
				g_recored[rec_idx].m_record_lock.Lock()
				tmprec=g_recored[rec_idx].m_record[tf_ch_key]
				if(tmprec==nil){
					g_recored[rec_idx].m_record[tf_ch_key]=&x
					tmprec=&x
				}
				g_recored[rec_idx].m_record_lock.Unlock()
			}else{
				g_recored[rec_idx].m_record_lock.RLock()
				tmprec=g_recored[rec_idx].m_record[tf_ch_key]
				g_recored[rec_idx].m_record_lock.RUnlock()
			}
			tmprec.m_lock.Lock()
			tmprec.order_id=oid
			tmprec.camp_id=cid
			tmprec.advert_id=advid
			tmprec.algorithm_type=algorithm_type
			
			tmprec.true_cost+=int64(realcost)
			tmprec.sys_cost+=int64(agentcost)
			tmprec.adv_cost+=int64(advcost)
			tmprec.view_num++
			tmprec.m_lock.Unlock()
		}
		//媒体分类报表-----------------------
		tf_mt_key:="CONTENT^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+media_type+"^"+exchangeid
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_mt_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_mt_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_mt_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_mt_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		tmprec.true_cost+=int64(realcost)
		tmprec.sys_cost+=int64(agentcost)
		tmprec.adv_cost+=int64(advcost)
		tmprec.view_num++
		tmprec.m_lock.Unlock()
		//素材报表-----------------------
		tf_material_key:="MATERIAL^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+adid
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_material_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_material_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_material_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_material_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		tmprec.true_cost+=int64(realcost)
		tmprec.sys_cost+=int64(agentcost)
		tmprec.adv_cost+=int64(advcost)
		tmprec.view_num++
		tmprec.m_lock.Unlock()
		//域名分类-------------------
		tf_dc_key:="DOMAINCATE^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+domain_category
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_dc_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_dc_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_dc_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_dc_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		tmprec.true_cost+=int64(realcost)
		tmprec.sys_cost+=int64(agentcost)
		tmprec.adv_cost+=int64(advcost)
		tmprec.view_num++
		tmprec.m_lock.Unlock()
		//人群报表-------------------
		if len(user_cate) > 0 {
			tf_crowd_key:="CROWD^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+user_cate
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_crowd_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
			if(tmprec==nil){
				var x reportInfo
				g_recored[rec_idx].m_record_lock.Lock()
				tmprec=g_recored[rec_idx].m_record[tf_crowd_key]
				if(tmprec==nil){
					g_recored[rec_idx].m_record[tf_crowd_key]=&x
					tmprec=&x
				}
				g_recored[rec_idx].m_record_lock.Unlock()
			}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_crowd_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
			tmprec.m_lock.Lock()
			tmprec.order_id=oid
			tmprec.camp_id=cid
			tmprec.advert_id=advid
			tmprec.algorithm_type=algorithm_type
			
			tmprec.true_cost+=int64(realcost)
			tmprec.sys_cost+=int64(agentcost)
			tmprec.adv_cost+=int64(advcost)
			tmprec.view_num++
			tmprec.m_lock.Unlock()
		}
		//性别报表-------------------
		if len(user_gender) > 0 {
			tf_gender_key:="GENDER^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+user_gender
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_gender_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
			if(tmprec==nil){
				var x reportInfo
				g_recored[rec_idx].m_record_lock.Lock()
				tmprec=g_recored[rec_idx].m_record[tf_gender_key]
				if(tmprec==nil){
					g_recored[rec_idx].m_record[tf_gender_key]=&x
					tmprec=&x
				}
				g_recored[rec_idx].m_record_lock.Unlock()
			}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_gender_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
			tmprec.m_lock.Lock()
			tmprec.order_id=oid
			tmprec.camp_id=cid
			tmprec.advert_id=advid
			tmprec.algorithm_type=algorithm_type
			
			tmprec.true_cost+=int64(realcost)
			tmprec.sys_cost+=int64(agentcost)
			tmprec.adv_cost+=int64(advcost)
			tmprec.view_num++
			tmprec.m_lock.Unlock()
		}
		//---------------------------------
		shift_g_recored_lock.RUnlock()
	}
}

func winnotic_process(strarr *[]string){
	adid:=(*strarr)[3]
	oid :=(*strarr)[4]
	cid :=(*strarr)[43]
	advid :=(*strarr)[44]
	browser := (*strarr)[12]
	os := (*strarr)[13]
	if len(browser) <= 0{
		browser="0"
	}
	if len(os) <= 0 {
		os="0"
	}
	slotid := (*strarr)[16]
	if len(slotid) <= 0 {
		slotid="-1"
	}
	areaid := (*strarr)[11]
	domain := getDomain((*strarr)[15])
	esd := getMd5(slotid+"_"+domain)
	exchangeid := (*strarr)[5]
	cs := (*strarr)[25]
	channel := (*strarr)[24]
	media_type := (*strarr)[8]
	domain_category := getDomainCate(domain)
	if len(domain_category)<=0 {
		domain_category="0^0"
	}
	timestamp ,_ :=strconv.Atoi((*strarr)[2])
	str_today:=time.Unix(int64(timestamp),0).Format("2006-01-02")
	str_hour:=strconv.Itoa(time.Unix(int64(timestamp),0).Hour())
	
	var user_cate_list []string = strings.Split((*strarr)[42],",")
	bid_time,_ := strconv.Atoi((*strarr)[23])
	user_cate := "bh_0"
	user_gender := "bh_gd_10000"
	if len((*strarr)[42]) > 2 {
		user_cate = user_cate_list[bid_time%len(user_cate_list)]
		if strings.Contains(user_cate, "gd_") {
			user_gender = user_cate
			user_cate = "bh_0"
		}
	}
	if len(user_cate) <= 0 {
		user_cate= "bh_0"
		user_gender= "bh_gd_10000"
	}
	
	advcost ,_ :=strconv.Atoi((*strarr)[20])
	agentcost ,_ :=strconv.Atoi((*strarr)[19])
	realcost ,_ :=strconv.Atoi((*strarr)[18])
	//algorithm_type ,_ :=strconv.Atoi((*strarr)[46])
	algorithm_type :=(*strarr)[45]
	
	if(realcost>0) {
		var tmprec *reportInfo
		shift_g_recored_lock.RLock()
		//时间报表 小时-------------------
		tf_h_key:="TF^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+str_hour
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_h_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_h_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_h_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_h_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		tmprec.true_cost+=int64(realcost)
		tmprec.sys_cost+=int64(agentcost)
		tmprec.adv_cost+=int64(advcost)
		tmprec.m_lock.Unlock()
		//时间报表 日---------------------
		tf_d_key:="TF^"+str_today+"^"+advid+"^"+cid+"^"+oid
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_d_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_d_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_d_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_d_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		tmprec.true_cost+=int64(realcost)
		tmprec.sys_cost+=int64(agentcost)
		tmprec.adv_cost+=int64(advcost)
		tmprec.m_lock.Unlock()
		//浏览器报表-----------------------
		tf_br_key:="BROWER^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+browser
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_br_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_br_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_br_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_br_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		tmprec.true_cost+=int64(realcost)
		tmprec.sys_cost+=int64(agentcost)
		tmprec.adv_cost+=int64(advcost)
		tmprec.m_lock.Unlock()
		//操作系统报表-----------------------
		tf_os_key:="OPERA^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+os
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_os_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_os_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_os_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()

		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_os_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		tmprec.true_cost+=int64(realcost)
		tmprec.sys_cost+=int64(agentcost)
		tmprec.adv_cost+=int64(advcost)
		tmprec.m_lock.Unlock()
		//广告位报表-----------------------
		tf_esd_key:="MEDIAADID^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+esd+"^"+slotid+"^"+domain
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_esd_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_esd_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_esd_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_esd_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		tmprec.true_cost+=int64(realcost)
		tmprec.sys_cost+=int64(agentcost)
		tmprec.adv_cost+=int64(advcost)
		tmprec.m_lock.Unlock()
		//地域报表-----------------------
		tf_area_key:="PLACE^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+areaid
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_area_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_area_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_area_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_area_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		tmprec.true_cost+=int64(realcost)
		tmprec.sys_cost+=int64(agentcost)
		tmprec.adv_cost+=int64(advcost)
		tmprec.m_lock.Unlock()
		//域名报表-----------------------
		tf_domain_key:="MEDIA^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+domain
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_domain_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_domain_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_domain_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_domain_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		tmprec.true_cost+=int64(realcost)
		tmprec.sys_cost+=int64(agentcost)
		tmprec.adv_cost+=int64(advcost)
		tmprec.m_lock.Unlock()
		//ExchangeID报表-----------------------
		tf_exid_key:="EXANGE^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+exchangeid
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_exid_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_exid_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_exid_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_exid_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		tmprec.true_cost+=int64(realcost)
		tmprec.sys_cost+=int64(agentcost)
		tmprec.adv_cost+=int64(advcost)
		tmprec.m_lock.Unlock()
		//频道报表-----------------------
		if(len(channel)>0||len(cs)>0){
			if len(channel)==0{
				channel="-1"
			}
			if len(cs)==0{
				cs="-1"
			}	
			tf_ch_key:="CHANNEL^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+channel+"^"+cs+"^"+exchangeid
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_ch_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
			if(tmprec==nil){
				var x reportInfo
				g_recored[rec_idx].m_record_lock.Lock()
				tmprec=g_recored[rec_idx].m_record[tf_ch_key]
				if(tmprec==nil){
					g_recored[rec_idx].m_record[tf_ch_key]=&x
					tmprec=&x
				}
				g_recored[rec_idx].m_record_lock.Unlock()
			}else{
				g_recored[rec_idx].m_record_lock.RLock()
				tmprec=g_recored[rec_idx].m_record[tf_ch_key]
				g_recored[rec_idx].m_record_lock.RUnlock()
			}
			tmprec.m_lock.Lock()
			tmprec.order_id=oid
			tmprec.camp_id=cid
			tmprec.advert_id=advid
			tmprec.algorithm_type=algorithm_type
			
			tmprec.true_cost+=int64(realcost)
			tmprec.sys_cost+=int64(agentcost)
			tmprec.adv_cost+=int64(advcost)
			tmprec.m_lock.Unlock()
		}
		//媒体分类报表-----------------------
		tf_mt_key:="CONTENT^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+media_type+"^"+exchangeid
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_mt_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_mt_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_mt_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_mt_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		tmprec.true_cost+=int64(realcost)
		tmprec.sys_cost+=int64(agentcost)
		tmprec.adv_cost+=int64(advcost)
		tmprec.m_lock.Unlock()
		//素材报表-----------------------
		tf_material_key:="MATERIAL^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+adid
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_material_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_material_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_material_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_material_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		tmprec.true_cost+=int64(realcost)
		tmprec.sys_cost+=int64(agentcost)
		tmprec.adv_cost+=int64(advcost)
		tmprec.m_lock.Unlock()
		//域名分类-------------------
		tf_dc_key:="DOMAINCATE^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+domain_category
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_dc_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_dc_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_dc_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_dc_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		tmprec.true_cost+=int64(realcost)
		tmprec.sys_cost+=int64(agentcost)
		tmprec.adv_cost+=int64(advcost)
		tmprec.m_lock.Unlock()
		//人群报表-------------------
		if len(user_cate) > 0 {
			tf_crowd_key:="CROWD^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+user_cate
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_crowd_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
			if(tmprec==nil){
				var x reportInfo
				g_recored[rec_idx].m_record_lock.Lock()
				tmprec=g_recored[rec_idx].m_record[tf_crowd_key]
				if(tmprec==nil){
					g_recored[rec_idx].m_record[tf_crowd_key]=&x
					tmprec=&x
				}
				g_recored[rec_idx].m_record_lock.Unlock()
			}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_crowd_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
			tmprec.m_lock.Lock()
			tmprec.order_id=oid
			tmprec.camp_id=cid
			tmprec.advert_id=advid
			tmprec.algorithm_type=algorithm_type
			
			tmprec.true_cost+=int64(realcost)
			tmprec.sys_cost+=int64(agentcost)
			tmprec.adv_cost+=int64(advcost)
			tmprec.m_lock.Unlock()
		}
		//性别报表-------------------
		if len(user_gender) > 0 {
			tf_gender_key:="GENDER^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+user_gender
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_gender_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
			if(tmprec==nil){
				var x reportInfo
				g_recored[rec_idx].m_record_lock.Lock()
				tmprec=g_recored[rec_idx].m_record[tf_gender_key]
				if(tmprec==nil){
					g_recored[rec_idx].m_record[tf_gender_key]=&x
					tmprec=&x
				}
				g_recored[rec_idx].m_record_lock.Unlock()
			}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_gender_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
			tmprec.m_lock.Lock()
			tmprec.order_id=oid
			tmprec.camp_id=cid
			tmprec.advert_id=advid
			tmprec.algorithm_type=algorithm_type
			
			tmprec.true_cost+=int64(realcost)
			tmprec.sys_cost+=int64(agentcost)
			tmprec.adv_cost+=int64(advcost)
			tmprec.m_lock.Unlock()
		}
		//---------------------------------
		shift_g_recored_lock.RUnlock()
	}
}

func record_Click(bhuid string,order_id string,clickinfo string) int {
	if len(bhuid) <=0 {
		return 0
	}
	redis_conn_click := pools_redis_click[0].Get()
	defer redis_conn_click.Close()
	res,err:=redis.Int(redis_conn_click.Do("HSETNX",bhuid,order_id,clickinfo))
	if err!=nil {
		fmt.Println("ErrHs:",res,"HSETNX",bhuid,order_id,clickinfo)
	}
	//fmt.Println("HSET",bhuid,order_id,clickinfo)
	now_time := time.Now().Unix()
	today := (now_time+28800)/86400*86400-28800
	res_exp,err:=redis_conn_click.Do("EXPIRE", bhuid, today+86399)
	if err!=nil {
		fmt.Println("ErrHs:",res_exp,"EXPIRE", bhuid, today+86399)
	}
	return res
}

func click_process(strarr *[]string){
	adid:=(*strarr)[3]
	oid :=(*strarr)[4]
	cid :=(*strarr)[42]
	advid :=(*strarr)[43]
	browser := (*strarr)[12]
	os := (*strarr)[13]
	if len(browser) <= 0{
		browser="0"
	}
	if len(os) <= 0 {
		os="0"
	}
	slotid := (*strarr)[17]
	if len(slotid) <= 0 {
		slotid="-1"
	}
	areaid := (*strarr)[11]
	domain := getDomain((*strarr)[15])
	esd := getMd5(slotid+"_"+domain)
	exchangeid := (*strarr)[5]
	cs := (*strarr)[21]
	channel := (*strarr)[20]
	media_type := (*strarr)[8]
	click_time := (*strarr)[2]
	timestamp ,_ :=strconv.Atoi((*strarr)[2])
	str_today:=time.Unix(int64(timestamp),0).Format("2006-01-02")
	str_hour:=strconv.Itoa(time.Unix(int64(timestamp),0).Hour())
	
	var user_cate_list []string = strings.Split((*strarr)[41],",")
	bid_time,_ := strconv.Atoi((*strarr)[19])
	user_cate := "bh_0"
	user_gender := "bh_gd_10000"
	if len((*strarr)[41]) > 2 {
		user_cate = user_cate_list[bid_time%len(user_cate_list)]
		if strings.Contains(user_cate, "gd_") {
			user_gender = user_cate
			user_cate = "bh_0"
		}
	}
	if len(user_cate) <= 0 {
		user_cate= "bh_0"
		user_gender= "bh_gd_10000"
	}
	
	bhuid:=(*strarr)[7]
	clickcost ,_ :=strconv.Atoi((*strarr)[45])
	//algorithm_type ,_ :=strconv.Atoi((*strarr)[44])
	algorithm_type :=(*strarr)[44]
	domain_category := getDomainCate(domain)
	if len(domain_category)<=0 {
		domain_category="0^0"
	}
	click_info := adid+"|"+oid+"|"+cid+"|"+advid+"|"+browser+"|"+os+"|"+slotid+"|"+areaid+"|"+domain+"|"+esd+"|"+exchangeid+"|"+cs+"|"+channel+"|"+media_type+"|"+str_today+"|"+str_hour+"|"+click_time+"|"+algorithm_type+":"+user_cate
	isuser_click:=record_Click(bhuid,oid,click_info)
	
	{
		var tmprec *reportInfo
		shift_g_recored_lock.RLock()
		//时间报表 小时-------------------
		tf_h_key:="TF^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+str_hour
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_h_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_h_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_h_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_h_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		tmprec.click_cost+=int64(clickcost)
		tmprec.click_num++
		if isuser_click==1 {
			tmprec.user_click_num++
		}
		tmprec.m_lock.Unlock()
		//时间报表 日---------------------
		tf_d_key:="TF^"+str_today+"^"+advid+"^"+cid+"^"+oid
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_d_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_d_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_d_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_d_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		tmprec.click_cost+=int64(clickcost)
		tmprec.click_num++
		if isuser_click==1 {
			tmprec.user_click_num++
		}
		tmprec.m_lock.Unlock()
		//浏览器报表-----------------------
		tf_br_key:="BROWER^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+browser
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_br_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_br_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_br_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_br_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		tmprec.click_cost+=int64(clickcost)
		tmprec.click_num++
		if isuser_click==1 {
			tmprec.user_click_num++
		}
		tmprec.m_lock.Unlock()
		//操作系统报表-----------------------
		tf_os_key:="OPERA^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+os
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_os_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_os_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_os_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()

		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_os_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		tmprec.click_cost+=int64(clickcost)
		tmprec.click_num++
		if isuser_click==1 {
			tmprec.user_click_num++
		}
		tmprec.m_lock.Unlock()
		//广告位报表-----------------------
		tf_esd_key:="MEDIAADID^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+esd+"^"+slotid+"^"+domain
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_esd_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_esd_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_esd_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_esd_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		tmprec.click_cost+=int64(clickcost)
		tmprec.click_num++
		if isuser_click==1 {
			tmprec.user_click_num++
		}
		tmprec.m_lock.Unlock()
		//地域报表-----------------------
		tf_area_key:="PLACE^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+areaid
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_area_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_area_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_area_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_area_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		tmprec.click_cost+=int64(clickcost)
		tmprec.click_num++
		if isuser_click==1 {
			tmprec.user_click_num++
		}
		tmprec.m_lock.Unlock()
		//域名报表-----------------------
		tf_domain_key:="MEDIA^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+domain
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_domain_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_domain_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_domain_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_domain_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		tmprec.click_cost+=int64(clickcost)
		tmprec.click_num++
		if isuser_click==1 {
			tmprec.user_click_num++
		}
		tmprec.m_lock.Unlock()
		//ExchangeID报表-----------------------
		tf_exid_key:="EXANGE^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+exchangeid
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_exid_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_exid_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_exid_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_exid_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		tmprec.click_cost+=int64(clickcost)
		tmprec.click_num++
		if isuser_click==1 {
			tmprec.user_click_num++
		}
		tmprec.m_lock.Unlock()
		//频道报表-----------------------
		if(len(channel)>0||len(cs)>0){
			if len(channel)==0{
				channel="-1"
			}
			if len(cs)==0{
				cs="-1"
			}	
			tf_ch_key:="CHANNEL^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+channel+"^"+cs+"^"+exchangeid
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_ch_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
			if(tmprec==nil){
				var x reportInfo
				g_recored[rec_idx].m_record_lock.Lock()
				tmprec=g_recored[rec_idx].m_record[tf_ch_key]
				if(tmprec==nil){
					g_recored[rec_idx].m_record[tf_ch_key]=&x
					tmprec=&x
				}
				g_recored[rec_idx].m_record_lock.Unlock()
			}else{
				g_recored[rec_idx].m_record_lock.RLock()
				tmprec=g_recored[rec_idx].m_record[tf_ch_key]
				g_recored[rec_idx].m_record_lock.RUnlock()
			}
			tmprec.m_lock.Lock()
			tmprec.order_id=oid
			tmprec.camp_id=cid
			tmprec.advert_id=advid
			tmprec.algorithm_type=algorithm_type
			
			tmprec.click_cost+=int64(clickcost)
			tmprec.click_num++
			if isuser_click==1 {
				tmprec.user_click_num++
			}
			tmprec.m_lock.Unlock()
		}
		//媒体分类报表-----------------------
		tf_mt_key:="CONTENT^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+media_type+"^"+exchangeid
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_mt_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_mt_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_mt_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_mt_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		tmprec.click_cost+=int64(clickcost)
		tmprec.click_num++
		if isuser_click==1 {
			tmprec.user_click_num++
		}
		tmprec.m_lock.Unlock()
		//素材报表-----------------------
		tf_material_key:="MATERIAL^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+adid
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_material_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_material_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_material_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_material_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		tmprec.click_cost+=int64(clickcost)
		tmprec.click_num++
		if isuser_click==1 {
			tmprec.user_click_num++
		}
		tmprec.m_lock.Unlock()
		//域名分类-------------------
		tf_dc_key:="DOMAINCATE^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+domain_category
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_dc_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_dc_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_dc_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_dc_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		tmprec.click_cost+=int64(clickcost)
		tmprec.click_num++
		if isuser_click==1 {
			tmprec.user_click_num++
		}
		tmprec.m_lock.Unlock()
		//人群报表-------------------
		if len(user_cate) > 0 {
			tf_crowd_key:="CROWD^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+user_cate
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_crowd_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
			if(tmprec==nil){
				var x reportInfo
				g_recored[rec_idx].m_record_lock.Lock()
				tmprec=g_recored[rec_idx].m_record[tf_crowd_key]
				if(tmprec==nil){
					g_recored[rec_idx].m_record[tf_crowd_key]=&x
					tmprec=&x
				}
				g_recored[rec_idx].m_record_lock.Unlock()
			}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_crowd_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
			tmprec.m_lock.Lock()
			tmprec.order_id=oid
			tmprec.camp_id=cid
			tmprec.advert_id=advid
			tmprec.algorithm_type=algorithm_type
			
			tmprec.click_cost+=int64(clickcost)
			tmprec.click_num++
			if isuser_click==1 {
				tmprec.user_click_num++
			}
			tmprec.m_lock.Unlock()
		}
		//性别报表-------------------
		if len(user_gender) > 0 {
			tf_gender_key:="GENDER^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+user_gender
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_gender_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
			if(tmprec==nil){
				var x reportInfo
				g_recored[rec_idx].m_record_lock.Lock()
				tmprec=g_recored[rec_idx].m_record[tf_gender_key]
				if(tmprec==nil){
					g_recored[rec_idx].m_record[tf_gender_key]=&x
					tmprec=&x
				}
				g_recored[rec_idx].m_record_lock.Unlock()
			}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_gender_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
			tmprec.m_lock.Lock()
			tmprec.order_id=oid
			tmprec.camp_id=cid
			tmprec.advert_id=advid
			tmprec.algorithm_type=algorithm_type
			
			tmprec.click_cost+=int64(clickcost)
			tmprec.click_num++
			if isuser_click==1 {
				tmprec.user_click_num++
			}
			tmprec.m_lock.Unlock()
		}
		//---------------------------------
		shift_g_recored_lock.RUnlock()
	}
}

func checkArrive(bhuid string, sid string) (bool,string,bool) {
	if len(bhuid) <=0 {
		return false, "", false
	}
	if len(sid) <=0 {
		return false, "", false
	}
	redis_conn_click := pools_redis_click[0].Get()
	defer redis_conn_click.Close()
	click_rec,err:=redis.StringMap(redis_conn_click.Do("HGETALL",bhuid))
	if err!=nil {
		fmt.Println("checkArrive Err:",click_rec,"HGETALL",bhuid)
		return false, "", false
	}
	//fmt.Println("HGETALL",bhuid,",click_rec:",click_rec,",err:",err)
	if len(click_rec) <= 0 {
		return false, "", false
	}
	
	
	res:=false
	res_str:=""
	click_time := int64(0)
	is_user_arrive := true
	
	redis_conn_sid := pools_redis_sid[0].Get()
	defer redis_conn_sid.Close()
	oidlist,err:=redis.String(redis_conn_sid.Do("GET",sid))
	if err!=nil {
		fmt.Println("checkArrive Err:",oidlist,"GET",sid)
		return res, res_str, is_user_arrive
	}
	//fmt.Println("SID:",sid,",olist:",oidlist)
	target_oid := "0"
	for oid, oidinfo := range click_rec {
  		if strings.Contains(oidlist, oid) {
  			var info_arr []string = strings.Split(oidinfo,"|")
  			if len(info_arr) >=18{
  				tmp_time,_ :=strconv.Atoi(info_arr[16])
  				if int64(tmp_time) > click_time{
  					target_oid=oid
  					click_time=int64(tmp_time)
  					res_str=oidinfo
  					res=true
  					if len(info_arr) == 19 {
  						is_user_arrive = false
  					}
  				}
  			}
  		}
  }
  if res==true {
  	now_time := time.Now().Unix()
		today := (now_time+28800)/86400*86400-28800
		redis_conn_click.Do("HSET",bhuid,target_oid,res_str+"|1")
		redis_conn_click.Do("EXPIRE", bhuid, today+86399)
  }
	return res, res_str, is_user_arrive
}

func calculateAliveTime(bhuid string, oid string, sessionid string, now_time int64) int64 {
	redis_conn_click := pools_redis_click[0].Get()
	defer redis_conn_click.Close()
	
	res:=int64(0)
	last_session_time, err:=redis.Int64(redis_conn_click.Do("HGET","alive_"+bhuid,(oid+"_"+sessionid)))
	if err!=nil {
		fmt.Println("ErrHs:",last_session_time,"HGET","alive_"+bhuid,(oid+"_"+sessionid))
	}
	if last_session_time > 0 {
		res = now_time - last_session_time
		if res < 0 {
			res = 0
		}
	}
	redis_conn_click.Do("HSET","alive_"+bhuid,(oid+"_"+sessionid),now_time)
	redis_conn_click.Do("EXPIRE", "alive_"+bhuid, 7200)
	return res
}

func arrive_process(strarr *[]string){
	bhuid:=(*strarr)[5]
	uri:=(*strarr)[6]
	timestamp ,_ :=strconv.Atoi((*strarr)[2])
	prara_map := make(map[string]string)
	paraURI(uri,&prara_map)
	sid:=prara_map["si"]
	sessionid:=prara_map["se"]
	is_two_jump:=false
	isok,str_info,is_user_arrive := checkArrive(bhuid, sid)
	isarrive:=true
	if strings.Contains(sid, "."){
		isarrive=false
	}
	
	//two_jump_num
	if isok == true {
		var info_arr []string = strings.Split(str_info,"|")
		if len(info_arr)<18 {
			return
		}
		var session_id_arr []string = strings.Split(sessionid,"_")
		if len(session_id_arr) == 2 {
			i_sn ,_ :=strconv.Atoi(session_id_arr[1])
			if i_sn == 1 {
				is_two_jump=true
			}
		}
		//------------------
		adid:=info_arr[0]
		oid :=info_arr[1]
		cid :=info_arr[2]
		advid :=info_arr[3]
		browser := info_arr[4]
		os := info_arr[5]
		slotid := info_arr[6]
		areaid := info_arr[7]
		domain := info_arr[8]
		esd := info_arr[9]
		exchangeid := info_arr[10]
		cs := info_arr[11]
		channel := info_arr[12]
		media_type := info_arr[13]
		str_today:=info_arr[14]
		str_hour:=info_arr[15]
		algorithm_type:=info_arr[17]
		user_cate:="bh_0"
		user_gender:= "bh_gd_10000"
		alive_time:=int64(0)
		var tmp_arr []string = strings.Split(info_arr[17],":")
		if len(tmp_arr)>=2 {
			algorithm_type=tmp_arr[0]
			user_cate=tmp_arr[1]
			if strings.Contains(user_cate, "gd_") {
				user_gender = user_cate
				user_cate = "bh_0"
			}
		}
		if len(user_cate) <= 0{
			user_cate="bh_0"
			user_gender= "bh_gd_10000"
		}
		if (isarrive==true&&len(session_id_arr)==2) {
			alive_time=calculateAliveTime(bhuid, oid, session_id_arr[0], int64(timestamp))
		}
		domain_category := getDomainCate(domain)
		if len(domain_category)<=0 {
			domain_category="0^0"
		}
		//------------------
		var tmprec *reportInfo
		shift_g_recored_lock.RLock()
		//时间报表 小时-------------------
		tf_h_key:="TF^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+str_hour
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_h_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_h_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_h_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_h_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		if isarrive == true {
			tmprec.arrive_num++
			if is_user_arrive == true {
				tmprec.user_arrive_num++
			}
		}else{
			tmprec.action_num++
		}
		tmprec.m_lock.Unlock()
		//时间报表 日---------------------
		tf_d_key:="TF^"+str_today+"^"+advid+"^"+cid+"^"+oid
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_d_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_d_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_d_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_d_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		if isarrive == true {
			tmprec.arrive_num++
			if is_user_arrive == true {
				tmprec.user_arrive_num++
			}
		}else{
			tmprec.action_num++
		}
		tmprec.m_lock.Unlock()
		//浏览器报表-----------------------
		tf_br_key:="BROWER^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+browser
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_br_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_br_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_br_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_br_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		if isarrive == true {
			tmprec.arrive_num++
			if is_user_arrive == true {
				tmprec.user_arrive_num++
			}
		}else{
			tmprec.action_num++
		}
		tmprec.m_lock.Unlock()
		//操作系统报表-----------------------
		tf_os_key:="OPERA^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+os
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_os_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_os_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_os_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()

		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_os_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		if isarrive == true {
			tmprec.arrive_num++
			if is_user_arrive == true {
				tmprec.user_arrive_num++
			}
		}else{
			tmprec.action_num++
		}
		tmprec.m_lock.Unlock()
		//广告位报表-----------------------
		tf_esd_key:="MEDIAADID^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+esd+"^"+slotid+"^"+domain
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_esd_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_esd_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_esd_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_esd_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		if isarrive == true {
			tmprec.arrive_num++
			if is_user_arrive == true {
				tmprec.user_arrive_num++
			}
			if is_two_jump == true {
				tmprec.two_jump_num++
				tmprec.alive_num++
			}
			tmprec.alive_num+=alive_time
		}else{
			tmprec.action_num++
		}
		tmprec.m_lock.Unlock()
		//地域报表-----------------------
		tf_area_key:="PLACE^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+areaid
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_area_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_area_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_area_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_area_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		if isarrive == true {
			tmprec.arrive_num++
			if is_user_arrive == true {
				tmprec.user_arrive_num++
			}
		}else{
			tmprec.action_num++
		}
		tmprec.m_lock.Unlock()
		//域名报表-----------------------
		tf_domain_key:="MEDIA^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+domain
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_domain_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_domain_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_domain_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_domain_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		if isarrive == true {
			tmprec.arrive_num++
			if is_user_arrive == true {
				tmprec.user_arrive_num++
			}
			if is_two_jump == true {
				tmprec.two_jump_num++
				tmprec.alive_num++
			}
			tmprec.alive_num+=alive_time
		}else{
			tmprec.action_num++
		}
		tmprec.m_lock.Unlock()
		//ExchangeID报表-----------------------
		tf_exid_key:="EXANGE^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+exchangeid
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_exid_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_exid_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_exid_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_exid_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		if isarrive == true {
			tmprec.arrive_num++
			if is_user_arrive == true {
				tmprec.user_arrive_num++
			}
		}else{
			tmprec.action_num++
		}
		tmprec.m_lock.Unlock()
		//频道报表-----------------------
		if(len(channel)>0||len(cs)>0){
			if len(channel)==0{
				channel="-1"
			}
			if len(cs)==0{
				cs="-1"
			}	
			tf_ch_key:="CHANNEL^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+channel+"^"+cs+"^"+exchangeid
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_ch_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
			if(tmprec==nil){
				var x reportInfo
				g_recored[rec_idx].m_record_lock.Lock()
				tmprec=g_recored[rec_idx].m_record[tf_ch_key]
				if(tmprec==nil){
					g_recored[rec_idx].m_record[tf_ch_key]=&x
					tmprec=&x
				}
				g_recored[rec_idx].m_record_lock.Unlock()
			}else{
				g_recored[rec_idx].m_record_lock.RLock()
				tmprec=g_recored[rec_idx].m_record[tf_ch_key]
				g_recored[rec_idx].m_record_lock.RUnlock()
			}
			tmprec.m_lock.Lock()
			tmprec.order_id=oid
			tmprec.camp_id=cid
			tmprec.advert_id=advid
			tmprec.algorithm_type=algorithm_type
			
			if isarrive == true {
				tmprec.arrive_num++
				if is_user_arrive == true {
					tmprec.user_arrive_num++
				}
			}else{
				tmprec.action_num++
			}
			tmprec.m_lock.Unlock()
		}
		//媒体分类报表-----------------------
		tf_mt_key:="CONTENT^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+media_type+"^"+exchangeid
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_mt_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_mt_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_mt_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_mt_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		if isarrive == true {
			tmprec.arrive_num++
			if is_user_arrive == true {
				tmprec.user_arrive_num++
			}
		}else{
			tmprec.action_num++
		}
		tmprec.m_lock.Unlock()
		//素材报表-----------------------
		tf_material_key:="MATERIAL^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+adid
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_material_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_material_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_material_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_material_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		if isarrive == true {
			tmprec.arrive_num++
			if is_user_arrive == true {
				tmprec.user_arrive_num++
			}
		}else{
			tmprec.action_num++
		}
		tmprec.m_lock.Unlock()
		//域名分类-------------------
		tf_dc_key:="DOMAINCATE^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+domain_category
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec=g_recored[rec_idx].m_record[tf_dc_key]
		g_recored[rec_idx].m_record_lock.RUnlock()
		if(tmprec==nil){
			var x reportInfo
			g_recored[rec_idx].m_record_lock.Lock()
			tmprec=g_recored[rec_idx].m_record[tf_dc_key]
			if(tmprec==nil){
				g_recored[rec_idx].m_record[tf_dc_key]=&x
				tmprec=&x
			}
			g_recored[rec_idx].m_record_lock.Unlock()
		}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_dc_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
		tmprec.m_lock.Lock()
		tmprec.order_id=oid
		tmprec.camp_id=cid
		tmprec.advert_id=advid
		tmprec.algorithm_type=algorithm_type
		
		if isarrive == true {
			tmprec.arrive_num++
			if is_user_arrive == true {
				tmprec.user_arrive_num++
			}
		}else{
			tmprec.action_num++
		}
		tmprec.m_lock.Unlock()
		//人群报表-------------------
		if len(user_cate) > 0 {
			tf_crowd_key:="CROWD^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+user_cate
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_crowd_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
			if(tmprec==nil){
				var x reportInfo
				g_recored[rec_idx].m_record_lock.Lock()
				tmprec=g_recored[rec_idx].m_record[tf_crowd_key]
				if(tmprec==nil){
					g_recored[rec_idx].m_record[tf_crowd_key]=&x
					tmprec=&x
				}
				g_recored[rec_idx].m_record_lock.Unlock()
			}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_crowd_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
			tmprec.m_lock.Lock()
			tmprec.order_id=oid
			tmprec.camp_id=cid
			tmprec.advert_id=advid
			tmprec.algorithm_type=algorithm_type
			
			if isarrive == true {
				tmprec.arrive_num++
				if is_user_arrive == true {
					tmprec.user_arrive_num++
				}
			}else{
				tmprec.action_num++
			}
			tmprec.m_lock.Unlock()
		}
		//性别报表-------------------
		if len(user_gender) > 0 {
			tf_gender_key:="GENDER^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+user_gender
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_gender_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
			if(tmprec==nil){
				var x reportInfo
				g_recored[rec_idx].m_record_lock.Lock()
				tmprec=g_recored[rec_idx].m_record[tf_gender_key]
				if(tmprec==nil){
					g_recored[rec_idx].m_record[tf_gender_key]=&x
					tmprec=&x
				}
				g_recored[rec_idx].m_record_lock.Unlock()
			}else{
			g_recored[rec_idx].m_record_lock.RLock()
			tmprec=g_recored[rec_idx].m_record[tf_gender_key]
			g_recored[rec_idx].m_record_lock.RUnlock()
		}
			tmprec.m_lock.Lock()
			tmprec.order_id=oid
			tmprec.camp_id=cid
			tmprec.advert_id=advid
			tmprec.algorithm_type=algorithm_type
			
			if isarrive == true {
				tmprec.arrive_num++
				if is_user_arrive == true {
					tmprec.user_arrive_num++
				}
			}else{
				tmprec.action_num++
			}
			tmprec.m_lock.Unlock()
		}
		//---------------------------------
		shift_g_recored_lock.RUnlock()
	}
}

func updateRecord() {
	for{
			time.Sleep(120*time.Second)
			old_rec_idx := rec_idx
			shift_g_recored_lock.Lock()
			if(rec_idx==0){
				rec_idx=1
			}else{
				rec_idx=0
			}
			shift_g_recored_lock.Unlock()	
			updateMap2Redis(old_rec_idx)
			
	}
}

func updateMap2Redis(idx int){
	//m_record map[string]*reportInfo
	redis_conn := pools_redis[idx].Get()
	defer redis_conn.Close()
	for redis_key,rpinfo := range g_recored[idx].m_record {
		/*cmd := "hmset "+redis_key+" "
		cmd+="order_id "+rpinfo.order_id
		cmd+=" campid "+rpinfo.camp_id
		cmd+=" adAccountName "+rpinfo.advert_id
		cmd+=" algorithm_type "+rpinfo.algorithm_type
		fmt.Println(cmd)*/
		res,err:=redis_conn.Do("HMSET", redis_key,"order_id",rpinfo.order_id,"campid",rpinfo.camp_id,"adAccountName",rpinfo.advert_id,"algorithm_type",rpinfo.algorithm_type)
		if err!=nil {
			fmt.Println("ErrHs:",res,"HMSET", redis_key,"order_id",rpinfo.order_id,"campid",rpinfo.camp_id,"adAccountName",rpinfo.advert_id,"algorithm_type",rpinfo.algorithm_type)
		}
		res,err=redis_conn.Do("EXPIRE", redis_key, 172800)
		if err!=nil {
			fmt.Println("ErrHs:",res,"EXPIRE", redis_key, 172800)
		}
		/*cmd = "HINCRBY "+redis_key+" "
		fmt.Println(cmd,"view",rpinfo.view_num)
		fmt.Println(cmd,"click",rpinfo.click_num)
		fmt.Println(cmd,"arrive",rpinfo.arrive_num)
		fmt.Println(cmd,"action",rpinfo.action_num)
		fmt.Println(cmd,"realprice",rpinfo.true_cost)
		fmt.Println(cmd,"private_price",rpinfo.sys_cost)
		fmt.Println(cmd,"open_price",rpinfo.adv_cost)*/
		
		if(rpinfo.view_num>0){
			res,err=redis_conn.Do("HINCRBY",redis_key,"view",rpinfo.view_num)
			if err!=nil {
				fmt.Println("ErrInc:",res,"HINCRBY",redis_key,"view",rpinfo.view_num)
			}
		}
		if(rpinfo.click_num>0){
			res,err=redis_conn.Do("HINCRBY",redis_key,"click",rpinfo.click_num)
			if err!=nil {
				fmt.Println("ErrInc:",res,"HINCRBY",redis_key,"click",rpinfo.click_num)
			}
		}
		if(rpinfo.arrive_num>0){
			res,err=redis_conn.Do("HINCRBY",redis_key,"arrive",rpinfo.arrive_num)
			if err!=nil {
				fmt.Println("ErrInc:",res,"HINCRBY",redis_key,"arrive",rpinfo.arrive_num)
			}
		}
		if(rpinfo.action_num>0){
			res,err=redis_conn.Do("HINCRBY",redis_key,"action",rpinfo.action_num)
			if err!=nil {
				fmt.Println("ErrInc:",res,"HINCRBY",redis_key,"action",rpinfo.action_num)
			}
		}
		if(rpinfo.true_cost>0){
			res,err=redis_conn.Do("HINCRBY",redis_key,"realprice",rpinfo.true_cost)
			if err!=nil {
				fmt.Println("ErrInc:",res,"HINCRBY",redis_key,"realprice",rpinfo.true_cost)
			}
		}
		if(rpinfo.sys_cost>0){
			res,err=redis_conn.Do("HINCRBY",redis_key,"private_price",rpinfo.sys_cost)
			if err!=nil {
				fmt.Println("ErrInc:",res,"HINCRBY",redis_key,"private_price",rpinfo.sys_cost)
			}
		}
		if(rpinfo.adv_cost>0){
			res,err=redis_conn.Do("HINCRBY",redis_key,"open_price",rpinfo.adv_cost)
			if err!=nil {
				fmt.Println("ErrInc:",res,"HINCRBY",redis_key,"open_price",rpinfo.adv_cost)
			}
		}
		if(rpinfo.click_cost>0){
			res,err=redis_conn.Do("HINCRBY",redis_key,"click_price",rpinfo.click_cost)
			if err!=nil {
				fmt.Println("ErrInc:",res,"HINCRBY",redis_key,"click_price",rpinfo.click_cost)
			}
		}
		if(rpinfo.user_click_num>0){
			res,err=redis_conn.Do("HINCRBY",redis_key,"userClick",rpinfo.user_click_num)
			if err!=nil {
				fmt.Println("ErrInc:",res,"HINCRBY",redis_key,"userClick",rpinfo.user_click_num)
			}
		}
		if(rpinfo.user_arrive_num>0){
			res,err=redis_conn.Do("HINCRBY",redis_key,"userReach",rpinfo.user_arrive_num)
			if err!=nil {
				fmt.Println("ErrInc:",res,"HINCRBY",redis_key,"userReach",rpinfo.user_arrive_num)
			}
		}
		if(rpinfo.two_jump_num>0&&(strings.Contains(redis_key,"MEDIAADID")||strings.Contains(redis_key,"MEDIA"))){
			res,err=redis_conn.Do("HINCRBY",redis_key,"twojumpnum",rpinfo.two_jump_num)
			if err!=nil {
				fmt.Println("ErrInc:",res,"HINCRBY",redis_key,"twojumpnum",rpinfo.two_jump_num)
			}
		}
		if(rpinfo.alive_time>0&&(strings.Contains(redis_key,"MEDIAADID")||strings.Contains(redis_key,"MEDIA"))){
			res,err=redis_conn.Do("HINCRBY",redis_key,"alive_time",rpinfo.alive_time)
			if err!=nil {
				fmt.Println("ErrInc:",res,"HINCRBY",redis_key,"alive_time",rpinfo.alive_time)
			}
		}
		if(rpinfo.alive_num>0&&(strings.Contains(redis_key,"MEDIAADID")||strings.Contains(redis_key,"MEDIA"))){
			res,err=redis_conn.Do("HINCRBY",redis_key,"alive_num",rpinfo.alive_num)
			if err!=nil {
				fmt.Println("ErrInc:",res,"HINCRBY",redis_key,"alive_num",rpinfo.alive_num)
			}
		}
		requestHttp(redis_key)
	}
	g_recored[idx].m_record=make(map[string]*reportInfo)
}

func main() {
	pools_redis = append(pools_redis,newPool("127.0.0.1:6373"),newPool("127.0.0.1:6373"))
	pools_redis_click = append(pools_redis_click,newPool("127.0.0.1:6374"),newPool("127.0.0.1:6374"))
	pools_redis_sid = append(pools_redis_sid,newPool("192.168.1.168:6378"),newPool("192.168.1.168:6378"))
	pools_redis_dcate = append(pools_redis_dcate,newPool("192.168.1.152:6376"),newPool("192.168.1.152:6376"))
	runtime.GOMAXPROCS(runtime.NumCPU())
	var quit chan int
	initRecoredSet()
	initTaskCh()
	initDomainCateMap()
	go fillTask()
	for i := 0; i < count; i++ {
      go processTask(i)
  }
  go updateRecord()
  go reloadDomainCateMap()
	<- quit
}