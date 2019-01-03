package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const earthRadius = 6378137
const rad = math.Pi / 180

var cline chan []string
var stwg sync.WaitGroup

var maplock sync.RWMutex

func main() {
	fmt.Println("begin...")
	//fmt.Println(GetSphereDistance(-99.189470, 19.302510, -99.189408, 19.302522))
	runtime.GOMAXPROCS(runtime.NumCPU())
	files, err := getAllFiles("./2018")
	if err != nil {
		panic(err)
	}
	//fmt.Printf("files:%v", files)
	sort.Strings(files)
	fmt.Printf("files:%v\n", files)

	for _, file := range files {
		fmt.Println("deal:", file)
		fi, err := os.Open(file)
		if err != nil {
			fmt.Printf("Error: %s\n", err)
			return
		}
		br := bufio.NewReader(fi)
		var num int
		for {
			if num%1000000 == 0 {
				fmt.Println("lines:", num)
			}

			line, _, c := br.ReadLine()
			if c == io.EOF {
				break
			}
			num++
			sline := string(line)
			data := strings.Split(sline, ",")
			//stData(data)
			cline <- data
		}

		fi.Close()
	}
	var end []string
	for i := 0; i < runtime.NumCPU(); i++ {
		cline <- end
	}
	time.Sleep(2 * time.Second)
	stwg.Wait()
	output()
}

func output() {
	fmt.Println("detail:")
	time.Now()
	wfi, err := os.OpenFile("out.log", os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}
	defer wfi.Close()

	for key, sp := range tollresultmap {
		sort.Slice(sp.p0t310, func(i, j int) bool { return sp.p0t310[i].timestamp < sp.p0t310[j].timestamp })

		//tollid+inout+tof+orderid+driverid
		//if key == "20006,entry,true,87961560317150,650910892391271" {
		//	fmt.Println(key, ":")
		//	for _, s := range sp.p0t310 {
		//		fmt.Printf("%v,%v;%v\n", s.lng, s.lat, s.timestamp)
		//	}
		//}

		var preds float64
		var preidx int
		isbefore := true
		var prepoint *tollresultpoint
		for idx, item := range sp.p0t310 {
			ds := GetSphereDistance(item.lng, item.lat, sp.tp.Lng, sp.tp.Lat)
			if idx == 0 {
				preds = ds
				preidx = idx
				prepoint = item
				continue
			}
			bds := GetSphereDistance(item.lng, item.lat, prepoint.lng, prepoint.lat)

			if preds < ds || (bds > ds && bds > preds) {
				isbefore = false
			}

			if isbefore {
				switch {
				case 310 < ds && ds <= 360:
					sp.b310 = append(sp.b310, sp.p0t310[preidx:idx+1]...)
				case 260 < ds && ds <= 310:
					sp.b310 = append(sp.b310, sp.p0t310[preidx:idx+1]...)
					sp.b260 = append(sp.b260, sp.p0t310[preidx:idx+1]...)
				case 210 < ds && ds <= 260:
					sp.b260 = append(sp.b260, sp.p0t310[preidx:idx+1]...)
					sp.b210 = append(sp.b210, sp.p0t310[preidx:idx+1]...)
				case 160 < ds && ds <= 210:
					sp.b210 = append(sp.b210, sp.p0t310[preidx:idx+1]...)
					sp.b160 = append(sp.b160, sp.p0t310[preidx:idx+1]...)
				case 110 < ds && ds <= 160:
					sp.b160 = append(sp.b160, sp.p0t310[preidx:idx+1]...)
					sp.b110 = append(sp.b110, sp.p0t310[preidx:idx+1]...)
				case 80 < ds && ds <= 110:
					sp.b110 = append(sp.b110, sp.p0t310[preidx:idx+1]...)
					sp.b60 = append(sp.b60, sp.p0t310[preidx:idx+1]...)
				case 60 < ds && ds <= 80:
					sp.b110 = append(sp.b110, sp.p0t310[preidx:idx+1]...)
					sp.b60 = append(sp.b60, sp.p0t310[preidx:idx+1]...)
				case 30 < ds && ds <= 60:
					sp.b60 = append(sp.b60, sp.p0t310[preidx:idx+1]...)
					sp.b30 = append(sp.b30, sp.p0t310[preidx:idx+1]...)
				case 10 < ds && ds <= 30:
					sp.b60 = append(sp.b60, sp.p0t310[preidx:idx+1]...)
					sp.b30 = append(sp.b30, sp.p0t310[preidx:idx+1]...)
				case 0 <= ds && ds <= 10:
					sp.b30 = append(sp.b30, sp.p0t310[preidx:idx+1]...)
				default:
					fmt.Println("before:", isbefore, "ds:", ds)
				}
			} else { //after
				switch {
				case 310 < ds && ds <= 360:
					sp.a310 = append(sp.a310, sp.p0t310[preidx:idx+1]...)
				case 260 < ds && ds <= 310:
					sp.a310 = append(sp.a310, sp.p0t310[preidx:idx+1]...)
					sp.a260 = append(sp.a260, sp.p0t310[preidx:idx+1]...)
				case 210 < ds && ds <= 260:
					sp.a260 = append(sp.a260, sp.p0t310[preidx:idx+1]...)
					sp.a210 = append(sp.a210, sp.p0t310[preidx:idx+1]...)
				case 160 < ds && ds <= 210:
					sp.a210 = append(sp.a210, sp.p0t310[preidx:idx+1]...)
					sp.a160 = append(sp.a160, sp.p0t310[preidx:idx+1]...)
				case 110 < ds && ds <= 160:
					sp.a160 = append(sp.a160, sp.p0t310[preidx:idx+1]...)
					sp.a110 = append(sp.a110, sp.p0t310[preidx:idx+1]...)
				case 80 < ds && ds <= 110:
					sp.a110 = append(sp.a110, sp.p0t310[preidx:idx+1]...)
					sp.a60 = append(sp.a60, sp.p0t310[preidx:idx+1]...)
				case 60 < ds && ds <= 80:
					sp.a110 = append(sp.a110, sp.p0t310[preidx:idx+1]...)
					sp.a60 = append(sp.a60, sp.p0t310[preidx:idx+1]...)
				case 30 < ds && ds <= 60:
					sp.a60 = append(sp.a60, sp.p0t310[preidx:idx+1]...)
					sp.a30 = append(sp.a30, sp.p0t310[preidx:idx+1]...)
				case 10 < ds && ds <= 30:
					sp.a60 = append(sp.a60, sp.p0t310[preidx:idx+1]...)
					sp.a30 = append(sp.a30, sp.p0t310[preidx:idx+1]...)
				case 0 <= ds && ds <= 10:
					sp.a30 = append(sp.a30, sp.p0t310[preidx:idx+1]...)
				default:
					fmt.Println("before:", isbefore, "ds:", ds)
				}
			}

			preidx = idx
			preds = ds
			prepoint = item
		}

		wfi.WriteString(fmt.Sprintf("%v,", key))
		outdetail(wfi, sp.b310, ",")
		outdetail(wfi, sp.b260, ",")
		outdetail(wfi, sp.b210, ",")
		outdetail(wfi, sp.b160, ",")
		outdetail(wfi, sp.b110, ",")
		outdetail(wfi, sp.b60, ",")
		outdetail(wfi, sp.b30, ",")
		outdetail(wfi, sp.a30, ",")
		outdetail(wfi, sp.a60, ",")
		outdetail(wfi, sp.a110, ",")
		outdetail(wfi, sp.a160, ",")
		outdetail(wfi, sp.a210, ",")
		outdetail(wfi, sp.a260, ",")
		outdetail(wfi, sp.a310, "\n")
	}
}

func outdetail(f *os.File, sp []*tollresultpoint, sep string) {
	var av float64
	tnum := len(sp)

	var pre *tollresultpoint
	var tspeed float64
	for idx, trt := range sp {
		if idx == 0 {
			pre = trt
			continue
		}
		d := GetSphereDistance(trt.lng, trt.lat, pre.lng, pre.lat)
		tn, _ := strconv.Atoi(trt.timestamp[:10])
		tp, _ := strconv.Atoi(pre.timestamp[:10])

		if tn-tp < 1 || d < 1 {
			tnum--
			continue
		}
		tspeed += d / float64(tn-tp)
		pre = trt
	}

	if tnum > 1 {
		av = tspeed / float64(tnum-1) * 1000
	} else {
		av = 0
	}
	f.WriteString(fmt.Sprintf("%.f%v", av, sep))

}

const (
	before  = -1
	station = 0
	after   = 1
)

type tollresult struct {
	timestamp string
	lng       float64
	lat       float64
	speed     string
	tollid    int
	inout     string
	tof       string
	orderid   string
	driverid  string
}

type tollresultpoint struct {
	where     int
	distance  float64
	timestamp string
	lng       float64
	lat       float64
}

type speeds struct {
	p0t310 []*tollresultpoint
	tp     *tPoint

	b30  []*tollresultpoint
	b60  []*tollresultpoint
	b110 []*tollresultpoint
	b160 []*tollresultpoint
	b210 []*tollresultpoint
	b260 []*tollresultpoint
	b310 []*tollresultpoint
	a30  []*tollresultpoint
	a60  []*tollresultpoint
	a110 []*tollresultpoint
	a160 []*tollresultpoint
	a210 []*tollresultpoint
	a260 []*tollresultpoint
	a310 []*tollresultpoint
}

//tollid+inout+tof+orderid+driverid
var tollresultmap map[string]*speeds

func stData() {

	stwg.Add(1)
	defer stwg.Done()

	//端上时间戳，服务器时间戳，经度，纬度，速度，X方向加速度，y方向加速度，z方向加速度，航向夹角，翻滚夹角，俯仰夹角，卫星数，海拔，气压，垂直精度,高速站ID，出入口，真or假值，订单ID，司机ID，订单开始时间，结束时间
	//1542812653535,1542812653660,-99189470,19302510,0,4.383783,2.456451,8.749610,0.000000,0.000000,0.000000,0,2284.000000,0.000000,0.000000,31008,entry,false,87961544387903,650910888092938,2018-11-21 22:30:22,2018-11-21 23:17:32

	for {
		data := <-cline
		if len(data) == 0 {
			break
		}

		r := &tollresult{}
		tollpoint := &tPoint{}
		for idx, d := range data {
			switch idx {
			case 1:
				//fmt.Println("服务器时间戳:", d)
				r.timestamp = d
			case 2:
				//fmt.Println("经度:", d)
				lng, _ := strconv.ParseFloat(d, 64)
				r.lng = lng / 1000000
			case 3:
				//fmt.Println("纬度:", d)
				lat, _ := strconv.ParseFloat(d, 64)
				r.lat = lat / 1000000

			case 4:
				//fmt.Println("速度:", d)
				r.speed = d
			case 15:
				//fmt.Println("高速站ID:", d)
				id, _ := strconv.Atoi(d)
				tollpoint, _ = tollMap[id]
				r.tollid = id
			case 16:
				//fmt.Println("出入口:", d)
				r.inout = d
			case 17:
				//fmt.Println("真or假值", d)
				r.tof = d
			case 18:
				//fmt.Println("订单ID", d)
				r.orderid = d
			case 19:
				//fmt.Println("司机ID", d)
				r.driverid = d
			}
		}

		stDataDetailSpeed(r, tollpoint)
	}
}

func stDataDetailSpeed(tr *tollresult, tp *tPoint) {
	if tr == nil || tp == nil {
		return
	}
	//calc
	sdistance := GetSphereDistance(tr.lng, tr.lat, tp.Lng, tp.Lat)
	//|| (tr.orderid == "87961596646385" && strconv.Itoa(tr.tollid) == "31014")
	// || (tr.orderid == "87961560317150" && strconv.Itoa(tr.tollid) == "20006")
	if sdistance <= 360 {
		maplock.Lock()
		defer maplock.Unlock()

		//adistance := GetSphereDistance(tr.lng, tr.lat, tp.AfterLng, tp.AfterLat)
		//bdistance := GetSphereDistance(tr.lng, tr.lat, tp.BeforeLng, tp.BeforeLat)
		//tollid+inout+tof+orderid+driverid
		key := fmt.Sprintf("%v,%v,%v,%v,%v", tr.tollid, tr.inout, tr.tof, tr.orderid, tr.driverid)
		//key := fmt.Sprintf("%v,%v,%v", tr.tollid, tr.inout, tr.tof)
		sp, ok := tollresultmap[key]
		if !ok {
			tsp := &speeds{}
			tollresultmap[key] = tsp
			sp = tsp
		}
		var where int
		//if bdistance < adistance {
		//	where = before
		//} else {
		//	where = after
		//}

		sp.tp = tp
		tmpspeed := &tollresultpoint{0, sdistance, tr.timestamp, tr.lng, tr.lat}
		if true {
			sp.p0t310 = append(sp.p0t310, tmpspeed)
		} else {
			switch {
			case 260 < sdistance && sdistance <= 310:
				switch where {
				case before:
					sp.b310 = append(sp.b310, tmpspeed)
				case after:
					sp.a310 = append(sp.a310, tmpspeed)
				}
			case 210 < sdistance && sdistance <= 260:
				switch where {
				case before:
					sp.b260 = append(sp.b260, tmpspeed)
				case after:
					sp.a260 = append(sp.a260, tmpspeed)
				}
			case 160 < sdistance && sdistance <= 210:
				switch where {
				case before:
					sp.b210 = append(sp.b210, tmpspeed)
				case after:
					sp.a210 = append(sp.a210, tmpspeed)
				}
			case 110 < sdistance && sdistance <= 160:
				switch where {
				case before:
					sp.b160 = append(sp.b160, tmpspeed)
				case after:
					sp.a160 = append(sp.a160, tmpspeed)
				}
			case 60 < sdistance && sdistance <= 110:
				switch where {
				case before:
					sp.b110 = append(sp.b110, tmpspeed)
				case after:
					sp.a110 = append(sp.a110, tmpspeed)
				}
			case 30 < sdistance && sdistance <= 60:
				switch where {
				case before:
					sp.b60 = append(sp.b60, tmpspeed)
				case after:
					sp.a60 = append(sp.a60, tmpspeed)
				}
			case sdistance <= 30:
				switch where {
				case before:
					sp.b30 = append(sp.b30, tmpspeed)
				case after:
					sp.a30 = append(sp.a30, tmpspeed)
				}
			}
		}

	}

}

//文件过滤
var filter = true

func getAllFiles(dirPth string) (files []string, err error) {
	dir, err := ioutil.ReadDir(dirPth)
	if err != nil {
		return nil, err
	}

	PthSep := string(os.PathSeparator)

	for _, fi := range dir {
		if fi.IsDir() { // 目录, 递归遍历
			//fmt.Println(dirPth + PthSep + fi.Name())
			tmpfiles, err := getAllFiles(dirPth + PthSep + fi.Name())
			if err == nil {
				files = append(files, tmpfiles...)
			}
		} else if filter {
			// 过滤指定格式
			ok := strings.HasPrefix(fi.Name(), "decoded_")
			if ok {
				files = append(files, dirPth+PthSep+fi.Name())
			}
		} else {
			//fmt.Println("find file:", dirPth+PthSep+fi.Name())
			files = append(files, dirPth+PthSep+fi.Name())
		}
	}

	return files, nil
}

func GetSphereDistance(lng1, lat1, lng2, lat2 float64) float64 {
	lngDiff := (lng1 - lng2) * rad

	radLat1 := lat1 * rad
	radLat2 := lat2 * rad
	latDiff := radLat1 - radLat2

	r := 2 * math.Asin(math.Sqrt(math.Pow(math.Sin(latDiff/2), 2)+math.Cos(radLat1)*math.Cos(radLat2)*math.Pow(math.Sin(lngDiff/2), 2)))
	return r * earthRadius
}

type tPoint struct {
	Lat       float64 `json:"lat,string"`
	Lng       float64 `json:"lng,string"`
	BeforeLat float64 `json:"lat,string"`
	BeforeLng float64 `json:"lng,string"`
	AfterLat  float64 `json:"lat,string"`
	AfterLng  float64 `json:"lng,string"`
}

var tollMap map[int]*tPoint

type oriToll struct {
	Id                      int      `json:"id,string"`
	ExternalId              string   `json:"externalId"`
	Name                    string   `json:"name"`
	TollLatitude            float64  `json:"tollLatitude,string"`
	TollLongitude           float64  `json:"tollLongitude,string"`
	ExtendStation           []tPoint `json:"extendStation,[]interface{}"`
	DelStation              []tPoint `json:"delStation,[]interface{}"`
	BeforeLocationLatitude  float64  `json:"beforeLocationLatitude,string"`
	BeforeLocationLongitude float64  `json:"beforeLocationLongitude,string"`
	ExtendBefore            []tPoint `json:"extendBefore,[]interface{}"`
	DelBefore               []tPoint `json:"delBefore,[]interface{}"`
	AfterLocationLatitude   float64  `json:"afterLocationLatitude,string"`
	AfterLocationLongitude  float64  `json:"afterLocationLongitude,string"`
	ExtendAfter             []tPoint `json:"extendAfter,[]interface{}"`
	DelAfter                []tPoint `json:"delAfter,[]interface{}"`
	Heading                 string   `json:"heading"`
	Price                   int32    `json:"price"`
	LastUpdate              string   `json:"lastUpdate"`
}

type oriTollData struct {
	Tolls []oriToll `json:"tolls"`
}

//读取收费站信息
func init() {
	tollresultmap = make(map[string]*speeds)

	fileBytes, err := ioutil.ReadFile("MX.json")
	if err != nil {
		panic(err)
	}
	data := oriTollData{}
	if err = json.Unmarshal(fileBytes, &data); err != nil {
		panic(fmt.Errorf("unmarshal file failed, err=%v", err))
	}

	tollMap = make(map[int]*tPoint)
	for _, toll := range data.Tolls {
		tollMap[toll.Id] = &tPoint{toll.TollLatitude, toll.TollLongitude,
			toll.BeforeLocationLatitude, toll.BeforeLocationLongitude,
			toll.AfterLocationLatitude, toll.AfterLocationLongitude}
	}

	cline = make(chan []string, 100000)
	for i := 0; i < runtime.NumCPU(); i++ {
		go stData()
	}
}
