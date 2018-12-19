package rb

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"its-thrift-gen-go/base"
	"its-thrift-gen-go/rb_pack_new"
	"os"
	"route-broker/rb/metrics"
	"route-broker/rb/toll"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	yaml "gopkg.in/yaml.v2"
)

var wg sync.WaitGroup //定义一个同步等待的组

func TestBatchFile(t *testing.T) {
	if err := myInit(); err != nil {
		fmt.Printf("err message:[%s]", err.Error())
	}
	go BatchFile("job_id=3531024aa.csv")
	go BatchFile("job_id=3531024ab.csv")
	time.Sleep(1 * time.Second)
	wg.Wait() //阻塞直到所有任务完成
}

func BatchFile(file string) {
	wg.Add(1)       //添加一个计数
	defer wg.Done() //减去一个计数
	handler := RouteBroker{}
	handler.InitRouteBroker()

	fi, err := os.Open(file)
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}
	defer fi.Close()

	wfi, err := os.OpenFile("out"+file, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}
	defer wfi.Close()

	br := bufio.NewReader(fi)
	var linenum int = 0
	for {
		line, _, c := br.ReadLine()
		if c == io.EOF {
			fmt.Println("EOF line=", linenum)
			break
		}
		linenum++
		sline := string(line)
		data := strings.Split(sline, ",")
		if len(data) < 13 {
			fmt.Println("Wrong line:", sline)
			continue
		}

		req := &rb_pack_new.GlobalHighwayTollFeeRequest{}

		DriverID, err := strconv.ParseInt(data[0], 10, 64)
		if err != nil {
			fmt.Printf("Error: %s\n", err)
			return
		}

		year, err := strconv.Atoi(data[1])
		if err != nil {
			fmt.Printf("Error: %s\n", err)
			return
		}
		month, err := strconv.Atoi(data[2])
		if err != nil {
			fmt.Printf("Error: %s\n", err)
			return
		}
		day, err := strconv.Atoi(data[3])
		if err != nil {
			fmt.Printf("Error: %s\n", err)
			return
		}
		hour, err := strconv.Atoi(data[4])
		if err != nil {
			fmt.Printf("Error: %s\n", err)
			return
		}
		min, err := strconv.Atoi(data[5])
		if err != nil {
			fmt.Printf("Error: %s\n", err)
			return
		}
		sec, err := strconv.Atoi(data[6])
		if err != nil {
			fmt.Printf("Error: %s\n", err)
			return
		}

		fyear, err := strconv.Atoi(data[7])
		if err != nil {
			fmt.Printf("Error: %s\n", err)
			return
		}
		fmonth, err := strconv.Atoi(data[8])
		if err != nil {
			fmt.Printf("Error: %s\n", err)
			return
		}
		fday, err := strconv.Atoi(data[9])
		if err != nil {
			fmt.Printf("Error: %s\n", err)
			return
		}
		fhour, err := strconv.Atoi(data[10])
		if err != nil {
			fmt.Printf("Error: %s\n", err)
			return
		}
		fmin, err := strconv.Atoi(data[11])
		if err != nil {
			fmt.Printf("Error: %s\n", err)
			return
		}
		fsec, err := strconv.Atoi(data[12])
		if err != nil {
			fmt.Printf("Error: %s\n", err)
			return
		}

		timezone, err := strconv.ParseInt(data[13], 10, 64)
		if err != nil {
			fmt.Printf("Error: %s\n", err)
			return
		}

		timezone = 8
		CST := time.FixedZone("Time Zone", int((time.Duration(timezone) * time.Hour).Seconds()))

		begintime := time.Date(year, time.Month(month), day, hour, min, sec, 00, CST).Unix()
		endtime := time.Date(fyear, time.Month(fmonth), fday, fhour, fmin, fsec, 00, CST).Unix()
		req.DriverID = DriverID
		req.DriverStartTime = int32(begintime)
		req.DriverEndTime = int32(endtime)
		req.CallerID = "justacallerid"
		req.LogID = 990099009900
		req.Biztype = 258
		req.OrderID = 33445522
		async := false
		req.Isasync = &async

		req.Trace = new(base.Trace)
		//废弃,没用
		//req.Trace.ReqFrom = base.ReqFrom_AU
		tmpregionid := "MX"
		req.Trace.RegionId = &tmpregionid

		//orderid := strconv.FormatInt(req.OrderID, 10)
		//driverid := strconv.FormatInt(req.DriverID, 10)
		//startime := strconv.FormatInt(int64(req.DriverStartTime), 10)
		//fmt.Println("redis key:", orderid+":"+driverid+":"+startime)

		r, _ := handler.GlobalTollFees(req)
		//
		//htfeeResp, _ := json.Marshal(*r)
		wfi.WriteString(fmt.Sprintf("%v,%v,%v\n", sline, r.GetAmountTollFee(), r.GetTollStationNode()))
	}

	//日志是异步打印,先写入chan,然后在从chan写入缓存区,直接flush可能都没有写入缓存区
	time.Sleep(200 * time.Millisecond)
	rootConfPack.MyNormalBaseLog.bufferWriter.Flush()
	rootConfPack.MyPublicBaseLog.bufferWriter.Flush()
	rootConfPack.MyWfBaseLog.bufferWriter.Flush()
}

func TestGetPassedTolls(t *testing.T) {
	if err := myInit(); err != nil {
		fmt.Printf("err message:[%s]", err.Error())
	}

	handler := RouteBroker{}
	handler.InitRouteBroker()

	req := &rb_pack_new.GlobalHighwayTollFeeRequest{}
	req.DriverID = 650910884288911

	CST := time.FixedZone("Beijing Time", int((8 * time.Hour).Seconds()))
	endtime := time.Date(2018, 10, 17, 05, 53, 14, 00, CST).Unix()
	begintime := time.Date(2018, 10, 17, 05, 18, 37, 00, CST).Unix()

	req.DriverStartTime = int32(begintime)
	req.DriverEndTime = int32(endtime)
	req.CallerID = "justacallerid"
	req.LogID = 990099009900
	req.Biztype = 258
	req.OrderID = 33445522
	async := false
	req.Isasync = &async
	//req.CityID = 55000116

	req.Trace = new(base.Trace)
	//废弃,没用
	//req.Trace.ReqFrom = base.ReqFrom_AU
	tmpregionid := "MX"
	req.Trace.RegionId = &tmpregionid

	orderid := strconv.FormatInt(req.OrderID, 10)
	driverid := strconv.FormatInt(req.DriverID, 10)
	startime := strconv.FormatInt(int64(req.DriverStartTime), 10)
	fmt.Println("redis key:", orderid+":"+driverid+":"+startime)

	r, _ := handler.GlobalTollFees(req)

	htfeeResp, _ := json.Marshal(*r)
	fmt.Printf("HighwayTollFeeResponse:%s", string(htfeeResp))

	//日志是异步打印,先写入chan,然后在从chan写入缓存区,直接flush可能都没有写入缓存区
	time.Sleep(200 * time.Millisecond)
	rootConfPack.MyNormalBaseLog.bufferWriter.Flush()
	rootConfPack.MyPublicBaseLog.bufferWriter.Flush()
	rootConfPack.MyWfBaseLog.bufferWriter.Flush()
}

// 单元测试的init
func myInit() (err error) {

	filePath := "../../../output/conf/.us01-v/route-broker.yaml"
	//confpath := "../../../output/conf/.us01-v/conf.yaml"
	confpath := "../../../output/conf/.us01-v/conf.yaml"
	//apollopath := "../../../output"
	overloadjson := "../../../output/conf/.us01-v/overload.json"
	NormalBaseLog := "../../../output/log/normal.log"
	PublicBaseLog := "../../../output/log/public.log"
	WfBaseLog := "../../../output/log/wf.log"
	dynamicConfPath := "../../../output/conf"

	zerolog.TimeFieldFormat = time.RFC3339Nano
	zerolog.DurationFieldUnit = time.Microsecond

	quitStruct.InitQuit()

	metrics.InitMetrics()

	InitIPManager()

	InitPool()

	InitServiceName()

	InitCommonValue()

	buf, err := ioutil.ReadFile(filePath)
	if err != nil {
		return ErrorNo{fmt.Errorf("err:%s, filepath:%s", err.Error(), filePath), FileReadErrNo}
	}

	rootConfPack = new(RootConfPackS)
	err = yaml.Unmarshal(buf, rootConfPack)
	if err != nil {
		return ErrorNo{fmt.Errorf("err:%s, filepath:%s", err.Error(), filePath), YamlReadErrNo}
	}
	//rootConfPack.ApolloPath = apollopath
	rootConfPack.ConfigModel = 0
	rootConfPack.OverloadFilePath = overloadjson
	rootConfPack.NormalLogFile = NormalBaseLog
	rootConfPack.PublicLogFile = PublicBaseLog
	rootConfPack.WfLogFile = WfBaseLog
	rootConfPack.SaveDynamicIPListDirPath = dynamicConfPath
	rootConfPack.ConfigPath = confpath
	//rootConfPack.LogBufferSize = 10
	rootConfPack.TollsConfigPath = "../conf/tollconf"

	InitApollo(rootConfPack.ApolloPath)

	if rootConfPack.QuitTimeout < 5 {
		return ErrorNo{fmt.Errorf("quit timeout must >= 5s"), ValueErrNo}
	}

	if rootConfPack.RWTimeout < 2 {
		return ErrorNo{fmt.Errorf("rw timeout must >= 2s"), ValueErrNo}
	}

	if rootConfPack.AcceptTimeout < 2 {
		return ErrorNo{fmt.Errorf("accept timeout must >= 2s"), ValueErrNo}
	}

	// 初始化log
	rootConfPack.MyNormalBaseLog, err = NewMyBaseLogWriter(
		rootConfPack.NormalLogFile,
		rootConfPack.LogBufferSize,
	)
	if err != nil {
		return err
	}
	rootConfPack.MyWfBaseLog, err = NewMyBaseLogWriter(
		rootConfPack.WfLogFile,
		rootConfPack.LogBufferSize,
	)
	if err != nil {
		return err
	}

	rootConfPack.MyPublicBaseLog, err = NewMyBaseLogWriter(
		rootConfPack.PublicLogFile,
		rootConfPack.LogBufferSize,
	)
	if err != nil {
		return err
	}

	// 初始化限流
	err = myInitOverloadProtect(
		rootConfPack.OverloadZKIPs,
		rootConfPack.OverloadZKPath,
		rootConfPack.OverloadFilePath,
		time.Duration(rootConfPack.OverloadZKTimeout)*time.Second,
	)
	if err != nil {
		return err
	}

	// 初始化city fence
	if rootConfPack.SetCityFenceEnable {
		err = rootConfPack.ChinaCityFence.ParseFromFile(rootConfPack.ChinaCityFenceFile)
		if err != nil {
			return err
		}
	}

	log.Info().Uint("pid", NowPID).Msgf("rootConfPack:%v", *rootConfPack)

	err = InitConf()
	if err != nil {
		return err
	}
	log.Info().Uint("pid", NowPID).Msg("InitConf end!")
	// Init caller manager
	InitCallerManager()
	log.Info().Uint("pid", NowPID).Msg("InitCallerManager end!")
	// 初始化manager
	InitManager()
	log.Info().Uint("pid", NowPID).Msg("InitManager end!")
	//InitRbHandler()
	log.Info().Uint("pid", NowPID).Msg("InitRbHandler end!")

	go func() {
		for {
			nowConf := GetNowConf()
			time.Sleep(time.Duration(nowConf.ReadConfigTimeInterval) * time.Second)
			err := InitConf()
			if err != nil {
				nowConf.Logger.WarnDltag("_rb_system").Err(err).Msg("dynamic load config is error")
				_ = metrics.NewCounterN(
					"load_conf_error",
					1,
					nil)
			}
		}
	}()

	// 初始化map_version补丁工具
	initMapVersionHelper()

	toll.InitTolls(rootConfPack.TollsConfigPath)

	return nil
}

// InitOverloadProtect 必须先调用该函数
func myInitOverloadProtect(zkIPs []string, zkPath, filePath string, timeout time.Duration) (err error) {
	tmp := &TokenBucketManager{
		zkIPs:    zkIPs,
		zkPath:   zkPath,
		filePath: filePath,
		timeout:  timeout,
	}

	zkSucc := false

	if !zkSucc {
		// 降级到文件读取
		err = tmp.fileInit()
		if err != nil {
			log.Warn().AnErr("file init", err).Uint("pid", NowPID).Msg("")
			return ErrorNo{errors.New("init overload fail"), TokenBucketInitErrNo}
		}
		log.Info().Uint("pid", NowPID).Msg("init overload by file")
	}

	tokenBucketManager = tmp

	return nil
}
