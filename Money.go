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
    "database/sql"
   _ "github.com/go-sql-driver/mysql"
    "log"
    "strings"
)

var logger_task *log.Logger
var logger_err *log.Logger
var db *sql.DB
var task_ch []chan string
var count int

func main() {

    var TaskFile string
    if len(os.Args) > 1 {
        TaskFile = os.Args[1]
    }
	runtime.GOMAXPROCS(runtime.NumCPU())
    initmylog()
    initmysql()
	initTaskCh()
	go fillTask(TaskFile)
	for i := 0; i < count; i++ {
        go processTask(i)
    }
    
	var quit chan int
	<- quit
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
	db, err = sql.Open("mysql", "worker:123qwe@(122.13.213.110:3306)/?charset=utf8")

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
    fmt.Print(str_log)
    var strarr []string = strings.Split(str_log,"|")
    if len(strarr) < 3 {
        fmt.Printf(" err request [%s] \n", str_log)
        return 
    }
    
    rows_subtask_info, err := db.Query("select t1.id,t1.parent_id,t1.descript,t1.next_task_id,t1.encourage_rules_id , t2.income,t2.experience,t2.master_income,t2.master_experience,t2.slice_group_id,t3.user_id,t3.user_name,t3.master_id from moneycool.subtaskinfo t1 left join moneycool.encourage_rules t2 on t1.encourage_rules_id = t2.id join moneycool.user_info t3 where t1.id = ? and t3.imei = ?", strarr[2], strarr[0])
    if err != nil {
        log.Fatal(err)
    }
    defer rows_subtask_info.Close()
    var id,parent_id,descript,next_task_id,encourage_rules_id,income,experience,master_income,master_experience,slice_group_id,user_id,user_name,master_id string
    for rows_subtask_info.Next() {
        if err := rows_subtask_info.Scan(&id,&parent_id,&descript,&next_task_id,&encourage_rules_id, &income,&experience,&master_income,&master_experience,&slice_group_id,&user_id,&user_name,&master_id); err != nil {
            log.Fatal(err)
        }
        break
    }
    if err := rows_subtask_info.Err();err != nil {
        log.Fatal(err)
    }
    fmt.Println(id,parent_id,descript,next_task_id,encourage_rules_id, income,experience,master_income,master_experience,slice_group_id,user_id,user_name)
        
    if parent_id != strarr[1] || len(user_id)==0{
        log.Fatal("Wrong Data!!")
    }
    
    var str_sql string = fmt.Sprintf("insert into moneycool.user_status (user_id,user_name, task_desc,task_income,task_experience,task_time,task_id,`status`,task_level,encourage_rules_id, settlement_status) values ('%s', '%s', '%s','%s', '%s', %d, '%s', 1, 2, '%s', 1)",
    user_id, user_name,descript,income,experience,time.Now().Unix(),id, encourage_rules_id);
    fmt.Println(str_sql)
    excuteSql(str_sql, idx, str_log)
    
    str_sql = "update moneycool.user_info t set income = income + ("+ income +"*(1+(select income_rate from moneycool.user_level_info where level = t.level))) , master_income = master_income + "+master_income+", remaining_sum = remaining_sum + "+income+", experience = experience + "+experience+",`level` = ( SELECT `level` FROM moneycool.user_level_info t2 WHERE t2.experience <= t.experience ORDER BY t2.experience DESC LIMIT 1 ) where imei = '"+strarr[0]+"'" 
    fmt.Println(str_sql)
    excuteSql(str_sql, idx, str_log)
    
    str_sql = "update moneycool.user_info t set income = income + "+ master_income +", experience = experience +"+master_experience+",`level`=( SELECT `level` FROM moneycool.user_level_info t2 WHERE t2.experience <= t.experience ORDER BY t2.experience DESC LIMIT 1 ) where user_id = '"+master_id+"'"
    fmt.Println(str_sql)
    excuteSql(str_sql, idx, str_log)

    str_sql = "insert into moneycool.user_slices_records (user_id, slice_id, slice_num) (select '"+user_id+"',slice_id,slice_num from moneycool.slice_group where group_id = "+slice_group_id+")"
    fmt.Println(str_sql)
    excuteSql(str_sql, idx, str_log)
}

func fillTask(TaskFile string){
    var tmpTaskFile string
	if len(TaskFile)==0 {
        tmpTaskFile="dsp_pipe_count"
    } else {
        tmpTaskFile=TaskFile
    }
    fmt.Println(tmpTaskFile)
    
    cmd := exec.Command("cat", tmpTaskFile)
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
