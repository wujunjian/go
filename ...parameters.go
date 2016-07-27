package main  
  
import (  
    "fmt"  
    "os"  
)  
  
func f1(args ...interface {}) {  
    f2(args...)  
    f2(args[1:]...)  
}  
  
func f2(args ...interface {}) {  
    for i, v := range args {  
        fmt.Fprintf(os.Stdout, "i = %d %v\n", i, v)  
    }  
    fmt.Fprintf(os.Stdout, "--------------\n")  
}  
  
func main() {  
    f1(1, "hello", 3.14, main)  
  
    // 匿名函数 1  
    f := func(i, j int) (result int) { // f 为函数地址  
        result = i+j  
        return  
    }  
  
    fmt.Fprintf(os.Stdout, "f = %v  f(1,3) = %v\n", f, f(1, 3))  
  
    // 匿名函数 2  
    x, y := func(i, j int) (m, n int) { // x y 为函数返回值  
        return j, i  
    }(1, 9) // 直接创建匿名函数并执行  
  
    fmt.Fprintf(os.Stdout, "x = %d   y = %d\n", x, y)  
}  
