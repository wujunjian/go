package BloomFilter

import (
    "fmt"
    //"bytes"
    "sync"
    //"strings"
)

var bits []byte
var n int
var masks = []byte{0x01,0x02,0x04,0x08,0x10,0x20,0x40,0x80}

var BloomFilterLocker sync.RWMutex

func BloomFilterInit(num int) {
    //for i:=0;i<num;i++ {
    //    bits = append(bits, byte(0))
    //}
    
    bits = make([]byte, num)
}

func BloomFilterhash1(str_s string) int {
    strByte := []byte(str_s)
    var hash int
    x := 0; 
    for i := 0; i < len(strByte); i++ {
        hash = (hash << 4) + int(strByte[i])
        x = hash & 0xF0000000
        if x != 0 {
            hash ^= (x >> 24)
        }
        //hash &= ~x
        hash &= ^x
    }
    return hash
}

func BloomFilterhash2(str_s string) int {

    var hash int
    strByte := []byte(str_s)
    sLength:=len(strByte)
    hash = 5381
    for i := 0; i < sLength; i++ {
        hash = ((hash << 5) + hash) + int(strByte[i]); 
    } 
    return hash; 
}

func BloomFilterhash3(str_s string) int {
    var hash int
    strByte := []byte(str_s)
    sLength:=len(strByte)
    seed := 131; // 31 131 1313 13131 131313 etc.. 
    hash = 0; 
    for i := 0; i < sLength; i++ { 
        hash = (hash * seed) + int(strByte[i]); 
    } 
    return hash; 
}

func BloomFilter_Add(key string) {
    h1:=BloomFilterhash1(key) % (n);    //在哪一位置1
    h2:=BloomFilterhash2(key) % (n);
    h3:=BloomFilterhash3(key) % (n);
    idx1:=h1>>3;    //具体到char数组的哪个下标
    idx2:=h2>>3;
    idx3:=h3>>3;
    
    BloomFilterLocker.Lock()
    bits[idx1] |= masks[h1%8];        //将相应位置1
    bits[idx2] |= masks[h2%8];
    bits[idx3] |= masks[h3%8];
    BloomFilterLocker.Unlock()
}


func BloomFilter_Check(str_s string) bool {
    var result bool
    h1:=BloomFilterhash1(str_s) % (n);
    h2:=BloomFilterhash2(str_s) % (n);
    h3:=BloomFilterhash3(str_s) % (n);
    idx1:=h1>>3;
    idx2:=h2>>3;
    idx3:=h3>>3;
 	
    BloomFilterLocker.RLock()

    if (bits[idx1] & masks[h1%8])!=0  && (bits[idx2] & masks[h2%8])!=0 && (bits[idx3] & masks[h3%8])!=0 {
        result = true
    }else{
        result = false
    }
    BloomFilterLocker.RUnlock()
    return result
}
