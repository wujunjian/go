package main

import (
    "fmt"
    "log"
    "gopkg.in/mgo.v2"
    "gopkg.in/mgo.v2/bson"
    "time"
)

type Person struct {
        Name string
        Phone string
}

func main() {
    //session, err := mgo.Dial("server1.example.com,server2.example.com")
    session, err := mgo.Dial("122.13.213.115:20000,122.13.213.115:20000,122.13.213.116:20000,122.13.213.117:20000,122.13.213.118:20000, 122.13.213.119:20000")
    if err != nil {
                panic(err)
        }
    defer session.Close()

    // Optional. Switch the session to a monotonic behavior.
    session.SetMode(mgo.Monotonic, true)
    //session.SetSafe(nil)

    c := session.DB("test").C("people")
    
    
    result := Person{}
    err = c.Find(bson.M{"name": "xuezhaomeng"}).One(&result)
    if err != nil {
        log.Fatal(err)
    }

    fmt.Println("Phone:", result.Phone)
    

    err = c.Insert(&Person{"xuezhaomeng", "666666666"},
                    &Person{"xuezhaomeng2", "13888888888"},
                    &Person{"xuezhaomeng3", "13666666666"})
    if err != nil {
        fmt.Println(err)
        err = c.Update(bson.M{"name": "xuezhaomeng"}, &Person{"xuezhaomeng", "44444444444444"})
        if err!=nil {
            fmt.Println("Update : ", err);
            return
        }else {
            fmt.Println("Update  success: ", err);  
        }
    }

    index := mgo.Index{
        Key: []string{"Name", "Phone"},
        Unique: true,
        DropDups: true,
        Background: true, // See notes.
        Sparse: true,
        ExpireAfter: time.Duration(20)*time.Second,
    }
    //err := collection.EnsureIndex(index)
    err = c.EnsureIndex(index)
    if err != nil {
        log.Fatal(err)
    }

    result = Person{}
    err = c.Find(bson.M{"name": "xuezhaomeng"}).One(&result)
    if err != nil {
        log.Fatal(err)
    }

    fmt.Println("Phone:", result.Phone)
}

