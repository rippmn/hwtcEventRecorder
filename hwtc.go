package hwtc

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"os"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
)

// PubSubMessage is the payload of a Pub/Sub event.
type PubSubMessage struct {
        Data []byte `json:"data"`
}

type Event struct {
        Count int `json:"count"`
        EventTime string `json:"eventDateTime"`
}

var dbPool *sql.DB

func init() {
	log.Println("init DB connection")
	var (
		dbUser                 = os.Getenv("DB_USR")                  // e.g. 'my-db-user'
		dbPwd                  = os.Getenv("DB_PWD")                  // e.g. 'my-db-password'
		instanceConnectionName = os.Getenv("INST_CONN_NM") // e.g. 'project:region:instance'
		dbName                 = os.Getenv("DB_NM")                  // e.g. 'my-database'
	)
        
	var err error
	dbPool, err = sql.Open("mysql", fmt.Sprintf("%s:%s@unix(//cloudsql/%s)/%s?parseTime=true", dbUser, dbPwd, instanceConnectionName, dbName))
	if err != nil {
	  log.Println("ERROR")
      log.Printf("sql.Open: %v", err)
      return
	}

	dbPool.SetMaxIdleConns(5)
	dbPool.SetMaxOpenConns(7)
	dbPool.SetConnMaxLifetime(1800)
	log.Println("DB Init End")
}

func RecordEvent(ctx context.Context, m PubSubMessage ) error {
	var tte Event
	log.Print("recording event")
	json.Unmarshal([]byte(m.Data), &tte)
	log.Println("unmarshall success")
	
	_ , err := dbPool.Exec("insert into tt_event(count, event_date_time) values(?,?)", tte.Count, tte.EventTime)

	if err != nil {
	  log.Println("ERROR")
      log.Printf("sql.Exec: %v", err)
	}
    return nil
}
