package main

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

func pause(text string) {
	fmt.Printf("text: %s, stats: %+v\n", text, db.Stats())
	fmt.Scanln()
}

type D struct {
	Name string `sql:"Variable_name"`
	Value string `sql:"Value"` 
}
var db *sql.DB

var text = "SHOW STATUS LIKE 'Threads_connected'"

func main() {
	var err error
	db, err = sql.Open("mysql", "test:test@tcp(127.0.0.1:32784)/test")
	fmt.Println("err is ", err)
	if err != nil {
		return
	}
	fmt.Scanln()
	_ = db

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return
	}
	connQuery := func(stmt string) func(context.Context) {
		return func(ctx context.Context) {
			r := D{}
			conn.QueryRowContext(ctx, stmt).Scan(&r.Name, &r.Value)
		}
	}

	dbQuery := func(stmt string) func(context.Context) {
		return func(ctx context.Context) {
			db.QueryRowContext(ctx, stmt)
		}
	}
	pause("before all")
	for _, s := range []Single{
		{connQuery(text), "connQuery"},
		{connQuery(text), "connQuery"},
		{dbQuery(text), "dbQuery"},
		{dbQuery(text), "dbQuery"},
	} {
		s.f(ctx)
		pause(fmt.Sprintf("after %s", s.text))
	}
	r := D{}
	fmt.Println(conn.QueryRowContext(ctx, text).Scan(&r.Name, &r.Value), r)
	pause("before db.Close()")
	db.Close()
	pause("after db.Close()")
	fmt.Println(conn.QueryRowContext(ctx, text).Scan(&r.Name, &r.Value), r)
	
}

type Single struct {
	f func(context.Context)
	text string
}
