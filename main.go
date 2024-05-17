package main

import (
	context "context"
	sql "database/sql"
	fmt "fmt"
	os "os"

	uuid "github.com/google/uuid"
	polypheny "github.com/polypheny/Polypheny-Go-Driver"
)

const driverName = "polypheny"
const connectionString = "localhost:20590,pa:"

func initDatabase() error {
	db, err := sql.Open(driverName, connectionString)
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = db.ExecContext(context.Background(), "DROP TABLE IF EXISTS dispersion")
	if err != nil {
		return err
	}
	_, err = db.ExecContext(context.Background(), "CREATE TABLE dispersion(key VARCHAR(255) NOT NULL, color VARCHAR(10), item VARCHAR(255), PRIMARY KEY(key))")
	return err
}

func checkColor(color string) bool {
	colors := [7]string{"red", "orange", "yellow", "green", "blue", "indigo", "purple"}
	for _, c := range colors {
		if c == color {
			return true
		}
	}
	return false
}

func addItem(color string, item string) {
	if !checkColor(color) {
		fmt.Printf("%s is not a supported color", color)
	}
	if len(item) > 255 {
		fmt.Printf("item too long")
	}
	db, err := sql.Open(driverName, connectionString)
	if err != nil {
		fmt.Printf("error when adding an item\n")
	}
	stmt, err := db.PrepareContext(context.Background(), "INSERT INTO dispersion VALUES(?, ?, ?)")
	if err != nil {
		fmt.Printf("error when adding an item\n")
	}
	key := uuid.New().String()
	defer stmt.Close()
	result, err := stmt.ExecContext(context.Background(), key, color, item)
	if err != nil {
		fmt.Printf("error when adding an item\n")
	}
	affectedRows, _ := result.RowsAffected()
	if affectedRows != 1 {
		fmt.Printf("error when adding an item\n")
	}
}

func getAll() {
	conn, err := polypheny.PolyphenyDriver{}.Open(connectionString)
	if err != nil {
		return
	}
	defer conn.Close()
	result, err := conn.(*polypheny.PolyphenyConn).QueryMongoContext(context.Background(), "db.dispersion.find()")
	if result == nil || err != nil {
		return
	}
	for _, item := range *result {
		fmt.Printf("%s: %s\n", item["color"].(string), item["item"].(string))
	}
}

func getAllWithColor(color string) {
	conn, err := polypheny.PolyphenyDriver{}.Open(connectionString)
	if err != nil {
		return
	}
	defer conn.Close()
	query := fmt.Sprintf("db.dispersion.find({color: '%s'})", color)
	result, err := conn.(*polypheny.PolyphenyConn).QueryMongoContext(context.Background(), query)
	if result == nil || err != nil {
		return
	}
	for _, item := range *result {
		fmt.Printf("%s: %s\n", item["key"].(string), item["item"].(string))
	}
}

func markDone(key string) {
	db, err := sql.Open(driverName, connectionString)
	if err != nil {
		fmt.Printf("failed to mark task %s done\n", key)
	}
	defer db.Close()
	query := fmt.Sprintf("DELETE FROM dispersion WHERE key = '%s'", key)
	_, err = db.ExecContext(context.Background(), query)
	if err != nil {
		fmt.Printf("failed to mark task %s done\n", key)
	}
}

func main() {
	if len(os.Args) < 2 {
		return
	}
	switch os.Args[1] {
	case "get":
		if len(os.Args) == 2 {
			getAll()
		} else {
			getAllWithColor(os.Args[2])
		}
	case "add":
		if len(os.Args) != 4 {
			return
		} else {
			addItem(os.Args[2], os.Args[3])
		}
	case "markdone":
		if len(os.Args) != 3 {
			return
		} else {
			markDone(os.Args[2])
		}
	case "init":
		initDatabase()
	}
}
