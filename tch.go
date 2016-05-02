package main

import (
	"flag"
	"encoding/csv"
	"io"
	"fmt"
	"bytes"
	"os"
	"log"
	"net/http"
	"io/ioutil"
	"time"
	"strconv"
	"database/sql"
		 _"github.com/go-sql-driver/mysql"
)

func main() {

	// Command line flags
	frequency := flag.Int("frequency",10,"an int")
	verbose := flag.Bool("verbose",false,"a bool")
	flag.Parse()

	for _ = range time.Tick(time.Duration(*frequency) * time.Minute) {
		processData(*verbose)
	}
}

func getData() (map[string]int) {

	// Get inventory data from online csv
	req, err := http.NewRequest("GET","http://someurl.com/somecsv_fl.csv", nil)
	if err != nil {panic(err)}
	client := http.Client{}
	res, err := client.Do(req)
	if err != nil {panic(err)}
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {panic(err)}
	res.Body.Close()

	// Wally beast!
	b := bytes.NewBufferString(string(body))

	// READ THE CSV FILE
	reader := csv.NewReader(b)
	reader.Comma = ','

	products := make(map[string]int)

	// ITERATE THROUGH THE FILE
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Println("CSV ITERATE ERROR:", err)
		}
		//fmt.Println(record)
		quantity,_ := strconv.Atoi(record[1])
		products[record[0]] = quantity
	}
	return products
}

func writeData(con *sql.DB,product string,newQuantity int,verbose bool) (int,int) {

	insertedCount := 0
	updatedCount := 0

	// Check if the product exists
	row := con.QueryRow("SELECT COUNT(*) as N FROM products WHERE products_model ='"+product+"'")
	var n int
	err := row.Scan(&n)
	if err != nil {fmt.Println(err)}

	// If exists update else insert
	if n ==  0 {

		// Does not exist, lets add it
		_,err = con.Exec("INSERT INTO products (products_quantity,products_model) values (?,?)",newQuantity,product)
		verboseOutput(product,"INSERTING",verbose)
		insertedCount += 1

	} else {

		// Query DB for the record
		rows, err := con.Query("SELECT products_quantity,products_model FROM products WHERE products_model ='"+product+"'")
		if err != nil {
		writeToLog("ERROR: Failed to execute query to select destination record")
			fmt.Println(err)
		}

		// Get the columns
		cols, err := rows.Columns()
		if err != nil {
			writeToLog("ERROR: Failed to get columns from destination database")
			fmt.Println(err)
		}

		// Result is your slice string.
		rawResult := make([][]byte, len(cols))
		result := make([]string, len(cols))
		dest := make([]interface{}, len(cols)) // A temporary interface{} slice
		for i, _ := range rawResult {
			dest[i] = &rawResult[i] // Put pointers to each string in the interface slice
		}

		for rows.Next() {
			err = rows.Scan(dest...)
			if err != nil {
				fmt.Println("Failed to scan row", err)
			}

			for i, raw := range rawResult {
				if raw == nil {
					result[i] = "\\N"
				} else {
					result[i] = string(raw)
				}
			}

			// Convert current qty to int 
			currentQtyInt,_ := strconv.Atoi(result[0])

			// If the qty doesnt match, update
			if currentQtyInt != newQuantity {

				// Convert the new qty to sting for our updat equery
				newQuantityString := strconv.Itoa(newQuantity)

				// Update the record
				_, err = con.Exec("UPDATE products SET products_quantity = "+newQuantityString+" WHERE products_model ='"+product+"'")
				verboseOutput(product,"UPDATING",verbose)

				updatedCount += 1
			} else {
				verboseOutput(product,"NO CHANGE FOR",verbose)
			}
		}
	}
	return insertedCount,updatedCount
}

func processData(verbose bool) {

	var total int
	var updatedTotal int
	var insertedTotal int

	// Get the products we will be processing
	products := getData()

	// Connect to the remote database
	con, err := sql.Open("mysql","DB_CREDS_HERE")
	if err != nil {panic(err)}
	defer con.Close()
    if err != nil {
        writeToLog("ERROR: Failed to connect to destination database")
		panic(err)
        return
    }

	// Iterate through the products and process accordingly
	for k, v := range products {
		inserted,updated := writeData(con,k,v,verbose)
		total += 1
		updatedTotal += updated
		insertedTotal += inserted
    }

	// Write stats to log
	totalString := strconv.Itoa(total)
	updatedString := strconv.Itoa(updatedTotal)
	insertedString := strconv.Itoa(insertedTotal)
	writeToLog(totalString+" products, "+insertedString+" inserted, "+updatedString+" updated")
}

func writeToLog(logData string) {
	f, err := os.OpenFile("triCounty.log", os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
	if err != nil {fmt.Println("ERROR: Cannot write to log file")}
	defer f.Close()
	log.SetOutput(f)
	log.Println(logData)
}

func verboseOutput(product string,action string,verbose bool) {
	if verbose == true {
		fmt.Println(action,":",product)
	}
}
