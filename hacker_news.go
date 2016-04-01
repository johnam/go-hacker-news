package main

import (
        "database/sql"
        "encoding/json"
        "errors"
        "flag"
        "fmt"
        "github.com/lib/pq"
        "io/ioutil"
        "log"
        "net/http"
        "strconv"
        "strings"
        "time"
)

const (
        ItemUrlTemplate = "https://hacker-news.firebaseio.com/v0/item/[item_id].json"
)

var (
        client        = &http.Client{}
        sqlDb         = flag.String("db", "postgres", "e.g. postgres")
        dbConnOptions = flag.String("db-conn-options", "", "e.g. user=<db_user> dbname=<db_name> sslmode=<sslmode>")
        dbTable       = flag.String("db-table", "hacker_news_items", "e.g. hacker_news_items")
        userAgent     = flag.String("user-agent", "", "e.g. localhost:research:v0.0.1 (by bob@example.com)")
        minId         = flag.Int("min-id", 0, "id of first item to fetch")
        maxId         = flag.Int("max-id", 0, "id of last item to fetch (https://hacker-news.firebaseio.com/v0/maxitem)")
        batchSize     = flag.Int("batch-size", 100000, "# of records given to each goroutine")
)

type Item struct {
        Id, Time, Score, Parent, Descendants, Ranking int64
        Type, By, Url, Title, Text, Parts             string
        Kids                                          []int
        Deleted, Dead                                 bool
}

func checkErr(err error) {
        if err != nil {
                log.Fatal(err)
        }
}

func get_and_load(url string) (*Item, error) {
        req, err := http.NewRequest("GET", url, nil)
        checkErr(err)

        req.Header.Add("User-Agent", *userAgent)
        resp, err := client.Do(req)
        checkErr(err)
        defer resp.Body.Close()

        if resp.StatusCode == 200 {
                bodyBytes, err := ioutil.ReadAll(resp.Body)
                checkErr(err)

                item := new(Item)
                json.Unmarshal(bodyBytes, &item)

                return item, nil
        } else {
                return nil, errors.New("Bad response")
        }
}

func item_url(itemId int) string {
        itemIdString := strconv.Itoa(itemId)
        url := strings.Replace(ItemUrlTemplate, "[item_id]", itemIdString, 1)

        return url
}

func insert_table_name(template string) string {
        return fmt.Sprintf(template, *dbTable)
}

func toNullString(s string) sql.NullString {
        return sql.NullString{String: s, Valid: s != ""}
}

func toNullInt64(i int64) sql.NullInt64 {
        return sql.NullInt64{Int64: i, Valid: i != 0}
}

func toNullTime(i int64) pq.NullTime {
        timestamp := time.Unix(i, 0).UTC()

        return pq.NullTime{Time: timestamp, Valid: i != 0}
}

func toNullKids(k []int) sql.NullString {
        var kidsStrings = []string{}

        for _, i := range k {
                s := strconv.Itoa(i)
                kidsStrings = append(kidsStrings, s)
        }

        kids := strings.Join(kidsStrings[:], ",")

        return sql.NullString{String: kids, Valid: kids != ""}
}

func store_item(db *sql.DB, item *Item) {
        if item == nil {
                fmt.Println("item is null")
        } else {
                result, err := db.Exec(
                        insert_table_name("INSERT INTO %s (id, type, by, url, title, text, score, time, parent, deleted, dead, descendants, ranking, kids, parts) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)"),
                        toNullInt64(item.Id),
                        toNullString(item.Type),
                        toNullString(item.By),
                        toNullString(item.Url),
                        toNullString(item.Title),
                        toNullString(item.Text),
                        toNullInt64(item.Score),
                        toNullTime(item.Time),
                        toNullInt64(item.Parent),
                        item.Deleted,
                        item.Dead,
                        toNullInt64(item.Descendants),
                        toNullInt64(item.Ranking),
                        toNullKids(item.Kids),
                        toNullString(item.Parts),
                )
                if err != nil {
                        fmt.Printf("error saving item: %+v\n", item)
                        fmt.Println("result:")
                        fmt.Println(result)
                        log.Fatal(err)
                }

                fmt.Println(result)
        }
}

func new_item(db *sql.DB, itemId int) bool {
        rows, err := db.Query(insert_table_name("SELECT COUNT(*) FROM %s WHERE id = $1"), itemId)
        checkErr(err)

        var count int
        for rows.Next() {
                err := rows.Scan(&count)
                checkErr(err)
        }

        if count == 0 {
                return true
        } else {
                fmt.Printf("item already exists: %d\n", itemId)
                return false
        }
}

func work(startId int, endId int) {
        db, err := sql.Open(*sqlDb, *dbConnOptions)
        checkErr(err)

        for itemId := startId; itemId <= endId; itemId++ {
                if new_item(db, itemId) {
                        url := item_url(itemId)

                        item, err := get_and_load(url)
                        checkErr(err)

                        store_item(db, item)

                        fmt.Printf("processed item id: %d\n", itemId)
                }

                if itemId == endId {
                        fmt.Printf("Finished: %d to %d\n", startId, endId)
                }
        }
}

func work_in_batches() {
        for startId := *minId; startId <= *maxId; startId += (*batchSize + 1) {
                fmt.Printf("start: %d\n", startId)
                endOfBatch := startId + *batchSize

                var endId int

                if endOfBatch > *maxId {
                        endId = *maxId
                } else {
                        endId = endOfBatch
                }
                fmt.Printf("end: %d\n", endId)

                go work(startId, endId)
        }
}

func main() {
        flag.Parse()

        fmt.Printf("Fetching records from %d to %d\n", *minId, *maxId)

        work_in_batches()

        var input string
        fmt.Scanln(&input)
        fmt.Println("done")
}
