package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gocolly/colly"
	"github.com/gocolly/colly/queue"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

type Company struct {
	gorm.Model
	ID           int    `json:"id"`
	Symbol       string `json:"symbol"`
	SecurityName string `json:"securityName"`
	Name         string `json:"name"`
	ActiveStatus string `json:"activeStatus"`
}

type Stock struct {
	gorm.Model
	RemoteId              string
	BusinessDate          string `gorm:"index:idx_last_update,unique"`
	SecurityId            int    `gorm:"type:numeric"`
	Symbol                string `gorm:"index:idx_last_update,unique"`
	SecurityName          string `gorm:"index:idx_last_update,unique"`
	OpenPrice             float64
	HighPrice             float64
	LowPrice              float64
	ClosePrice            float64
	TotalTradedQuantity   float64
	TotalTradedValue      float64
	PreviousDayClosePrice float64
	FiftyTwoWeekHigh      float64
	FiftyTwoWeekLow       float64
	LastUpdatedTime       string
	LastUpdatedPrice      float64
	TotalTrades           int `gorm:"type:numeric"`
	AverageTradedPrice    float64
	MarketCapitalization  float64
}

func main() {
	dsn := "host=0.0.0.0 user=postgres password=password dbname=stock port=5432 sslmode=disable"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		panic(err)
	}

	now := time.Date(2021, 06, 24, 0, 0, 0, 0, time.UTC)
	db.AutoMigrate(&Company{}, &Stock{})
	c := colly.NewCollector()
	c.SetRequestTimeout(time.Minute)
	c.Limit(&colly.LimitRule{
		// Filter domains affected by this rule
		DomainGlob: "www.nepalstock.com",
		// Set a delay between requests to these domains
		Delay: 1 * time.Second,
		// Add an additional random delay
		RandomDelay: 1 * time.Second,
	})
	mutex := sync.Mutex{}
	log.Default().Println("Scrapping Started")
	loadCompany(db)
	c.OnHTML(".table.table-condensed.table-hover tbody", func(e *colly.HTMLElement) {
		log.Println("Table Found")
		var wg sync.WaitGroup
		var s []string
		e.ForEach("tr", func(i int, row *colly.HTMLElement) {
			log.Println("Scanning Row")
			data := Stock{}
			if i >= 2 && i < e.DOM.Children().Length()-4 {
				row.ForEach("td", func(i int, h2 *colly.HTMLElement) {
					switch h2.Index {
					case 1:
						// Company Name
						data.SecurityName = h2.Text
					case 2:
						// No of Shares
						data.TotalTrades = convInt(h2.Text)
					case 3:
						// High price
						data.HighPrice = convFloat(h2.Text)
					case 4:
						// low price
						data.LowPrice = convFloat(h2.Text)
					case 5:
						// closing price
						data.ClosePrice = convFloat(h2.Text)
					case 6:
						data.TotalTradedQuantity = convFloat(h2.Text)
					case 7:
						data.TotalTradedValue = convFloat(h2.Text)
					case 8:
						data.PreviousDayClosePrice = convFloat(h2.Text)
					}
				})
			} else if i == 0 {
				s = strings.Split(strings.TrimPrefix(row.ChildText(".pull-left"), "As of "), "\u00A0")
			}
			if data.SecurityName != "" {
				wg.Add(1)
				data.BusinessDate = s[0]
				data.LastUpdatedTime = s[1]
				go write(&data, db, &wg, &mutex)
			}
		})
		wg.Wait()
	})

	c.OnScraped(func(r *colly.Response) {

		fmt.Println("Finished", r.Request.URL)
		c.Visit(fmt.Sprintf("http://www.nepalstock.com/todaysprice?startDate=%s&_limit=300", fmt.Sprintf("%d-%d-%d\n", now.Year(), now.Month(), now.Day())))
	})

	c.OnError(func(r *colly.Response, e error) {
		fmt.Println(e)
		log.Panic(e)
	})

	log.Default().Println("Site visit start")

	q, _ := queue.New(
		5, // Number of consumer threads
		&queue.InMemoryQueueStorage{MaxSize: 10000}, // Use default queue storage
	)
	for i := 0; i < 3800; i++ {
		if i != 0 {
			now = now.Add(-24 * time.Hour)
		}
		url := fmt.Sprintf("http://www.nepalstock.com/todaysprice?startDate=%s&_limit=300", fmt.Sprintf("%d-%d-%d", now.Year(), now.Month(), now.Day()))
		q.AddURL(url)
		// err = c.Visit(url)
		// if err != nil {
		// 	log.Fatal(err)
		// }
		fmt.Println(url)
	}
	q.Run(c)
	log.Println("Scraping finished, check  database for results")
}

func convInt(str string) int {
	d, _ := strconv.Atoi(str)
	return d
}

func convFloat(str string) float64 {
	d, _ := strconv.ParseFloat(str, 64)
	return d
}

func write(data *Stock, db *gorm.DB, wg *sync.WaitGroup, mutex *sync.Mutex) {
	mutex.Lock()
	defer wg.Done()
	defer mutex.Unlock()
	fmt.Println("Writing to Database ", data.BusinessDate)
	comp := &Company{}
	err := db.Where(&Company{SecurityName: data.SecurityName}).First(&comp).Error
	if err != nil {
		fmt.Println(err)
	}
	data.Symbol = comp.Symbol
	data.SecurityId = comp.ID
	err = db.Clauses(clause.OnConflict{UpdateAll: true}).Create(data).Error
	if err != nil {
		fmt.Println(err)
	}

}

func loadCompany(db *gorm.DB) {
	c1 := colly.NewCollector()

	c1.OnResponse(func(r *colly.Response) {
		var company []Company
		json.Unmarshal(r.Body, &company)
		db.Transaction(func(tx *gorm.DB) error {
			if err := tx.Create(&company).Error; err != nil {
				return err
			}
			return nil
		})
	})
	c1.Visit("https://newweb.nepalstock.com/api/nots/security?nonDelisted=true")
}
