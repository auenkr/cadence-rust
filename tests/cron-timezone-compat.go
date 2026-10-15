package main

import (
	"fmt"
	"github.com/robfig/cron/v3"
	"time"
)

func main() {
	for _, c := range [][3]string{
		{"CRON_TZ=America/New_York 5 16 * * *", "2026-01-01T00:00:00Z", "2026-01-01T21:05:00Z"},
		{"TZ=Asia/Kathmandu @daily", "1900-01-01T00:00:00Z", "1900-01-01T18:18:44Z"},
		{"TZ=Asia/Kathmandu @daily", "2026-01-01T00:00:00Z", "2026-01-01T18:15:00Z"},
		{"TZ=America/New_York 30 2 * * *", "2026-03-08T00:00:00Z", "2026-03-09T06:30:00Z"},
		{"TZ=America/New_York 30 1 * * *", "2026-11-01T05:30:00Z", "2026-11-01T06:30:00Z"},
		{"TZ=America/New_York @every 1h", "2026-11-01T05:30:00Z", "2026-11-01T06:30:00Z"},
	} {
		s, e := cron.ParseStandard(c[0])
		if e != nil {
			panic(e)
		}
		t, _ := time.Parse(time.RFC3339, c[1])
		got := s.Next(t).Format(time.RFC3339)
		if got != c[2] {
			panic(got)
		}
		fmt.Println(c[0], got)
	}
}
