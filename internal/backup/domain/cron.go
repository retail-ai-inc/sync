package domain

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// A backup job's schedule is a five-field cron expression, and until now
// nothing read it. Two things went wrong as a result.

// Schedule is a parsed cron expression: which minutes, hours, days, months and
// weekdays a job runs on.
type Schedule struct {
	minutes  fieldSet
	hours    fieldSet
	days     fieldSet
	months   fieldSet
	weekdays fieldSet
	// daysRestricted and weekdaysRestricted record whether each of the two day
	// fields names anything in particular. cron treats them as alternatives when
	// both do, which is the one place its semantics are not simply "all fields
	// must match".
	daysRestricted     bool
	weekdaysRestricted bool
}

type fieldSet map[int]bool

type bounds struct {
	name     string
	min, max int
}

var cronBounds = []bounds{
	{"minute", 0, 59},
	{"hour", 0, 23},
	{"day of month", 1, 31},
	{"month", 1, 12},
	{"day of week", 0, 6},
}

func ParseSchedule(expr string) (Schedule, error) {
	fields := strings.Fields(strings.TrimSpace(expr))
	if len(fields) != 5 {
		return Schedule{}, fmt.Errorf("a cron expression has five fields; %q has %d",
			expr, len(fields))
	}

	sets := make([]fieldSet, 5)
	for i, field := range fields {
		set, err := parseField(field, cronBounds[i])
		if err != nil {
			return Schedule{}, err
		}
		sets[i] = set
	}

	return Schedule{
		minutes:            sets[0],
		hours:              sets[1],
		days:               sets[2],
		months:             sets[3],
		weekdays:           sets[4],
		daysRestricted:     fields[2] != "*",
		weekdaysRestricted: fields[4] != "*" && fields[4] != "?",
	}, nil
}

// parseField reads one field: a list of terms, each a star, a number, a range,
// any of those with a step.
func parseField(field string, b bounds) (fieldSet, error) {
	set := fieldSet{}

	for _, term := range strings.Split(field, ",") {
		term = strings.TrimSpace(term)
		if term == "" {
			return nil, fmt.Errorf("the %s field has an empty term", b.name)
		}

		step := 1
		if base, stepText, found := strings.Cut(term, "/"); found {
			parsed, err := strconv.Atoi(stepText)
			if err != nil || parsed < 1 {
				return nil, fmt.Errorf("%q is not a step in the %s field", stepText, b.name)
			}
			step = parsed
			term = base
		}

		low, high := b.min, b.max
		switch {
		case term == "*" || term == "?":
			// The whole range.
		case strings.Contains(term, "-"):
			from, to, _ := strings.Cut(term, "-")
			var err error
			if low, err = boundedValue(from, b); err != nil {
				return nil, err
			}
			if high, err = boundedValue(to, b); err != nil {
				return nil, err
			}
			if low > high {
				return nil, fmt.Errorf("%q runs backwards in the %s field", term, b.name)
			}
		default:
			value, err := boundedValue(term, b)
			if err != nil {
				return nil, err
			}
			low, high = value, value
			if step > 1 {
				// "5/15" means from 5 to the end of the range in steps of 15.
				high = b.max
			}
		}

		for v := low; v <= high; v += step {
			set[v] = true
		}
	}

	if len(set) == 0 {
		return nil, fmt.Errorf("the %s field names nothing", b.name)
	}
	return set, nil
}

// boundedValue reads one number and checks it is in range. Sunday is written
// as 0 or 7.
func boundedValue(text string, b bounds) (int, error) {
	value, err := strconv.Atoi(strings.TrimSpace(text))
	if err != nil {
		return 0, fmt.Errorf("%q is not a number in the %s field", text, b.name)
	}
	if b.name == "day of week" && value == 7 {
		value = 0
	}
	if value < b.min || value > b.max {
		return 0, fmt.Errorf("%d is outside %d-%d in the %s field", value, b.min, b.max, b.name)
	}
	return value, nil
}

// Next reports the first time at or after the given instant that the schedule
// names, or the zero time when it names none within four years — which only a
// date like 30 February can manage.
func (s Schedule) Next(after time.Time) time.Time {
	// Whole minutes, and strictly after the instant given.
	t := after.Truncate(time.Minute).Add(time.Minute)
	limit := after.AddDate(4, 0, 0)

	for ; t.Before(limit); t = t.Add(time.Minute) {
		if !s.months[int(t.Month())] {
			// Skip to the start of the next month rather than a minute at a time.
			t = time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location()).
				AddDate(0, 1, 0).Add(-time.Minute)
			continue
		}
		if !s.matchesDay(t) {
			t = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()).
				AddDate(0, 0, 1).Add(-time.Minute)
			continue
		}
		if !s.hours[t.Hour()] {
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location()).
				Add(time.Hour).Add(-time.Minute)
			continue
		}
		if s.minutes[t.Minute()] {
			return t
		}
	}
	return time.Time{}
}

// matchesDay applies cron's rule for the two day fields: when both name
// something in particular, either one matching is enough.
func (s Schedule) matchesDay(t time.Time) bool {
	day := s.days[t.Day()]
	weekday := s.weekdays[int(t.Weekday())]

	switch {
	case s.daysRestricted && s.weekdaysRestricted:
		return day || weekday
	case s.daysRestricted:
		return day
	case s.weekdaysRestricted:
		return weekday
	default:
		return true
	}
}
