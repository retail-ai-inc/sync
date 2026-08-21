package domain

import (

	// "github.com/sirupsen/logrus"

	"strconv"
)

// CountQuery represents the query conditions to count documents
type CountQuery struct {
	Conditions []CountCondition `json:"conditions"`
}

// CountCondition represents a single condition for counting
type CountCondition struct {
	Field    string `json:"field"`
	Operator string `json:"operator"`
	Table    string `json:"table"`
	Value    string `json:"value"`
}

// ParseInt parses a string to an integer with error handling.
func ParseInt(s string) (int, error) {
	return strconv.Atoi(s)
}
