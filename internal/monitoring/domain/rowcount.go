package domain

import (
	"strconv"
)

type CountQuery struct {
	Conditions []CountCondition `json:"conditions"`
}

type CountCondition struct {
	Field    string `json:"field"`
	Operator string `json:"operator"`
	Table    string `json:"table"`
	Value    string `json:"value"`
}

func ParseInt(s string) (int, error) {
	return strconv.Atoi(s)
}
