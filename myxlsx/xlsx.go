package myxlsx

import (
	"fmt"

	"github.com/360EntSecGroup-Skylar/excelize"
)

/*
var (
	gAlphabet []string = []string{
		"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"}
)
*/

var (
	xlsxStyles map[string]string = map[string]string{
		"title": `{
			"border": [{
				"type": "left",
				"color": "000000",
				"style": 2
			},
			{
				"type": "right",
				"color": "000000",
				"style": 2
			},
			{
				"type": "top",
				"color": "000000",
				"style": 2
			},
			{
				"type": "bottom",
				"color": "000000",
				"style": 2
			}],
			"fill": {
				"type": "pattern",
				"color": ["#008000"],
				"pattern": 1
			}
		}`,
		"body": `{
			"border": [{
				"type": "left",
				"color": "000000",
				"style": 2
			},
			{
				"type": "right",
				"color": "000000",
				"style": 2
			},
			{
				"type": "top",
				"color": "000000",
				"style": 2
			},
			{
				"type": "bottom",
				"color": "000000",
				"style": 2
			}]
		}`,
		"bodyred": `{
			"border": [{
				"type": "left",
				"color": "000000",
				"style": 2
			},
			{
				"type": "right",
				"color": "000000",
				"style": 2
			},
			{
				"type": "top",
				"color": "000000",
				"style": 2
			},
			{
				"type": "bottom",
				"color": "000000",
				"style": 2
			}],
			"fill": {
				"type": "pattern",
				"color": ["#FF0000"],
				"pattern": 1
			}
		}`,
	}
)

func GetStyleString(styleName string) string {
	return xlsxStyles[styleName]
}

func GetSytleIdx(xlsx *excelize.File, styleName string) (int, error) {
	return xlsx.NewStyle(GetStyleString(styleName))
}

// sRowIdx and sColIdx starts with 0
func SetCellValuesAndStyle(xlsx *excelize.File, vals [][]interface{}, sheetName string, styleIdx int, sRowIdx, sColIdx int) {
	var (
		cIdx int = sColIdx
		rIdx int = sRowIdx
		axis string
	)

	for r := range vals {
		cIdx = sColIdx
		for c := range vals[r] {
			axis = fmt.Sprintf("%s%d", excelize.ToAlphaString(cIdx), rIdx+1)
			xlsx.SetCellStyle(sheetName, axis, axis, styleIdx)
			xlsx.SetCellValue(sheetName, axis, vals[r][c])
			cIdx++
		}
		rIdx++
	}

}
