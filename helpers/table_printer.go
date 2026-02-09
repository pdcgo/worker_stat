package helpers

import (
	"fmt"
	"io"
	"os"
	"reflect"
	"sort"
	"strings"

	"github.com/olekukonko/tablewriter"
)

func PrintTable(writer io.Writer, datas any) {
	v := reflect.ValueOf(datas)
	if !v.IsValid() {
		return
	}

	if writer == nil {
		writer = os.Stdout
	}

	table := tablewriter.NewWriter(writer)

	var headers []string
	var rows [][]string

	switch v.Kind() {

	case reflect.Map:
		// map[string]any → single row
		headers, rows = fromMap(v)

	case reflect.Slice:
		if v.Len() == 0 {
			return
		}

		first := v.Index(0)
		if first.Kind() == reflect.Interface {
			first = first.Elem()
		}
		if first.Kind() == reflect.Ptr {
			first = first.Elem()
		}

		switch first.Kind() {
		case reflect.Struct:
			headers, rows = fromStructSlice(v)
		case reflect.Map:
			headers, rows = fromMapSlice(v)
		default:
			panic("PrintTable: unsupported slice element type")
		}

	case reflect.Ptr:
		PrintTable(writer, v.Elem().Interface())
		return

	default:
		panic("PrintTable: unsupported type")
	}

	table.Header(headers)
	for _, r := range rows {
		table.Append(r)
	}
	table.Footer(headers)

	table.Render()
}

func fromMapSlice(v reflect.Value) ([]string, [][]string) {
	headerSet := map[string]struct{}{}

	for i := 0; i < v.Len(); i++ {
		m := v.Index(i)
		if m.Kind() == reflect.Interface {
			m = m.Elem()
		}
		for _, k := range m.MapKeys() {
			headerSet[k.String()] = struct{}{}
		}
	}

	headers := make([]string, 0, len(headerSet))
	for k := range headerSet {
		headers = append(headers, k)
	}
	sort.Strings(headers)

	var rows [][]string
	for i := 0; i < v.Len(); i++ {
		m := v.Index(i)
		if m.Kind() == reflect.Interface {
			m = m.Elem()
		}

		row := make([]string, len(headers))
		for j, h := range headers {
			val := m.MapIndex(reflect.ValueOf(h))
			if val.IsValid() {
				switch val.Kind() {
				case reflect.Float32, reflect.Float64:
					row[j] = FormatIDR(val.Float())
				default:
					row[j] = fmt.Sprint(val.Interface())
				}

			}
		}
		rows = append(rows, row)
	}

	return headers, rows
}

func fromMap(v reflect.Value) ([]string, [][]string) {
	keys := v.MapKeys()
	headers := make([]string, 0, len(keys))

	for _, k := range keys {
		headers = append(headers, k.String())
	}
	sort.Strings(headers)

	row := make([]string, len(headers))
	for i, h := range headers {
		row[i] = fmt.Sprint(v.MapIndex(reflect.ValueOf(h)).Interface())
	}

	return headers, [][]string{row}
}

func fromStructSlice(v reflect.Value) ([]string, [][]string) {
	if v.Kind() != reflect.Slice {
		v = reflect.ValueOf(v)
	}

	var rows [][]string
	var headers []string
	var fieldIdx []int

	elem := v.Index(0)
	if elem.Kind() == reflect.Interface {
		elem = elem.Elem()
	}
	if elem.Kind() == reflect.Ptr {
		elem = elem.Elem()
	}

	typ := elem.Type()

	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		if f.PkgPath != "" {
			continue
		}
		headers = append(headers, f.Name)
		fieldIdx = append(fieldIdx, i)
	}

	for i := 0; i < v.Len(); i++ {
		rv := v.Index(i)
		if rv.Kind() == reflect.Interface {
			rv = rv.Elem()
		}
		if rv.Kind() == reflect.Ptr {
			rv = rv.Elem()
		}

		var row []string
		for _, idx := range fieldIdx {

			switch rv.Field(idx).Kind() {
			case reflect.Float32, reflect.Float64:
				row = append(row, FormatIDR(rv.Field(idx).Float()))
			default:
				row = append(row, fmt.Sprint(rv.Field(idx).Interface()))
			}
		}
		rows = append(rows, row)
	}

	return headers, rows
}

func FormatIDR(v float64) string {
	s := fmt.Sprintf("%.2f", v) // "224923535.00"

	parts := strings.Split(s, ".")
	intPart := parts[0]
	decPart := parts[1]

	// add thousand separators
	var out []byte
	for i, r := range intPart {
		if i > 0 && (len(intPart)-i)%3 == 0 {
			out = append(out, '.')
		}
		out = append(out, byte(r))
	}

	return string(out) + "," + decPart
}
