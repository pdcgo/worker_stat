package replication

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/shopspring/decimal"
)

type DecoderFunc func(raw []byte) (any, error)

type Decoder struct {
	byOID map[uint32]DecoderFunc
}

const (
	OIDBool        = 16
	OIDInt8        = 20
	OIDInt4        = 23
	OIDText        = 25
	OIDFloat8      = 701
	OIDNumeric     = 1700
	OIDTimestampTZ = 1184
	OIDJSONB       = 3802
)

func NewDecoder() *Decoder {
	d := &Decoder{
		byOID: map[uint32]DecoderFunc{},
	}

	d.byOID[OIDBool] = func(b []byte) (any, error) {
		return b[0] == 't', nil
	}

	d.byOID[OIDInt8] = func(b []byte) (any, error) {
		return strconv.ParseInt(string(b), 10, 64)
	}

	d.byOID[OIDInt4] = func(b []byte) (any, error) {
		return strconv.Atoi(string(b))
	}

	d.byOID[OIDFloat8] = func(b []byte) (any, error) {
		return strconv.ParseFloat(string(b), 64)
	}

	d.byOID[OIDNumeric] = func(b []byte) (any, error) {
		return decimal.NewFromString(string(b))
	}

	d.byOID[OIDText] = func(b []byte) (any, error) {
		return string(b), nil
	}

	d.byOID[OIDTimestampTZ] = func(b []byte) (any, error) {
		return time.Parse(time.RFC3339Nano, string(b))
	}

	d.byOID[OIDJSONB] = func(b []byte) (any, error) {
		var v any
		err := json.Unmarshal(b, &v)
		return v, err
	}

	return d
}

func (d *Decoder) DecodeRow(
	fields []pgconn.FieldDescription,
	row [][]byte,
) (map[string]any, error) {

	out := make(map[string]any, len(fields))

	for i, f := range fields {
		col := string(f.Name)

		if row[i] == nil {
			out[col] = nil
			continue
		}

		fn, ok := d.byOID[f.DataTypeOID]
		if !ok {
			out[col] = string(row[i]) // fallback
			continue
		}

		v, err := fn(row[i])
		if err != nil {
			return nil, err
		}

		out[col] = v
	}

	return out, nil
}

func (d *Decoder) Scan(res *pgconn.Result, dst any) error {
	for _, row := range res.Rows {
		m, err := d.DecodeRow(res.FieldDescriptions, row)
		if err != nil {
			return err
		}

		err = ToStruct(dst, m)
		if err != nil {
			return err
		}
	}

	return nil
}

func ToStruct(dst any, data any) error {
	if dst == nil {
		return fmt.Errorf("dst is nil")
	}

	rv := reflect.ValueOf(dst)
	if rv.Kind() != reflect.Ptr || rv.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("dst must be pointer to struct")
	}

	m, ok := data.(map[string]any)
	if !ok {
		return fmt.Errorf("data must be map[string]any")
	}

	sv := rv.Elem()
	st := sv.Type()

	for i := 0; i < st.NumField(); i++ {
		fieldType := st.Field(i)
		fieldVal := sv.Field(i)

		if !fieldVal.CanSet() {
			continue
		}

		col := fieldType.Tag.Get("db")
		if col == "" {
			continue
		}

		raw, ok := m[col]
		if !ok || raw == nil {
			continue
		}

		rawVal := reflect.ValueOf(raw)

		// Direct assignable
		if rawVal.Type().AssignableTo(fieldVal.Type()) {
			fieldVal.Set(rawVal)
			continue
		}

		// Convertible (e.g. int -> int64)
		if rawVal.Type().ConvertibleTo(fieldVal.Type()) {
			fieldVal.Set(rawVal.Convert(fieldVal.Type()))
			continue
		}

		return fmt.Errorf(
			"cannot assign column %q (%T) to field %q (%s)",
			col, raw, fieldType.Name, fieldVal.Type(),
		)
	}

	return nil
}
