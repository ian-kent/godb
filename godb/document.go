package godb

import (
	"reflect"
)

type Document struct {
	ObjectID ObjectID
	Fields   map[string]interface{}
}

var FieldCache = make(map[string][]reflect.StructField)

func GetFields(value interface{}) map[string]interface{} {
	fnm := GetFieldNames(value)
	fields := make(map[string]interface{}, 0)
	vl := reflect.ValueOf(value).Elem()

	for i, f := range fnm {
		fields[f.Name] = vl.Field(i).Interface()
	}

	return fields
}

func GetFieldNames(value interface{}) []reflect.StructField {
	tp := reflect.TypeOf(value).Elem()
	tn := tp.Name()

	if fields, ok := FieldCache[tn]; tn != "" && ok {
		return fields
	}

	fields := make([]reflect.StructField, tp.NumField())
	for i := 0; i < tp.NumField(); i++ {
		fields[i] = tp.Field(i)
	}
	FieldCache[tn] = fields

	return fields
}

func Marshal(value interface{}) *Document {
	doc := &Document{
		ObjectID: NewObjectID(),
		Fields:   GetFields(value),
	}

	return doc
}

func (d Document) Unmarshal(value interface{}) {
	tp := reflect.TypeOf(value).Elem()
	vl := reflect.ValueOf(value).Elem()

	for i := 0; i < vl.NumField(); i++ {
		nv := reflect.ValueOf(d.Fields[tp.Field(i).Name])

		switch vl.Field(i).Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			vl.Field(i).SetInt(nv.Int())
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			vl.Field(i).SetUint(nv.Uint())
		case reflect.Float32, reflect.Float64:
			vl.Field(i).SetFloat(nv.Float())
		case reflect.String:
			vl.Field(i).SetString(nv.String())
		default:
			vl.Field(i).Set(nv)
		}
	}
}
