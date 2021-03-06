// Code generated by github.com/actgardner/gogen-avro. DO NOT EDIT.
/*
 * SOURCES:
 *     person-key.avsc
 *     person-value.avsc
 */
package schema

import (
    "io"
    "github.com/actgardner/gogen-avro/vm/types"
    "github.com/actgardner/gogen-avro/vm"
    "github.com/actgardner/gogen-avro/compiler"
)


// A person
  
type Person struct {


    // Person full name


        Name string



    // Person home address


        Address *UnionNullString



    // Person age is years


        Age *UnionNullInt


}

func NewPerson() (*Person) {
    return &Person{}
}

func DeserializePerson(r io.Reader) (*Person, error) {
    t := NewPerson()
    deser, err := compiler.CompileSchemaBytes([]byte(t.Schema()), []byte(t.Schema()))
    if err != nil {
        return nil, err
    }

    err = vm.Eval(r, deser, t)
    if err != nil {
        return nil, err
    }
    return t, err
}

func DeserializePersonFromSchema(r io.Reader, schema string) (*Person, error) {
    t := NewPerson()

    deser, err := compiler.CompileSchemaBytes([]byte(schema), []byte(t.Schema()))
    if err != nil {
        return nil, err
    }

    err = vm.Eval(r, deser, t)
    if err != nil {
        return nil, err
    }
    return t, err
}

func writePerson(r *Person, w io.Writer) error {
    var err error

    err = vm.WriteString( r.Name, w)
    if err != nil {
        return err
    }

    err = writeUnionNullString( r.Address, w)
    if err != nil {
        return err
    }

    err = writeUnionNullInt( r.Age, w)
    if err != nil {
        return err
    }

    return err
}

func (r *Person) Serialize(w io.Writer) error {
    return writePerson(r, w)
}

func (r *Person) Schema() string {
    return "{\"doc\":\"A person\",\"fields\":[{\"doc\":\"Person full name\",\"name\":\"name\",\"type\":{\"avro.java.string\":\"String\",\"type\":\"string\"}},{\"default\":null,\"doc\":\"Person home address\",\"name\":\"address\",\"type\":[\"null\",{\"avro.java.string\":\"String\",\"type\":\"string\"}]},{\"default\":null,\"doc\":\"Person age is years\",\"name\":\"age\",\"type\":[\"null\",\"int\"]}],\"name\":\"Person\",\"namespace\":\"com.opi.kafka.avro\",\"type\":\"record\"}"
}

func (r *Person) SchemaName() string {
    return "com.opi.kafka.avro.Person"
}

func (_ *Person) SetBoolean(v bool) { panic("Unsupported operation") }
func (_ *Person) SetInt(v int32) { panic("Unsupported operation") }
func (_ *Person) SetLong(v int64) { panic("Unsupported operation") }
func (_ *Person) SetFloat(v float32) { panic("Unsupported operation") }
func (_ *Person) SetDouble(v float64) { panic("Unsupported operation") }
func (_ *Person) SetBytes(v []byte) { panic("Unsupported operation") }
func (_ *Person) SetString(v string) { panic("Unsupported operation") }
func (_ *Person) SetUnionElem(v int64) { panic("Unsupported operation") }

func (r *Person) Get(i int) types.Field {
    switch (i) {

    case 0:


            return (*types.String)(&r.Name)


    case 1:

            r.Address = NewUnionNullString()



            return r.Address


    case 2:

            r.Age = NewUnionNullInt()



            return r.Age


    }
    panic("Unknown field index")
}

func (r *Person) SetDefault(i int) {
    switch (i) {

        

        
    case 1:
            r.Address = NewUnionNullString()

        return


        
    case 2:
            r.Age = NewUnionNullInt()

        return


    }
    panic("Unknown field index")
}

func (_ *Person) AppendMap(key string) types.Field { panic("Unsupported operation") }
func (_ *Person) AppendArray() types.Field { panic("Unsupported operation") }
func (_ *Person) Finalize() { }
