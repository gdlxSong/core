package scheme

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/qri-io/jsonschema"
)

func TestJSONScheme(t *testing.T) {
	ctx := context.Background()
	var schemaData = []byte(`{
    "$id": "https://qri.io/schema/",
    "$comment" : "sample comment",
    "title": "Person",
    "type": "object",
    "properties": {
        "firstName": {
            "type": "string"
        },
        "lastName": {
            "type": "string"
        },
        "age": {
            "description": "Age in years",
            "type": "integer",
            "minimum": 0
        },
        "friends": {
          "type" : "array",
          "items" : { "title" : "REFERENCE", "$ref" : "#" }
        }
    },
    "required": ["firstName", "lastName"]
  }`)

	rs := &jsonschema.Schema{}
	if err := json.Unmarshal(schemaData, rs); err != nil {
		panic("unmarshal schema: " + err.Error())
	}

	var valid = []byte(`{
    "firstName" : "George",
    "lastName" : "Michael"
    }`)
	errs, err := rs.ValidateBytes(ctx, valid)
	if err != nil {
		panic(err)
	}

	if len(errs) > 0 {
		fmt.Println(errs[0].Error())
	}

	var invalidPerson = []byte(`{
    "firstName" : "Prince"
    }`)

	errs, err = rs.ValidateBytes(ctx, invalidPerson)
	if err != nil {
		panic(err)
	}
	if len(errs) > 0 {
		fmt.Println(errs[0].Error())
	}

	var invalidFriend = []byte(`{
    "firstName" : "Jay",
    "lastName" : "Z",
    "friends" : [{
      "firstName" : "Nas"
      }]
    }`)
	errs, err = rs.ValidateBytes(ctx, invalidFriend)
	if err != nil {
		panic(err)
	}
	if len(errs) > 0 {
		fmt.Println(errs[0].Error())
	}
}

// Output:
// /: {"firstName":"Prince... "lastName" value is required
// /friends/0: {"firstName":"Nas"} "lastName" value is required
