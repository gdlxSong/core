/*
Copyright 2021 The tKeel Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package constraint

import (
	"github.com/pkg/errors"
	"github.com/tkeel-io/collectjs"
	"github.com/tkeel-io/tdtl"
)

func EncodeJSON(kvalues map[string]tdtl.Node) ([]byte, error) {
	collect := collectjs.New("{}")
	for key, val := range kvalues {
		collect.Set(key, []byte(val.String()))
	}
	return collect.GetRaw(), errors.Wrap(collect.GetError(), "Encode Json")
}
