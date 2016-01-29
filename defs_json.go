//  Copyright (c) 2016 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package cbgt

import (
	"encoding/json"
)

// An IndexDefNested overrides IndexDef with Params and SourceParams
// fields that are JSON nested objects instead of strings, for
// easier-to-use API.
type IndexDefNested struct {
	indexDefBase

	Params       map[string]interface{} `json:"params"`
	SourceParams map[string]interface{} `json:"sourceParams"`
}

// An IndexDefEnveloped overrides IndexDef with Params and
// SourceParams fields that are enveloped JSON (JSON encoded as
// strings), for backwards compatibility.
type IndexDefEnveloped struct {
	indexDefBase

	Params       string `json:"params"`
	SourceParams string `json:"sourceParams"`
}

// -------------------------------------------------------------------

// Implemention of json.Unmarshaler interface, which accepts either
// the new, natural, nested JSON format or the older, enveloped
// format.
func (def *IndexDef) UnmarshalJSON(b []byte) error {
	// First, try the old, backwards compatible, enveloped format.
	var ide IndexDefEnveloped

	err := json.Unmarshal(b, &ide)
	if err == nil {
		indexDefFromBase(&ide.indexDefBase, def)

		def.Params = ide.Params
		def.SourceParams = ide.SourceParams

		return nil
	}

	// Else, try the new, "natural", non-enveloped, nested format.
	var idn IndexDefNested

	err = json.Unmarshal(b, &idn)
	if err != nil {
		return err
	}

	indexDefFromBase(&idn.indexDefBase, def)

	jp, _ := json.Marshal(idn.Params)
	def.Params = string(jp)

	js, _ := json.Marshal(idn.SourceParams)
	def.SourceParams = string(js)

	return nil
}

// Implemention of json.Marshaler interface.  The IndexDef JSON format
// is now the natural, nested JSON format (as opposed to the previous,
// enveloped format).
func (def *IndexDef) MarshalJSON() ([]byte, error) {
	var idn IndexDefNested

	indexDefToBase(def, &idn.indexDefBase)

	if len(def.Params) > 0 {
		var mp map[string]interface{}

		err := json.Unmarshal([]byte(def.Params), &mp)
		if err != nil {
			return nil, err
		}

		idn.Params = mp
	}

	if len(def.SourceParams) > 0 {
		var ms map[string]interface{}

		err := json.Unmarshal([]byte(def.SourceParams), &ms)
		if err != nil {
			return nil, err
		}

		idn.SourceParams = ms
	}

	return json.Marshal(idn)
}

// indexDefToBase copies non-envelope'able fields from the indexDef to
// the indexDefBase.
func indexDefToBase(indexDef *IndexDef, base *indexDefBase) {
	base.Type = indexDef.Type
	base.Name = indexDef.Name
	base.UUID = indexDef.UUID
	base.SourceType = indexDef.SourceType
	base.SourceName = indexDef.SourceName
	base.SourceUUID = indexDef.SourceUUID
	base.PlanParams = indexDef.PlanParams
}

// indexDefFromBase copies non-envelope'able fields from the
// indexDefBase to the indexDef.
func indexDefFromBase(base *indexDefBase, indexDef *IndexDef) {
	indexDef.Type = base.Type
	indexDef.Name = base.Name
	indexDef.UUID = base.UUID
	indexDef.SourceType = base.SourceType
	indexDef.SourceName = base.SourceName
	indexDef.SourceUUID = base.SourceUUID
	indexDef.PlanParams = base.PlanParams
}

// -------------------------------------------------------------------

// An PlanPIndexNested overrides PlanPIndex with Params and SourceParams
// fields that are JSON nested objects instead of strings, for
// easier-to-use API.
type PlanPIndexNested struct {
	planPIndexBase

	IndexParams  map[string]interface{} `json:"indexParams"`
	SourceParams map[string]interface{} `json:"sourceParams"`
}

// An PlanPIndexEnveloped overrides PlanPIndex with Params and
// SourceParams fields that are enveloped JSON (JSON encoded as
// strings), for backwards compatibility.
type PlanPIndexEnveloped struct {
	planPIndexBase

	IndexParams  string `json:"indexParams"`
	SourceParams string `json:"sourceParams"`
}

// -------------------------------------------------------------------

// Implemention of json.Unmarshaler interface, which accepts either
// the new, natural, nested JSON format or the older, enveloped
// format.
func (ppi *PlanPIndex) UnmarshalJSON(b []byte) error {
	// First, try the old, backwards compatible, enveloped format.
	var ppie PlanPIndexEnveloped

	err := json.Unmarshal(b, &ppie)
	if err == nil {
		planPIndexFromBase(&ppie.planPIndexBase, ppi)

		ppi.IndexParams = ppie.IndexParams
		ppi.SourceParams = ppie.SourceParams

		return nil
	}

	// Else, try the new, "natural", non-enveloped, nested format.
	var ppin PlanPIndexNested

	err = json.Unmarshal(b, &ppin)
	if err != nil {
		return err
	}

	planPIndexFromBase(&ppin.planPIndexBase, ppi)

	ji, _ := json.Marshal(ppin.IndexParams)
	ppi.IndexParams = string(ji)

	js, _ := json.Marshal(ppin.SourceParams)
	ppi.SourceParams = string(js)

	return nil
}

// Implemention of json.Marshaler interface.  The PlanPIndex JSON format
// is now the natural, nested JSON format (as opposed to the previous,
// enveloped format).
func (ppi *PlanPIndex) MarshalJSON() ([]byte, error) {
	var ppin PlanPIndexNested

	planPIndexToBase(ppi, &ppin.planPIndexBase)

	if len(ppi.IndexParams) > 0 {
		var mp map[string]interface{}

		err := json.Unmarshal([]byte(ppi.IndexParams), &mp)
		if err != nil {
			return nil, err
		}

		ppin.IndexParams = mp
	}

	if len(ppi.SourceParams) > 0 {
		var ms map[string]interface{}

		err := json.Unmarshal([]byte(ppi.SourceParams), &ms)
		if err != nil {
			return nil, err
		}

		ppin.SourceParams = ms
	}

	return json.Marshal(ppin)
}

// planPIndexToBase copies non-envelope'able fields from the planPIndex to
// the planPIndexBase.
func planPIndexToBase(planPIndex *PlanPIndex, base *planPIndexBase) {
	base.Name = planPIndex.Name
	base.UUID = planPIndex.UUID
	base.IndexType = planPIndex.IndexType
	base.IndexName = planPIndex.IndexName
	base.IndexUUID = planPIndex.IndexUUID
	base.SourceType = planPIndex.SourceType
	base.SourceName = planPIndex.SourceName
	base.SourceUUID = planPIndex.SourceUUID
	base.SourcePartitions = planPIndex.SourcePartitions
	base.Nodes = planPIndex.Nodes
}

// planPIndexFromBase copies non-envelope'able fields from the
// planPIndexBase to the planPIndex.
func planPIndexFromBase(base *planPIndexBase, planPIndex *PlanPIndex) {
	planPIndex.Name = base.Name
	planPIndex.UUID = base.UUID
	planPIndex.IndexType = base.IndexType
	planPIndex.IndexName = base.IndexName
	planPIndex.IndexUUID = base.IndexUUID
	planPIndex.SourceType = base.SourceType
	planPIndex.SourceName = base.SourceName
	planPIndex.SourceUUID = base.SourceUUID
	planPIndex.SourcePartitions = base.SourcePartitions
	planPIndex.Nodes = base.Nodes
}
