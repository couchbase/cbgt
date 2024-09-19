//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbgt

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
	indexDefToBase(def, &ide.indexDefBase)

	err := UnmarshalJSON(b, &ide)
	if err == nil {
		indexDefFromBase(&ide.indexDefBase, def)

		def.Params = ide.Params
		def.SourceParams = ide.SourceParams

		return nil
	}

	// Else, try the new, "natural", non-enveloped, nested format.
	var idn IndexDefNested
	indexDefToBase(def, &idn.indexDefBase)

	err = UnmarshalJSON(b, &idn)
	if err != nil {
		return err
	}

	indexDefFromBase(&idn.indexDefBase, def)

	jp, _ := MarshalJSON(idn.Params)
	def.Params = string(jp)

	js, _ := MarshalJSON(idn.SourceParams)
	def.SourceParams = string(js)

	return nil
}

// Implemention of json.Marshaler interface.  The IndexDef JSON output
// format is now the natural, nested JSON format (as opposed to the
// previous, enveloped format).
func (def *IndexDef) MarshalJSON() ([]byte, error) {
	var idn IndexDefNested

	indexDefToBase(def, &idn.indexDefBase)

	if len(def.Params) > 0 {
		var mp map[string]interface{}

		err := UnmarshalJSON([]byte(def.Params), &mp)
		if err != nil {
			return nil, err
		}

		idn.Params = mp
	}

	if len(def.SourceParams) > 0 {
		var ms map[string]interface{}

		err := UnmarshalJSON([]byte(def.SourceParams), &ms)
		if err != nil {
			return nil, err
		}

		idn.SourceParams = ms
	}

	return MarshalJSON(idn)
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
	base.HibernationPath = indexDef.HibernationPath
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
	indexDef.HibernationPath = base.HibernationPath
}

// -------------------------------------------------------------------

// A PlanPIndexNested overrides PlanPIndex with IndexParams and
// SourceParams fields that are JSON nested objects instead of
// strings, for easier-to-use API.
type PlanPIndexNested struct {
	planPIndexBase

	IndexParams  map[string]interface{} `json:"indexParams,omitempty"`
	SourceParams map[string]interface{} `json:"sourceParams,omitempty"`
}

// A PlanPIndexEnveloped overrides PlanPIndex with IndexParams and
// SourceParams fields that are enveloped JSON (JSON encoded as
// strings), for backwards compatibility.
type PlanPIndexEnveloped struct {
	planPIndexBase

	IndexParams  string `json:"indexParams,omitempty"`
	SourceParams string `json:"sourceParams,omitempty"`
}

// -------------------------------------------------------------------

// Implemention of json.Unmarshaler interface, which accepts either
// the new, nested JSON format or the older, enveloped format.
func (ppi *PlanPIndex) UnmarshalJSON(b []byte) error {
	// First, try the old, backwards compatible, enveloped format.
	var ppie PlanPIndexEnveloped

	err := UnmarshalJSON(b, &ppie)
	if err == nil {
		planPIndexFromBase(&ppie.planPIndexBase, ppi)

		ppi.IndexParams = ppie.IndexParams
		ppi.SourceParams = ppie.SourceParams

		return nil
	}

	// Else, try the new, "natural", non-enveloped, nested format.
	ppin := PlanPIndexNested{
		IndexParams:  map[string]interface{}{},
		SourceParams: map[string]interface{}{},
	}

	err = UnmarshalJSON(b, &ppin)
	if err != nil {
		return err
	}

	planPIndexFromBase(&ppin.planPIndexBase, ppi)

	ji, _ := MarshalJSON(ppin.IndexParams)
	ppi.IndexParams = string(ji)

	js, _ := MarshalJSON(ppin.SourceParams)
	ppi.SourceParams = string(js)

	return nil
}

// Implemention of json.Marshaler interface.  The PlanPIndex JSON
// output format is now the natural, nested JSON format (as opposed to
// the previous, enveloped format).
func (ppi *PlanPIndex) MarshalJSON() ([]byte, error) {
	var ppin PlanPIndexNested

	planPIndexToBase(ppi, &ppin.planPIndexBase)

	if len(ppi.IndexParams) > 0 {
		var mp map[string]interface{}

		err := UnmarshalJSON([]byte(ppi.IndexParams), &mp)
		if err != nil {
			return nil, err
		}

		ppin.IndexParams = mp
	}

	if len(ppi.SourceParams) > 0 {
		var ms map[string]interface{}

		err := UnmarshalJSON([]byte(ppi.SourceParams), &ms)
		if err != nil {
			return nil, err
		}

		ppin.SourceParams = ms
	}

	return MarshalJSON(ppin)
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
	base.HibernationPath = planPIndex.HibernationPath
	base.PlannerVersion = planPIndex.PlannerVersion
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
	planPIndex.HibernationPath = base.HibernationPath
	planPIndex.PlannerVersion = base.PlannerVersion
}
