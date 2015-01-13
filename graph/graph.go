package graph

/* Tim Henderson (tadh@case.edu)
*
* Copyright (c) 2015, Tim Henderson, Case Western Reserve University
* Cleveland, Ohio 44106. All Rights Reserved.
*
* This library is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation; either version 3 of the License, or (at
* your option) any later version.
*
* This library is distributed in the hope that it will be useful, but
* WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
* General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this library; if not, write to the Free Software
* Foundation, Inc.,
*   51 Franklin Street, Fifth Floor,
*   Boston, MA  02110-1301
*   USA
*/

import (
	"os"
	"io"
	"fmt"
	"bytes"
	"strings"
	"encoding/json"
)

import (
	"github.com/timtadh/data-structures/trie"
)

type jsonObject map[string]interface{}

type error_list []error
type ParseErrors error_list
type SerializeErrors error_list

func (self error_list) Error() string {
	var s []string
	for _, err := range self {
		s = append(s, err.Error())
	}
	return "[" + strings.Join(s, ",") + "]"
}
func (self ParseErrors) Error() string { return error_list(self).Error() }
func (self SerializeErrors) Error() string { return error_list(self).Error() }

type Vertex struct {
	Id int64 "id"
	Label string "label"
	Rest jsonObject
}

type Arc struct {
	Src int64
	Targ int64
}

type Edge struct {
	Arc
	Label string "label"
	Rest jsonObject
}

type Graph struct {
	Index *trie.TST
	V map[int64]*Vertex "vertices"
	E map[Arc][]*Edge "edges"
	closure map[Arc]bool
	kids map[int64][]int64
	parents map[int64][]int64
}

func ProcessLines(reader io.Reader, process func([]byte)) {

	const SIZE = 4096

	read_chunk := func() (chunk []byte, closed bool) {
		chunk = make([]byte, 4096)
		if n, err := reader.Read(chunk); err == io.EOF {
			return nil, true
		} else if err != nil {
			panic(err)
		} else {
			return chunk[:n], false
		}
	}

	parse := func(buf []byte) (obuf, line []byte, ok bool) {
		for i := 0; i < len(buf); i++ {
			if buf[i] == '\n' {
				line = buf[:i+1]
				obuf = buf[i+1:]
				return obuf, line, true
			}
		}
		return buf, nil, false
	}

	var buf []byte
	read_line := func() (line []byte, closed bool) {
		ok := false
		buf, line, ok = parse(buf)
		for !ok {
			chunk, closed := read_chunk()
			if closed || len(chunk) == 0 {
				return buf, true
			}
			buf = append(buf, chunk...)
			buf, line, ok = parse(buf)
		}
		return line, false
	}

	var line []byte
	closed := false
	for !closed {
		line, closed = read_line()
		process(line)
	}
}

func renderJson(obj jsonObject) (data []byte, err error) {
	return json.Marshal(obj)
}

func parseJson(data []byte) (obj jsonObject, err error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&obj); err != nil {
		return nil, err
	}
	return obj, nil
}

func parseLine(line []byte) (line_type string, data []byte) {
	split := bytes.Split(line, []byte("\t"))
	return strings.TrimSpace(string(split[0])), bytes.TrimSpace(split[1])
}

func newGraph() *Graph {
	return &Graph{
		Index: new(trie.TST),
		V: make(map[int64]*Vertex),
		E: make(map[Arc][]*Edge),
		kids: make(map[int64][]int64),
		parents: make(map[int64][]int64),
	}
}

func LoadGraph(reader io.Reader) (graph *Graph, err error) {
	var errors ParseErrors
	graph = newGraph()

	ProcessLines(reader, func(line []byte) {
		if len(line) == 0 || !bytes.Contains(line, []byte("\t")) {
			return
		}
		line_type, data := parseLine(line)

		switch line_type {
		case "vertex":
			if v, err := LoadVertex(data); err == nil {
				graph.addVertex(v)
			} else {
				errors = append(errors, err)
			}
		case "edge":
			if e, err := LoadEdge(data); err == nil {
				graph.addEdge(e)
			} else {
				errors = append(errors, err)
			}
		default:
			errors = append(errors, fmt.Errorf("Unknown line type %v", line_type))
			return
		}
	})
	if len(errors) == 0 {
		return graph, nil
	}
	return graph, errors
}

func (self *Graph) addVertex(v *Vertex) {
	self.V[v.Id] = v
	if v.Label == "" {
		return
	}
	var vertices []*Vertex
	if self.Index.Has([]byte(v.Label)) {
		obj, err := self.Index.Get([]byte(v.Label))
		if err != nil {
			panic(err)
		}
		vertices = obj.([]*Vertex)
	}
	vertices = append(vertices, v)
	if vertices == nil {
		panic("verticies == nil")
	}
	err := self.Index.Put([]byte(v.Label), vertices)
	if err != nil {
		file, _ := os.Create("panic.dot")
		fmt.Fprintln(file, self.Index.Dotty())
		file.Close()
		fmt.Println(v.Label)
		panic(err)
	}
}

func (self *Graph) addEdge(e *Edge) {
	self.E[e.Arc] = append(self.E[e.Arc], e)
	self.kids[e.Src] = append(self.kids[e.Src], e.Targ)
	self.parents[e.Targ] = append(self.parents[e.Targ], e.Src)
}

func (self *Graph) Vertices() []int64 {
	vertices := make([]int64, 0, len(self.V)+1)
	for _, v := range self.V {
		vertices = append(vertices, v.Id)
	}
	return vertices
}

func (self *Graph) Has(v *Vertex) bool {
	_, has := self.V[v.Id]
	return has
}

func (self *Graph) Kids(v *Vertex) []*Vertex {
	ids := self.kids[v.Id]
	kids := make([]*Vertex, 0, len(ids))
	for _, id := range ids {
		kids = append(kids, self.V[id])
	}
	return kids
}

func (self *Graph) Parents(v *Vertex) []*Vertex {
	ids := self.parents[v.Id]
	parents := make([]*Vertex, 0, len(ids))
	for _, id := range ids {
		parents = append(parents, self.V[id])
	}
	return parents
}

func (self *Graph) Serialize() ([]byte, error) {
	var lines [][]byte
	var errors SerializeErrors
	for _,v := range self.V {
		b, err := v.Serialize()
		if err != nil {
			errors = append(errors, err)
		} else {
			lines = append(lines, []byte("vertex\t"), b, []byte("\n"))
		}
	}
	for _, edges := range self.E {
		for _, e := range edges {
			b, err := e.Serialize()
			if err != nil {
				errors = append(errors, err)
			} else {
				lines = append(lines, []byte("edge\t"), b, []byte("\n"))
			}
		}
	}
	final := bytes.Join(lines, nil)
	if len(errors) == 0 {
		return final, nil
	}
	return final, errors
}

func LoadVertex(data []byte) (vertex *Vertex, err error) {
	obj, err := parseJson(data)
	if err != nil {
		return nil, err
	}
	id, err := obj["id"].(json.Number).Int64()
	if err != nil {
		return nil, err
	}
	label := obj["label"].(string)
	vertex = &Vertex{
		Id: id,
		Label: strings.TrimSpace(label),
		Rest: make(jsonObject),
	}
	for k,v := range obj {
		vertex.Rest[k] = v
	}
	return vertex, nil
}

func (self *Vertex) Serialize() ([]byte, error) {
	obj := make(jsonObject)
	obj["id"] = self.Id
	obj["label"] = self.Label
	for k, v := range self.Rest {
		obj[k] = v
	}
	return renderJson(obj)
}

func (self *Vertex) String() string {
	return fmt.Sprintf(
		"Vertex(%d, '%s')%v;",
		self.Id, self.Label, self.Rest,
	)
}

func LoadEdge(data []byte) (edge *Edge, err error) {
	obj, err := parseJson(data)
	if err != nil {
		return nil, err
	}
	src, err := obj["src"].(json.Number).Int64()
	if err != nil {
		return nil, err
	}
	targ, err := obj["targ"].(json.Number).Int64()
	if err != nil {
		return nil, err
	}
	edge = &Edge{
		Arc: Arc{
			Src: src,
			Targ: targ,
		},
		Label: obj["label"].(string),
		Rest: make(jsonObject),
	}
	for k,v := range obj {
		edge.Rest[k] = v
	}
	return edge, nil
}

func (self *Edge) Serialize() ([]byte, error) {
	obj := make(jsonObject)
	obj["src"] = self.Src
	obj["targ"] = self.Targ
	obj["label"] = self.Label
	for k, v := range self.Rest {
		obj[k] = v
	}
	return renderJson(obj)
}

func (self *Edge) String() string {
	return fmt.Sprintf(
		"%d->%d '%s' %v;",
		self.Src, self.Targ, self.Label, self.Rest,
	)
}

