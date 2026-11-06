// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package compact

import (
	"bytes"
	"encoding/json"
	"slices"
	"strconv"
)

// kind tells which JSON value a node holds.
type kind int

const (
	kindObject kind = iota
	kindArray
	kindString
	// kindNull has the text null, so it renders like a literal.
	kindNull
	// kindLiteral covers numbers and booleans, with the text the input had.
	kindLiteral
)

// node is a JSON value that remembers the order of an object's members, which
// a `map[string]any` would lose.
type node struct {
	text    string // the value of a string, or the JSON text of a null or literal
	members []member
	items   []node
	kind    kind
}

// member is one key and value of an object.
type member struct {
	key string
	val node
}

// parser reads values from a decoder. Payload checks the input first and the
// decoder never fails on valid input. The loops over members and items run at
// most `limit` times, the length of the input, which is more than any valid
// array or object has. Bad input ends them instead of repeating forever.
type parser struct {
	dec   *json.Decoder
	limit int
}

// next reads one token. The decoder can't fail on valid input, so the error is
// dropped.
func (p *parser) next() json.Token {
	token, _ := p.dec.Token() //nolint:errcheck // the input is valid JSON

	return token
}

// parse reads one value.
func (p *parser) parse() node {
	switch value := p.next().(type) {
	case json.Delim:
		if value == '{' {
			return p.parseObject()
		}

		return p.parseArray()
	case string:
		return stringNode(value)
	case json.Number:
		return literal(value.String())
	case bool:
		return literal(strconv.FormatBool(value))
	default:
		return node{kind: kindNull, text: "null", members: nil, items: nil}
	}
}

func (p *parser) parseObject() node {
	var members []member

	for range p.limit {
		if !p.dec.More() {
			// gomutants:disable-next-line INVERT_LOOP_CTRL reason="More remains false, so continue only spins to the limit"
			break
		}

		key, _ := p.next().(string) //nolint:errcheck // an object key is a string
		members = append(members, member{key: key, val: p.parse()})
	}

	p.next() // the closing brace

	return node{kind: kindObject, text: "", members: members, items: nil}
}

func (p *parser) parseArray() node {
	var items []node

	for range p.limit {
		if !p.dec.More() {
			// gomutants:disable-next-line INVERT_LOOP_CTRL reason="More remains false, so continue only spins to the limit"
			break
		}

		items = append(items, p.parse())
	}

	p.next() // the closing bracket

	return node{kind: kindArray, text: "", members: nil, items: items}
}

func literal(text string) node {
	return node{kind: kindLiteral, text: text, members: nil, items: nil}
}

func stringNode(text string) node {
	return node{kind: kindString, text: text, members: nil, items: nil}
}

// render writes value as JSON without whitespace.
func render(value node) []byte {
	var out bytes.Buffer

	write(&out, value)

	return out.Bytes()
}

// write appends the JSON text of `value` to `out`, with no whitespace between
// tokens. An object writes its members in the order of the node, which is the
// order of the input, and an array does the same with its items. A null
// and a literal write the text they carry, as it came in. That keeps a number
// such as 1e2 or 12345678901234567890 exactly as the webhook had it.
//
//nolint:exhaustive // a null and a literal both write their text
func write(out *bytes.Buffer, value node) {
	switch value.kind {
	case kindObject:
		out.WriteByte('{')

		for i, entry := range value.members {
			if i > 0 {
				out.WriteByte(',')
			}

			writeString(out, entry.key)
			out.WriteByte(':')
			write(out, entry.val)
		}

		out.WriteByte('}')
	case kindArray:
		out.WriteByte('[')

		for i, item := range value.items {
			if i > 0 {
				out.WriteByte(',')
			}

			write(out, item)
		}

		out.WriteByte(']')
	case kindString:
		writeString(out, value.text)
	default:
		out.WriteString(value.text)
	}
}

// writeString writes `text` as a JSON string. Unlike `json.Marshal`, it leaves
// `<`, `>` and `&` as written, because a comment body reads better that way.
// The encoder never fails on a string, and it ends the value with a newline.
func writeString(out *bytes.Buffer, text string) {
	enc := json.NewEncoder(out)
	enc.SetEscapeHTML(false)

	_ = enc.Encode(text) //nolint:errcheck,errchkjson // a string always encodes

	out.Truncate(out.Len() - 1)
}

// rewrite replaces each member of an object with the members that `rewriter`
// returns for it, possibly none. Any other node comes back unchanged.
func (value node) rewrite(rewriter func(member) []member) node {
	if value.kind != kindObject {
		return value
	}

	members := make([]member, 0, len(value.members))
	for _, entry := range value.members {
		members = append(members, rewriter(entry)...)
	}

	return node{kind: kindObject, text: "", members: members, items: nil}
}

// has reports whether `value` is an object with a member named `key`.
func (value node) has(key string) bool {
	return slices.ContainsFunc(value.members, func(entry member) bool { return entry.key == key })
}

// pick keeps the members of an object whose keys are listed.
func (value node) pick(keys ...string) node {
	return value.rewrite(func(entry member) []member {
		if slices.Contains(keys, entry.key) {
			return []member{entry}
		}

		return nil
	})
}
