// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package recordio

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Reader decodes length-prefixed Medios RecordIO JSON frames:
//   <byte-length>\n<payload-bytes>
type Reader struct {
	br *bufio.Reader
}

func NewReader(r io.Reader) *Reader {
	return &Reader{br: bufio.NewReader(r)}
}

// Next reads the next complete JSON record into dst.
func (r *Reader) Next(dst any) error {
	for {
		line, err := r.br.ReadString('\n')
		if err != nil {
			return err
		}
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		size, err := strconv.Atoi(trimmed)
		if err != nil || size < 0 {
			return fmt.Errorf("invalid recordio length %q: %w", trimmed, err)
		}
		buf := make([]byte, size)
		if _, err := io.ReadFull(r.br, buf); err != nil {
			return err
		}
		return json.Unmarshal(buf, dst)
	}
}
