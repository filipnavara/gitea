// Copyright 2015 The Gogs Authors. All rights reserved.
// Copyright 2019 The Gitea Authors. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package git

import (
	"bufio"
	"bytes"
	"io"

	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

// CommitGPGSignature represents a git commit signature part.
type CommitGPGSignature struct {
	Signature string

	gogitCommit *object.Commit
	gogitTag    *object.Tag
}

const (
	beginpgp     string = "-----BEGIN PGP SIGNATURE-----"
	gpgsigheader string = "gpgsig "
)

// Verify verifies if the commit signature is cryptographically valid
func (cs *CommitGPGSignature) Verify(armoredKeyRing string) error {
	if cs.gogitCommit != nil {
		_, err := cs.gogitCommit.Verify(armoredKeyRing)
		return err
	}

	_, err := cs.gogitTag.Verify(armoredKeyRing)
	return err
}

// GetPayload gets object content with the GPG signature stripped off
func (cs *CommitGPGSignature) GetPayload() string {
	var payload string

	if cs.gogitCommit != nil {
		encoded := &plumbing.MemoryObject{}
		err := cs.gogitCommit.Encode(encoded)
		if err != nil {
			return ""
		}

		reader, err := encoded.Reader()
		r := bufio.NewReader(reader)

		var message bool
		var gpgsig bool
		for {
			line, err := r.ReadBytes('\n')
			if err != nil && err != io.EOF {
				return ""
			}

			if gpgsig {
				if len(line) > 0 && line[0] == ' ' {
					continue
				} else {
					gpgsig = false
				}
			}

			if !message {
				if len(bytes.TrimSpace(line)) == 0 {
					message = true
				} else if bytes.HasPrefix(line, []byte(gpgsigheader)) {
					gpgsig = true
					continue
				}
			}

			payload += string(line)

			if err == io.EOF {
				break
			}
		}
	} else if cs.gogitTag != nil {
		encoded := &plumbing.MemoryObject{}
		err := cs.gogitTag.Encode(encoded)
		if err != nil {
			return ""
		}

		reader, err := encoded.Reader()
		r := bufio.NewReader(reader)

		for {
			line, err := r.ReadBytes('\n')
			if err != nil && err != io.EOF {
				return ""
			}

			if bytes.Contains(line, []byte(beginpgp)) {
				break
			}

			payload += string(line)

			if err == io.EOF {
				break
			}
		}
	}

	return payload
}
