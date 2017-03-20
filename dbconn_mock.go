// Copyright (c) 2015 ZeroStack Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// A mock DB connection

package dbconn

import (
  "github.com/gocql/gocql"
)

// DBConnMock is the structure that facilitates a mock DB Store for testing.
type DBConnMock struct {
}

var _ DBConn = &DBConnMock{}

// NewDBConnMock is a factory method that returns a DBConnMock.
func NewDBConnMock() (*DBConnMock, error) {
  d := DBConnMock{}
  return &d, nil
}

// Add (mock) does not do anything to the mock DB.
func (d *DBConnMock) Add(count int) error {
  return nil
}

// Done (mock) does not do anything to the mock DB.
func (d *DBConnMock) Done() error {
  return nil
}

// Connect (mock) does not do anything to the mock DB.
func (d *DBConnMock) Connect() error {
  return nil
}

// GetSession (mock) does not do anything to the mock DB.
func (d *DBConnMock) GetSession() *gocql.Session {
  return nil
}

// ReleaseSession (mock) release a session object.
func (d *DBConnMock) ReleaseSession(s *gocql.Session) error {
  return nil
}
