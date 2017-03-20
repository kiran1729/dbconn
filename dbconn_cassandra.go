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

package dbconn

import (
  "fmt"
  "sync"
  "time"

  "github.com/gocql/gocql"
  "github.com/golang/glog"
)

// DBConnCassandra implements the client interface to Cassandra.
// serverList is the cassandra server cluster information.
// keyspace is the keyspace to be used for this session.
type DBConnCassandra struct {
  dbConfig      *DBConfig
  schemaSetupFn func() error
  rwlock        sync.RWMutex // guards dbSession and clusterCfg
  schemaDone    bool         // indicates if schema has been populated or not
  clusterCfg    *gocql.ClusterConfig
  dbSession     *gocql.Session
}

// DBConfig list the DB config params.
type DBConfig struct {
  Servers           string
  Keyspace          string
  Consistency       *gocql.Consistency
  ProtocolVersion   int
  ReplicationFactor uint32
  TTL               time.Duration
  IOTimeout         time.Duration
}

// ConsistencyNames list all the possbile consistencies as defined in gocql's
// sessions.go. The mapping should exactly be the same as defined in gocql.
var ConsistencyNames = map[string]gocql.Consistency{
  "default":     0,
  "any":         gocql.Any,
  "one":         gocql.One,
  "two":         gocql.Two,
  "three":       gocql.Three,
  "quorum":      gocql.Quorum,
  "all":         gocql.All,
  "localquorum": gocql.LocalQuorum,
  "eachquorum":  gocql.EachQuorum,
}

var _ DBConn = &DBConnCassandra{}

// GetCQLConsistency returns gocql consistency for a given consistency string.
func GetCQLConsistency(consistencyStr string) (*gocql.Consistency, error) {
  consistency, ok := ConsistencyNames[consistencyStr]
  if !ok {
    return nil, fmt.Errorf("invalid consistency %s", consistencyStr)
  }
  return &consistency, nil
}

// NewDBConfig constructs DBConfig, initialized with input.
func NewDBConfig(servers, keyspace, consistencyStr string, protocolVersion int,
  replicationFactor uint32, ttl, ioTimeout time.Duration) (*DBConfig, error) {

  consistency, err := GetCQLConsistency(consistencyStr)
  if err != nil {
    return nil, err
  }
  return &DBConfig{
    Servers:           servers,
    Keyspace:          keyspace,
    Consistency:       consistency,
    ProtocolVersion:   protocolVersion,
    ReplicationFactor: replicationFactor,
    TTL:               ttl,
    IOTimeout:         ioTimeout,
  }, nil

}

// NewDBConnCassandra returns an instance of DBConnCassandra.
// ServerList is the list of cassandra servers.
// keyspace is the Cassandra keyspace used by this session.
// TODO: Take envType and replication factor from the caller.
func NewDBConnCassandra(dbConfig *DBConfig, setupSchemaFn func() error) (
  *DBConnCassandra, error) {

  dbConn := &DBConnCassandra{
    dbConfig:      dbConfig,
    schemaSetupFn: setupSchemaFn,
  }
  if err := dbConn.Connect(); err != nil {
    glog.V(2).Infof("error connecting to Cassandra server: %s, keyspace: %s",
      dbConfig.Servers, dbConfig.Keyspace)
    // This isn't an error for the caller, connect will be retried
    // when GetSession() is invoked.
    // TODO: figure out how this will play with the retry-loop that we
    // run outside
    return dbConn, err
  }
  return dbConn, nil
}

// Add increments the waitgroup counter
func (c *DBConnCassandra) Add(count int) error {
  return nil
}

// Done decrements the waitgroup counter
func (c *DBConnCassandra) Done() error {
  return nil
}

// isSessionValid returns true is given session is valid.
func isSessionValid(s *gocql.Session) bool {
  query := "select now() from system.local"
  if err := s.Query(query).Exec(); err != nil {
    return false
  }
  return true
}

// Connect connects or reconnects to Cassandra cluster using the info supplied
// in NewDBConnCassandra. Note that this function needs to be called without
// holding any locks on dbconn. You need to ReleaseSession() if you hold a
// session.
func (c *DBConnCassandra) Connect() error {
  // check if session is valid by taking a readlock
  c.rwlock.RLock()
  if c.dbSession != nil {
    // verify if the connection is good by running a test query
    if isSessionValid(c.dbSession) {
      // session is recovered already, so no need to recover
      c.rwlock.RUnlock()
      return nil
    }
  }
  c.rwlock.RUnlock()
  // either dbSession is nil or the dbsession is no longer valid, recover
  // db connection anyways
  c.rwlock.Lock()

  if c.schemaSetupFn != nil {
    if !c.schemaDone {
      // TODO: take replication factor from the caller
      if err := c.schemaSetupFn(); err != nil {
        glog.Errorf("error in setting up schema %v", err)
        c.rwlock.Unlock()
        return err
      }
      c.schemaDone = true
      glog.Info("schema setup successfully")
    }
  }
  // if not, then go ahead and take a write lock and createSessionInternal
  // release write lock and return
  c.clusterCfg = gocql.NewCluster(c.dbConfig.Servers)
  c.clusterCfg.Keyspace = c.dbConfig.Keyspace
  c.clusterCfg.Consistency = *(c.dbConfig.Consistency)
  c.clusterCfg.Timeout = c.dbConfig.IOTimeout
  c.clusterCfg.ProtoVersion = c.dbConfig.ProtocolVersion

  var errS error
  c.dbSession, errS = c.clusterCfg.CreateSession()
  if errS != nil {
    glog.Warningf("error creating cassandra session to cluster: %s :: %v ",
      c.dbConfig.Servers, errS)
    // c.dbSession = nil
    c.rwlock.Unlock()
    return errS
  }
  glog.Infof("recovered cassandra db connection")
  c.rwlock.Unlock()
  return nil
}

// GetKeyspace returns keyspace for DBConnCassandra.
func (c *DBConnCassandra) GetKeyspace() string {
  return c.dbConfig.Keyspace
}

// GetSession returns a *gocql.Session.
func (c *DBConnCassandra) GetSession() *gocql.Session {
  c.rwlock.RLock()
  if c.dbSession == nil {
    // first time connect call
    c.rwlock.RUnlock()
    if err := c.Connect(); err != nil {
      glog.Warningf("error connecting to Cassandra server: %s, keyspace: %s",
        c.dbConfig.Servers, c.dbConfig.Keyspace)
      return nil
    }
    return c.GetSession()
  }
  return c.dbSession
}

// ReleaseSession releases given session.
func (c *DBConnCassandra) ReleaseSession(s *gocql.Session) error {
  if s != nil {
    c.rwlock.RUnlock()
  }
  return nil
}
